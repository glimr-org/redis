//// Redis Cache Operations
////
//// The framework's unified cache module defines composite
//// operations like `pull`, `remember`, and JSON helpers — but
//// those are all built on top of 8 simple primitives that each
//// backend must provide. This module is the Redis
//// implementation of those primitives. Adding composite logic
//// here would mean reimplementing it for every backend (ETS,
//// SQLite, etc.), so we deliberately keep this layer thin and
//// let the framework do the heavy lifting.

import gleam/option.{None, Some}
import glimr/cache/cache.{type CacheError, ConnectionError, NotFound}
import glimr_redis/cache/pool.{type Pool}
import valkyrie

// ------------------------------------------------------------- Public Functions

/// Valkyrie returns its own error type when a key is missing,
/// but the framework's CachePool expects NotFound specifically.
/// If we passed Valkyrie errors through directly, the
/// framework's `remember` and `pull` functions wouldn't know
/// whether the key was missing or the connection was broken.
///
pub fn get(pool: Pool, key: String) -> Result(String, CacheError) {
  let conn = pool.get_connection(pool)
  let timeout = pool.get_timeout(pool)
  let prefixed_key = prefix_key(pool, key)

  case valkyrie.get(conn, prefixed_key, timeout) {
    Ok(value) -> Ok(value)
    Error(_) -> Error(NotFound)
  }
}

/// A naive approach would be to SET the key and then call
/// EXPIRE separately — but that leaves a brief window where the
/// key exists without a TTL. If the app crashes between the two
/// calls, that key lives in Redis forever, slowly leaking
/// memory. Passing the TTL as part of SetOptions makes it a
/// single atomic operation.
///
pub fn put(
  pool: Pool,
  key: String,
  value: String,
  ttl_seconds: Int,
) -> Result(Nil, CacheError) {
  let conn = pool.get_connection(pool)
  let timeout = pool.get_timeout(pool)
  let prefixed_key = prefix_key(pool, key)
  let options =
    Some(valkyrie.SetOptions(
      existence_condition: None,
      return_old: False,
      expiry_option: Some(valkyrie.ExpirySeconds(ttl_seconds)),
    ))

  case valkyrie.set(conn, prefixed_key, value, options, timeout) {
    Ok(_) -> Ok(Nil)
    Error(e) ->
      Error(ConnectionError("Failed to set cache key: " <> error_to_string(e)))
  }
}

/// You might think this could just call `put` with a very large
/// TTL, but that's subtly wrong — a key with a TTL of 100 years
/// will still show a TTL when you inspect it, and code that
/// checks "does this key expire?" would get the wrong answer.
/// Omitting the expiry option entirely creates a truly
/// persistent key with no TTL metadata.
///
pub fn put_forever(
  pool: Pool,
  key: String,
  value: String,
) -> Result(Nil, CacheError) {
  let conn = pool.get_connection(pool)
  let timeout = pool.get_timeout(pool)
  let prefixed_key = prefix_key(pool, key)

  case valkyrie.set(conn, prefixed_key, value, None, timeout) {
    Ok(_) -> Ok(Nil)
    Error(e) ->
      Error(ConnectionError("Failed to set cache key: " <> error_to_string(e)))
  }
}

/// Returning Ok even when the key doesn't exist is intentional
/// — if you had to check `has` before calling `forget`, another
/// request could delete the key between your check and your
/// delete, causing a spurious error. Making it idempotent means
/// callers can just fire and forget without worrying about race
/// conditions.
///
pub fn forget(pool: Pool, key: String) -> Result(Nil, CacheError) {
  let conn = pool.get_connection(pool)
  let timeout = pool.get_timeout(pool)
  let prefixed_key = prefix_key(pool, key)

  case valkyrie.del(conn, [prefixed_key], timeout) {
    Ok(_) -> Ok(Nil)
    Error(e) ->
      Error(ConnectionError(
        "Failed to delete cache key: " <> error_to_string(e),
      ))
  }
}

/// The obvious way to check if a key exists is to GET it and
/// see if you got a value back — but that transfers the entire
/// value over the network for nothing. For a 50KB cached JSON
/// blob, that's a lot of wasted bandwidth just to learn "yes,
/// it's there." EXISTS returns a simple count without touching
/// the value at all.
///
pub fn has(pool: Pool, key: String) -> Bool {
  let conn = pool.get_connection(pool)
  let timeout = pool.get_timeout(pool)
  let prefixed_key = prefix_key(pool, key)

  case valkyrie.exists(conn, [prefixed_key], timeout) {
    Ok(count) -> count > 0
    Error(_) -> False
  }
}

/// The tempting approach is `KEYS glimr:cache:*` followed by
/// DEL, but KEYS scans the entire keyspace in one shot and
/// blocks Redis while it does it. On a production instance with
/// millions of keys, that can freeze every other client for
/// seconds. SCAN does the same work in small batches, giving
/// Redis a chance to serve other requests between each batch.
///
pub fn flush(pool: Pool) -> Result(Nil, CacheError) {
  let conn = pool.get_connection(pool)
  let timeout = pool.get_timeout(pool)
  let pattern = pool.get_prefix(pool) <> ":*"

  flush_with_scan(conn, pattern, 0, timeout)
}

/// Incrementing a counter by doing GET, parsing the string to
/// an int, adding, and SET back is both slow and broken under
/// concurrency — two requests could read the same value and
/// both write back the same increment, losing one update. Redis
/// INCRBY does the whole thing atomically on the server side,
/// which is essential for rate limiters and hit counters.
///
pub fn increment(pool: Pool, key: String, by: Int) -> Result(Int, CacheError) {
  let conn = pool.get_connection(pool)
  let timeout = pool.get_timeout(pool)
  let prefixed_key = prefix_key(pool, key)

  case valkyrie.incrby(conn, prefixed_key, by, timeout) {
    Ok(new_value) -> Ok(new_value)
    Error(e) ->
      Error(ConnectionError("Failed to increment: " <> error_to_string(e)))
  }
}

// ------------------------------------------------------------- Private Functions

/// Every key gets the pool's prefix prepended so that when
/// flush runs, it can match "glimr:cache:mypool:*" and only
/// delete keys belonging to that pool. Without prefixing,
/// flushing one cache store would wipe out everything in Redis
/// — queues, sessions, other apps, the lot.
///
fn prefix_key(pool: Pool, key: String) -> String {
  pool.get_prefix(pool) <> ":" <> key
}

/// Deleting keys in batches of 100 is a compromise — too small
/// and you make hundreds of round-trips to flush a large cache,
/// too large and you block Redis for other clients. When Redis
/// returns cursor 0, it means the scan has wrapped around the
/// entire keyspace and we're done.
///
fn flush_with_scan(
  conn: valkyrie.Connection,
  pattern: String,
  cursor: Int,
  timeout: Int,
) -> Result(Nil, CacheError) {
  case valkyrie.scan(conn, cursor, Some(pattern), 100, None, timeout) {
    Ok(#(keys, next_cursor)) -> {
      // Delete the batch of keys if any found
      case keys {
        [] -> Nil
        _ -> {
          let _ = valkyrie.del(conn, keys, timeout)
          Nil
        }
      }

      // Continue scanning if not done
      case next_cursor {
        0 -> Ok(Nil)
        _ -> flush_with_scan(conn, pattern, next_cursor, timeout)
      }
    }
    Error(e) ->
      Error(ConnectionError("Failed to flush cache: " <> error_to_string(e)))
  }
}

/// Valkyrie's error type has variants for every possible Redis
/// failure mode, but cache callers just want a string they can
/// log or show to the user. Formatting these here rather than
/// in Valkyrie keeps the error messages tailored to how Glimr
/// presents them.
///
fn error_to_string(error: valkyrie.Error) -> String {
  case error {
    valkyrie.NotFound -> "Not found"
    valkyrie.Conflict -> "Conflict"
    valkyrie.RespError(msg) -> "RESP error: " <> msg
    valkyrie.ConnectError(_) -> "Connection error"
    valkyrie.Timeout -> "Timeout"
    valkyrie.TcpError(_) -> "TCP error"
    valkyrie.ServerError(msg) -> "Server error: " <> msg
    valkyrie.PoolError(_) -> "Pool error"
  }
}
