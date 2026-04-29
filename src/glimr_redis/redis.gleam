//// Redis Adapter
////
//// A clean public entry point that hides config parsing, pool
//// construction, cache operations, and session storage behind
//// simple start calls. The app's main module reads as a simple
//// sequence of start calls rather than manual config loading
//// and plumbing.
////

import gleam/dict
import gleam/erlang/process
import gleam/option.{None, Some}
import gleam/otp/static_supervisor as supervisor
import glimr/cache.{
  type CacheError, type CachePool, type CacheStore, ConnectionError, NotFound,
  RedisStore,
}
import glimr/config
import glimr/session.{type SessionStore}
import valkyrie

// ------------------------------------------------------------- Public Types

/// Pool is opaque because a Pool that wasn't started through
/// start_pool would have no supervisor behind it — cache
/// operations would silently fail or crash with confusing
/// Valkyrie errors instead of a clear startup failure.
///
pub opaque type Pool {
  Pool(conn: valkyrie.Connection, timeout: Int, prefix: String)
}

// ------------------------------------------------------------- Public Functions

/// Without this, every app's main module would need to import
/// the config loader, the driver module, and the pool module
/// just to get a cache going — three imports and four lines of
/// boilerplate repeated in every project. This collapses all of
/// that into a single call with just a name.
///
pub fn start(name: String) -> CachePool {
  let internal = start_pool(name)
  wrap_pool(internal)
}

/// Sessions need a backing store, and Redis is the go-to for
/// multi-server deployments where in-memory stores won't work.
/// This starts a Redis pool and returns a SessionStore you can
/// pass straight to `session.setup()` in your bootstrap — same
/// one-liner pattern as starting a cache.
///
pub fn session_store(name: String) -> SessionStore {
  let pool = start_pool(name)
  let lifetime = config.get_int("session.lifetime")

  session.new(
    load: fn(session_id) { session_load(pool, session_id) },
    save: fn(session_id, data, flash) {
      session_save(pool, session_id, data, flash, lifetime)
    },
    destroy: fn(session_id) { session_destroy(pool, session_id) },
    gc: fn() { Nil },
    cookie_value: fn(id, _, _) { id },
  )
}

// ------------------------------------------------------------- Internal Public Functions

/// The public start() function wraps the pool into a CachePool
/// immediately, but session setup needs the raw Pool type
/// because the session store talks to Redis directly rather than
/// going through the cache abstraction. Exposing this as
/// @internal lets both paths share the same config loading
/// without duplicating it.
///
@internal
pub fn start_pool(name: String) -> Pool {
  let stores = cache.load_stores()
  let store = cache.find_by_name(name, stores)
  start_pool_from_store(store)
}

/// Console commands like `glimr cache:clear` discover which
/// cache backend to use at runtime from the config. If the Redis
/// URL is wrong, start() would panic and crash the CLI with a
/// stack trace — not a great developer experience. This variant
/// returns a Result so the console can show a friendly error
/// message instead.
///
@internal
pub fn try_start_cache(store: CacheStore) -> Result(CachePool, String) {
  case try_start_pool(store) {
    Ok(internal) -> Ok(wrap_pool(internal))
    Error(msg) -> Error(msg)
  }
}

/// The framework's CachePool is backend-agnostic — it just holds
/// closures for each operation. This is where we wire the
/// Redis-specific implementations into those closures, capturing
/// the internal pool so callers never need to know Redis is
/// involved. Both start() and try_start_cache() funnel through
/// here to avoid duplicating the wiring.
///
@internal
pub fn wrap_pool(internal: Pool) -> CachePool {
  cache.new_pool(
    get: fn(key) { cache_get(internal, key) },
    put: fn(key, value, ttl) { cache_put(internal, key, value, ttl) },
    put_forever: fn(key, value) { cache_put_forever(internal, key, value) },
    forget: fn(key) { cache_forget(internal, key) },
    flush: fn() { cache_flush(internal) },
    increment: fn(key, by) { cache_increment(internal, key, by) },
    has: fn(key) { cache_has(internal, key) },
    stop: fn() { stop_pool(internal) },
  )
}

/// Console commands create pools on the fly and then exit. If we
/// don't explicitly shut down the pool, those connections sit
/// open until the BEAM VM terminates — which can exhaust Redis's
/// connection limit if someone runs several commands in quick
/// succession.
///
@internal
pub fn stop_pool(pool: Pool) -> Nil {
  let _ = valkyrie.shutdown(pool.conn, pool.timeout)
  Nil
}

/// Pool is opaque, so internal callers can't read the conn field
/// directly. These accessors are the trade-off for keeping the
/// type safe — a small price for preventing invalid pools from
/// being constructed.
///
@internal
pub fn get_connection(pool: Pool) -> valkyrie.Connection {
  pool.conn
}

/// Having the timeout live on the pool rather than as a per-call
/// argument means you can't accidentally use 500ms for one
/// operation and 5000ms for another on the same connection,
/// which would make debugging latency issues much harder.
///
@internal
pub fn get_timeout(pool: Pool) -> Int {
  pool.timeout
}

/// The prefix is derived from the pool name at startup, so
/// callers don't need to know the naming convention — they just
/// ask the pool for its prefix and use it to build keys and
/// flush patterns.
///
@internal
pub fn get_prefix(pool: Pool) -> String {
  pool.prefix
}

// ------------------------------------------------------------- Private Functions

/// Every Glimr cache key starts with this prefix so the
/// application's cache data doesn't collide with other data that
/// might live in the same Redis instance — queues, sessions, or
/// keys from other apps sharing the server.
///
const key_prefix = "glimr:cache"

/// Individual Redis connections can drop under load or during
/// network blips. Running them under a OneForOne supervisor
/// means a single dropped connection gets restarted without
/// affecting the other connections in the pool. The named
/// process gives us a stable handle that keeps working even
/// after a connection is recycled behind the scenes.
///
fn start_pool_from_store(store: CacheStore) -> Pool {
  let #(name, url, pool_size) = extract_redis_config(store)
  let pool_name = process.new_name("glimr_redis_pool_" <> name)

  let assert Ok(config) = valkyrie.url_config(url)

  let child_spec =
    valkyrie.supervised_pool(
      config,
      size: pool_size,
      name: option.Some(pool_name),
      timeout: 5000,
    )

  let assert Ok(_) =
    supervisor.new(supervisor.OneForOne)
    |> supervisor.add(child_spec)
    |> supervisor.start

  let conn = valkyrie.named_connection(pool_name)

  let prefix = key_prefix <> ":" <> name
  Pool(conn: conn, timeout: 5000, prefix: prefix)
}

/// When a developer runs `./glimr cache:clear` from the CLI, the
/// command needs to connect to Redis and flush keys. But if the
/// Redis URL is misconfigured, crashing the whole CLI with a
/// panic is a terrible experience — they just want an error
/// message they can act on. This variant returns Result so the
/// console command can handle failures gracefully.
///
fn try_start_pool(store: CacheStore) -> Result(Pool, String) {
  case try_extract_redis_config(store) {
    Ok(#(name, url, pool_size)) -> {
      let pool_name = process.new_name("glimr_redis_pool_" <> name)

      case valkyrie.url_config(url) {
        Ok(config) -> {
          let child_spec =
            valkyrie.supervised_pool(
              config,
              size: pool_size,
              name: option.Some(pool_name),
              timeout: 5000,
            )

          case
            supervisor.new(supervisor.OneForOne)
            |> supervisor.add(child_spec)
            |> supervisor.start
          {
            Ok(_) -> {
              let conn = valkyrie.named_connection(pool_name)
              let prefix = key_prefix <> ":" <> name
              Ok(Pool(conn: conn, timeout: 5000, prefix: prefix))
            }
            Error(_) -> Error("Failed to start Redis pool supervisor")
          }
        }
        Error(_) -> Error("Invalid Redis URL: " <> url)
      }
    }
    Error(msg) -> Error(msg)
  }
}

/// At application boot, a missing REDIS_URL means nothing
/// cache-related will work — there's no sensible fallback.
/// Panicking immediately with a clear message pointing at the
/// missing env var saves developers from chasing down cryptic
/// connection errors minutes later.
///
fn extract_redis_config(store: CacheStore) -> #(String, String, Int) {
  case store {
    RedisStore(name, url_result, pool_size_result) -> {
      let url = case url_result {
        Ok(u) -> u
        Error(var) ->
          panic as { "Redis URL environment variable '" <> var <> "' not set" }
      }
      let pool_size = case pool_size_result {
        Ok(s) -> s
        Error(var) ->
          panic as {
            "Redis pool size environment variable '" <> var <> "' not set"
          }
      }
      #(name, url, pool_size)
    }
    _ -> panic as "Cannot create Redis pool from non-Redis store"
  }
}

/// Same config extraction as above but returns Result instead of
/// panicking. Console commands call this because they need to
/// show a helpful error message and exit cleanly, not crash the
/// CLI with a stack trace.
///
fn try_extract_redis_config(
  store: CacheStore,
) -> Result(#(String, String, Int), String) {
  case store {
    RedisStore(name, url_result, pool_size_result) -> {
      case url_result, pool_size_result {
        Ok(url), Ok(pool_size) -> Ok(#(name, url, pool_size))
        Error(var), _ ->
          Error("Redis URL environment variable '" <> var <> "' not set")
        _, Error(var) ->
          Error("Redis pool size environment variable '" <> var <> "' not set")
      }
    }
    _ -> Error("Cannot create Redis pool from non-Redis store")
  }
}

/// Valkyrie returns its own error type when a key is missing,
/// but the framework's CachePool expects NotFound specifically.
/// If we passed Valkyrie errors through directly, the framework's
/// `remember` and `pull` functions wouldn't know whether the key
/// was missing or the connection was broken.
///
fn cache_get(pool: Pool, key: String) -> Result(String, CacheError) {
  let prefixed_key = prefix_key(pool, key)

  case valkyrie.get(pool.conn, prefixed_key, pool.timeout) {
    Ok(value) -> Ok(value)
    Error(_) -> Error(NotFound)
  }
}

/// A naive approach would be to SET the key and then call EXPIRE
/// separately — but that leaves a brief window where the key
/// exists without a TTL. If the app crashes between the two
/// calls, that key lives in Redis forever, slowly leaking memory.
/// Passing the TTL as part of SetOptions makes it a single atomic
/// operation.
///
fn cache_put(
  pool: Pool,
  key: String,
  value: String,
  ttl_seconds: Int,
) -> Result(Nil, CacheError) {
  let prefixed_key = prefix_key(pool, key)
  let options =
    Some(valkyrie.SetOptions(
      existence_condition: None,
      return_old: False,
      expiry_option: Some(valkyrie.ExpirySeconds(ttl_seconds)),
    ))

  case valkyrie.set(pool.conn, prefixed_key, value, options, pool.timeout) {
    Ok(_) -> Ok(Nil)
    Error(e) ->
      Error(ConnectionError("Failed to set cache key: " <> error_to_string(e)))
  }
}

/// You might think this could just call `put` with a very large
/// TTL, but that's subtly wrong — a key with a TTL of 100 years
/// will still show a TTL when you inspect it, and code that
/// checks "does this key expire?" would get the wrong answer.
/// Omitting the expiry option entirely creates a truly persistent
/// key with no TTL metadata.
///
fn cache_put_forever(
  pool: Pool,
  key: String,
  value: String,
) -> Result(Nil, CacheError) {
  let prefixed_key = prefix_key(pool, key)

  case valkyrie.set(pool.conn, prefixed_key, value, None, pool.timeout) {
    Ok(_) -> Ok(Nil)
    Error(e) ->
      Error(ConnectionError("Failed to set cache key: " <> error_to_string(e)))
  }
}

/// Returning Ok even when the key doesn't exist is intentional —
/// if you had to check `has` before calling `forget`, another
/// request could delete the key between your check and your
/// delete, causing a spurious error. Making it idempotent means
/// callers can just fire and forget without worrying about race
/// conditions.
///
fn cache_forget(pool: Pool, key: String) -> Result(Nil, CacheError) {
  let prefixed_key = prefix_key(pool, key)

  case valkyrie.del(pool.conn, [prefixed_key], pool.timeout) {
    Ok(_) -> Ok(Nil)
    Error(e) ->
      Error(ConnectionError(
        "Failed to delete cache key: " <> error_to_string(e),
      ))
  }
}

/// The obvious way to check if a key exists is to GET it and see
/// if you got a value back — but that transfers the entire value
/// over the network for nothing. For a 50KB cached JSON blob,
/// that's a lot of wasted bandwidth just to learn "yes, it's
/// there." EXISTS returns a simple count without touching the
/// value at all.
///
fn cache_has(pool: Pool, key: String) -> Bool {
  let prefixed_key = prefix_key(pool, key)

  case valkyrie.exists(pool.conn, [prefixed_key], pool.timeout) {
    Ok(count) -> count > 0
    Error(_) -> False
  }
}

/// The tempting approach is `KEYS glimr:cache:*` followed by DEL,
/// but KEYS scans the entire keyspace in one shot and blocks
/// Redis while it does it. On a production instance with millions
/// of keys, that can freeze every other client for seconds. SCAN
/// does the same work in small batches, giving Redis a chance to
/// serve other requests between each batch.
///
fn cache_flush(pool: Pool) -> Result(Nil, CacheError) {
  let pattern = pool.prefix <> ":*"
  flush_with_scan(pool.conn, pattern, 0, pool.timeout)
}

/// Incrementing a counter by doing GET, parsing the string to an
/// int, adding, and SET back is both slow and broken under
/// concurrency — two requests could read the same value and both
/// write back the same increment, losing one update. Redis INCRBY
/// does the whole thing atomically on the server side, which is
/// essential for rate limiters and hit counters.
///
fn cache_increment(pool: Pool, key: String, by: Int) -> Result(Int, CacheError) {
  let prefixed_key = prefix_key(pool, key)

  case valkyrie.incrby(pool.conn, prefixed_key, by, pool.timeout) {
    Ok(new_value) -> Ok(new_value)
    Error(e) ->
      Error(ConnectionError("Failed to increment: " <> error_to_string(e)))
  }
}

/// Every key gets the pool's prefix prepended so that when flush
/// runs, it can match "glimr:cache:mypool:*" and only delete keys
/// belonging to that pool. Without prefixing, flushing one cache
/// store would wipe out everything in Redis — queues, sessions,
/// other apps, the lot.
///
fn prefix_key(pool: Pool, key: String) -> String {
  pool.prefix <> ":" <> key
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
      case keys {
        [] -> Nil
        _ -> {
          let _ = valkyrie.del(conn, keys, timeout)
          Nil
        }
      }

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
/// log or show to the user. Formatting these here rather than in
/// Valkyrie keeps the error messages tailored to how Glimr
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

/// Prefixes the session ID with the pool's namespace and a
/// ":session:" segment so session keys don't collide with cache
/// keys or other data stored in the same Redis instance. The
/// prefix comes from the cache config, ensuring each app or
/// environment gets its own keyspace.
///
fn session_key(pool: Pool, session_id: String) -> String {
  pool.prefix <> ":session:" <> session_id
}

/// A simple GET — Redis returns nil for expired or missing keys,
/// so there's no need for a separate expiration check like the
/// SQL stores do. Falling back to empty dicts on any error means
/// a missing or expired session degrades to a fresh one rather
/// than crashing the request.
///
fn session_load(
  pool: Pool,
  session_id: String,
) -> #(dict.Dict(String, String), dict.Dict(String, String)) {
  let key = session_key(pool, session_id)

  case valkyrie.get(pool.conn, key, pool.timeout) {
    Ok(payload_json) -> session.decode_payload(payload_json)
    Error(_) -> #(dict.new(), dict.new())
  }
}

/// SET with EX (expiry in seconds) is atomic — it writes the
/// payload and sets the TTL in one command, so there's no window
/// where the key exists without an expiration. The TTL is reset
/// on every save, so active sessions stay alive while idle ones
/// expire automatically. No upsert logic needed because Redis SET
/// overwrites by default.
///
fn session_save(
  pool: Pool,
  session_id: String,
  data: dict.Dict(String, String),
  flash: dict.Dict(String, String),
  lifetime: Int,
) -> Nil {
  let key = session_key(pool, session_id)
  let encoded = session.encode_payload(data, flash)
  let ttl_seconds = lifetime * 60

  let options =
    Some(valkyrie.SetOptions(
      existence_condition: None,
      return_old: False,
      expiry_option: Some(valkyrie.ExpirySeconds(ttl_seconds)),
    ))

  let _ = valkyrie.set(pool.conn, key, encoded, options, pool.timeout)
  Nil
}

/// Deletes the key immediately so the old session ID can never
/// be reused — important after invalidation to prevent session
/// fixation attacks. DEL is idempotent; if the key already
/// expired or was never created, the command simply returns zero
/// keys deleted.
///
fn session_destroy(pool: Pool, session_id: String) -> Nil {
  let key = session_key(pool, session_id)

  let _ = valkyrie.del(pool.conn, [key], pool.timeout)
  Nil
}
