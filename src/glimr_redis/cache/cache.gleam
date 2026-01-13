//// Redis Cache Operations
////
//// Provides cache operations for Redis-based caching. Uses 
//// Valkyrie for Redis communication with connection pooling.
//// All keys are automatically prefixed for safe flush operations.

import gleam/dynamic/decode
import gleam/json.{type Json}
import gleam/option.{None, Some}
import gleam/result
import glimr/cache/cache.{
  type CacheError, ComputeError, ConnectionError, NotFound, SerializationError,
}
import glimr_redis/cache/pool.{type Pool}
import valkyrie

// ------------------------------------------------------------- Public Functions

/// Retrieves a value from the cache by key. Returns the cached
/// string value on success, or NotFound if the key does not
/// exist in the cache.
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

/// Stores a value in the cache with a TTL (time-to-live) in
/// seconds. The value will automatically expire after the
/// specified duration. Returns Ok(Nil) on success.
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

/// Stores a value in the cache permanently without any
/// expiration time. The value will remain until explicitly
/// deleted or the cache is flushed.
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

/// Removes a value from the cache by key. This operation is
/// idempotent and returns Ok(Nil) even if the key did not
/// exist in the cache.
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

/// Checks if a key exists in the cache. Returns True if the
/// key is present, False otherwise. Does not return the value,
/// only checks for existence.
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

/// Removes all cached values for this pool using SCAN + DEL.
/// Only deletes keys with this pool's prefix, leaving other
/// data in Redis intact. Safe for shared Redis instances.
///
pub fn flush(pool: Pool) -> Result(Nil, CacheError) {
  let conn = pool.get_connection(pool)
  let timeout = pool.get_timeout(pool)
  let pattern = pool.get_prefix(pool) <> ":*"

  flush_with_scan(conn, pattern, 0, timeout)
}

/// Retrieves a JSON value from the cache and decodes it using
/// the provided decoder. Returns SerializationError if the
/// cached value cannot be parsed as valid JSON.
///
pub fn get_json(
  pool: Pool,
  key: String,
  decoder: decode.Decoder(a),
) -> Result(a, CacheError) {
  use value <- result.try(get(pool, key))

  case json.parse(value, decoder) {
    Ok(decoded) -> Ok(decoded)
    Error(_) -> Error(SerializationError("Failed to decode JSON"))
  }
}

/// Stores a value as JSON in the cache with a TTL. The value
/// is encoded using the provided encoder function before
/// being stored as a JSON string.
///
pub fn put_json(
  pool: Pool,
  key: String,
  value: a,
  encoder: fn(a) -> Json,
  ttl_seconds: Int,
) -> Result(Nil, CacheError) {
  let json_string = json.to_string(encoder(value))
  put(pool, key, json_string, ttl_seconds)
}

/// Stores a value as JSON in the cache permanently without
/// expiration. The value is encoded using the provided
/// encoder function before being stored.
///
pub fn put_json_forever(
  pool: Pool,
  key: String,
  value: a,
  encoder: fn(a) -> Json,
) -> Result(Nil, CacheError) {
  let json_string = json.to_string(encoder(value))
  put_forever(pool, key, json_string)
}

/// Retrieves a value and removes it from the cache atomically.
/// Useful for one-time tokens or values that should only be
/// read once. Returns NotFound if key does not exist.
///
pub fn pull(pool: Pool, key: String) -> Result(String, CacheError) {
  case get(pool, key) {
    Ok(value) -> {
      let _ = forget(pool, key)
      Ok(value)
    }
    Error(e) -> Error(e)
  }
}

/// Increments a numeric value in the cache by the specified
/// amount. If the key does not exist, it is initialized to 0
/// before incrementing. Returns the new value.
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

/// Decrements a numeric value in the cache by the specified
/// amount. If the key does not exist, it is initialized to 0
/// before decrementing. Returns the new value.
///
pub fn decrement(pool: Pool, key: String, by: Int) -> Result(Int, CacheError) {
  let conn = pool.get_connection(pool)
  let timeout = pool.get_timeout(pool)
  let prefixed_key = prefix_key(pool, key)

  case valkyrie.decrby(conn, prefixed_key, by, timeout) {
    Ok(new_value) -> Ok(new_value)
    Error(e) ->
      Error(ConnectionError("Failed to decrement: " <> error_to_string(e)))
  }
}

/// Gets a value from cache, or computes and stores it if not
/// found. The compute function is only called on cache miss.
/// Returns ComputeError if the compute function fails.
///
pub fn remember(
  pool: Pool,
  key: String,
  ttl_seconds: Int,
  compute: fn() -> Result(String, e),
) -> Result(String, CacheError) {
  case get(pool, key) {
    Ok(value) -> Ok(value)
    Error(NotFound) -> {
      case compute() {
        Ok(value) -> {
          let _ = put(pool, key, value, ttl_seconds)
          Ok(value)
        }
        Error(_) -> Error(ComputeError("Compute function failed"))
      }
    }
    Error(e) -> Error(e)
  }
}

/// Gets a value from cache, or computes and stores it
/// permanently if not found. The compute function is only
/// called on cache miss. Value never expires.
///
pub fn remember_forever(
  pool: Pool,
  key: String,
  compute: fn() -> Result(String, e),
) -> Result(String, CacheError) {
  case get(pool, key) {
    Ok(value) -> Ok(value)
    Error(NotFound) -> {
      case compute() {
        Ok(value) -> {
          let _ = put_forever(pool, key, value)
          Ok(value)
        }
        Error(_) -> Error(ComputeError("Compute function failed"))
      }
    }
    Error(e) -> Error(e)
  }
}

/// Gets a JSON value from cache, or computes, encodes, and
/// stores it if not found. Handles both cache miss and
/// deserialization errors by recomputing the value.
///
pub fn remember_json(
  pool: Pool,
  key: String,
  ttl_seconds: Int,
  decoder: decode.Decoder(a),
  compute: fn() -> Result(a, e),
  encoder: fn(a) -> Json,
) -> Result(a, CacheError) {
  case get_json(pool, key, decoder) {
    Ok(value) -> Ok(value)
    Error(NotFound) | Error(SerializationError(_)) -> {
      case compute() {
        Ok(value) -> {
          let _ = put_json(pool, key, value, encoder, ttl_seconds)
          Ok(value)
        }
        Error(_) -> Error(ComputeError("Compute function failed"))
      }
    }
    Error(e) -> Error(e)
  }
}

// ------------------------------------------------------------- Private Functions

/// Prefixes a key with the pool's namespace. This ensures all
/// cache keys are isolated to their respective pools and can
/// be safely flushed without affecting other data.
///
fn prefix_key(pool: Pool, key: String) -> String {
  pool.get_prefix(pool) <> ":" <> key
}

/// Recursively scans and deletes keys matching the pattern.
/// Uses Redis SCAN command to iterate through keys in batches
/// of 100, avoiding blocking operations on large datasets.
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

/// Converts a Valkyrie error to a human-readable string for
/// error messages. Maps each error variant to a descriptive
/// message suitable for logging or debugging.
///
fn error_to_string(error: valkyrie.Error) -> String {
  case error {
    valkyrie.NotFound -> "Not found"
    valkyrie.Conflict -> "Conflict"
    valkyrie.RespError(msg) -> "RESP error: " <> msg
    valkyrie.ConnectionError -> "Connection error"
    valkyrie.Timeout -> "Timeout"
    valkyrie.TcpError(_) -> "TCP error"
    valkyrie.ServerError(msg) -> "Server error: " <> msg
    valkyrie.PoolError(_) -> "Pool error"
  }
}
