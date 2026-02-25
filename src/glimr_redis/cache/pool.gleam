//// Redis Connection Pool
////
//// Redis connections are expensive to establish and can't be
//// shared across concurrent requests without coordination.
//// Valkyrie handles the pooling and checkout mechanics, but
//// cache operations also need a key prefix and a timeout —
//// bundling all three into an opaque Pool means every cache
//// call gets a consistent setup without passing three separate
//// arguments around.

import gleam/erlang/process
import gleam/option
import gleam/otp/static_supervisor as supervisor
import glimr/cache/driver.{type CacheStore, RedisStore}
import valkyrie

// ------------------------------------------------------------- Private Consts

/// Every Glimr cache key starts with this prefix so the
/// application's cache data doesn't collide with other data
/// that might live in the same Redis instance — queues,
/// sessions, or keys from other apps sharing the server.
///
const key_prefix = "glimr:cache"

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

/// Individual Redis connections can drop under load or during
/// network blips. Running them under a OneForOne supervisor
/// means a single dropped connection gets restarted without
/// affecting the other connections in the pool. The named
/// process gives us a stable handle that keeps working even
/// after a connection is recycled behind the scenes.
///
pub fn start_pool(store: CacheStore) -> Pool {
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

/// Pool is opaque, so cache.gleam can't read the conn field
/// directly. These accessors are the trade-off for keeping the
/// type safe — a small price for preventing invalid pools from
/// being constructed.
///
pub fn get_connection(pool: Pool) -> valkyrie.Connection {
  pool.conn
}

/// Having the timeout live on the pool rather than as a
/// per-call argument means you can't accidentally use 500ms for
/// one operation and 5000ms for another on the same connection,
/// which would make debugging latency issues much harder.
///
pub fn get_timeout(pool: Pool) -> Int {
  pool.timeout
}

/// The prefix is derived from the pool name at startup, so
/// cache.gleam doesn't need to know the naming convention — it
/// just asks the pool for its prefix and uses it to build keys
/// and flush patterns.
///
pub fn get_prefix(pool: Pool) -> String {
  pool.prefix
}

// ------------------------------------------------------------- Internal Public Functions

/// When a developer runs `./glimr cache:clear` from the CLI,
/// the command needs to connect to Redis and flush keys. But if
/// the Redis URL is misconfigured, crashing the whole CLI with
/// a panic is a terrible experience — they just want an error
/// message they can act on. This variant returns Result so the
/// console command can handle failures gracefully.
///
@internal
pub fn try_start_pool(store: CacheStore) -> Result(Pool, String) {
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

/// Console commands create pools on the fly and then exit. If
/// we don't explicitly shut down the pool, those connections
/// sit open until the BEAM VM terminates — which can exhaust
/// Redis's connection limit if someone runs several commands in
/// quick succession.
///
@internal
pub fn stop_pool(pool: Pool) -> Nil {
  let _ = valkyrie.shutdown(pool.conn, pool.timeout)
  Nil
}

// ------------------------------------------------------------- Private Functions

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

/// Same config extraction as above but returns Result instead
/// of panicking. Console commands call this because they need
/// to show a helpful error message and exit cleanly, not crash
/// the CLI with a stack trace.
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
