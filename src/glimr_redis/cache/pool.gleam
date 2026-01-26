//// Redis Connection Pool
////
//// Provides connection pooling for Redis using Valkyrie.
//// Pools manage a set of reusable connections and handle
//// checkout/checkin automatically.

import gleam/erlang/process
import gleam/option
import gleam/otp/static_supervisor as supervisor
import glimr/cache/driver.{type CacheStore, RedisStore}
import valkyrie

const key_prefix = "glimr:cache"

// ------------------------------------------------------------- Public Types

/// A Redis connection pool that manages reusable connections
/// using Valkyrie's supervised pool. Created via start_pool
/// and provides access to connection, timeout, and key prefix.
///
pub opaque type Pool {
  Pool(conn: valkyrie.Connection, timeout: Int, prefix: String)
}

// ------------------------------------------------------------- Public Functions

/// Creates a new Redis connection pool from the given store
/// config. Starts a supervised pool that manages connections
/// automatically with the configured pool size and timeout.
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

/// Returns a connection from the pool for use in cache
/// operations. The connection is managed by Valkyrie and
/// handles checkout/checkin automatically.
///
pub fn get_connection(pool: Pool) -> valkyrie.Connection {
  pool.conn
}

/// Returns the default timeout in milliseconds for Redis
/// operations. This timeout is applied to all commands
/// executed through this pool's connection.
///
pub fn get_timeout(pool: Pool) -> Int {
  pool.timeout
}

/// Returns the key prefix for this pool. All cache keys are
/// automatically prefixed with this value to provide namespace
/// isolation between different cache stores.
///
pub fn get_prefix(pool: Pool) -> String {
  pool.prefix
}

// -------------------------------------------------- Internal Public Functions

/// Stops a connection pool by shutting down the Valkyrie
/// connection. This should be called when the pool is no
/// longer needed to release resources.
///
@internal
pub fn stop_pool(pool: Pool) -> Nil {
  let _ = valkyrie.shutdown(pool.conn, pool.timeout)
  Nil
}

// ------------------------------------------------------------- Private Functions

/// Extracts Redis name, URL and pool size from a store config.
/// Panics if called with a non-Redis store or if required
/// environment variables are not set.
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
