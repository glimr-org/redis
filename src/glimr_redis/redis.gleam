//// Redis Cache Entry Point
////
//// Provides the main entry point for starting Redis cache 
//// pools. Use this module to initialize Redis cache stores 
//// defined in your config_cache.gleam.

import glimr/cache/driver.{type CacheStore}
import glimr_redis/cache/pool.{type Pool}

// ------------------------------------------------------------- Public Functions

/// Starts a Redis cache pool for the named store.
/// Looks up the store configuration by name from the provided
/// list and initializes a connection pool.
///
/// ## Example
///
/// ```gleam
/// import glimr_redis/redis
/// import config/config_cache
///
/// let pool = redis.start("main", config_cache.stores())
/// ```
///
pub fn start(name: String, stores: List(CacheStore)) -> Pool {
  let store = driver.find_by_name(name, stores)
  pool.start_pool(store)
}
