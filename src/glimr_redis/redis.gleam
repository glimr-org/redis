//// Redis Cache Entry Point
////
//// This module exists to simplify Redis pool initialization by
//// hiding the config loading and store lookup machinery from
//// consumers. Without this, every call site would need to load
//// the cache config, find the right store, and start the pool
//// themselves, leading to repetitive boilerplate.
////

import glimr/cache/driver
import glimr/config/cache
import glimr_redis/cache/pool.{type Pool}

// ------------------------------------------------------------- Public Functions

/// Convenience function that handles the full initialization
/// flow so callers don't need to understand the config system.
/// Panics on missing stores to fail fast during app startup
/// rather than silently returning an unusable pool.
///
pub fn start(name: String) -> Pool {
  let stores = cache.load()
  let store = driver.find_by_name(name, stores)
  pool.start_pool(store)
}
