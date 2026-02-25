//// Redis Entry Point
////
//// Application boot needs to wire up cache pools and session
//// stores, but the underlying config parsing and pool
//// construction are spread across several internal modules.
//// This module is the public entry point that ties them
//// together — each function takes a name or pool and returns a
//// ready-to-use resource, so the app's main module reads as a
//// simple sequence of start calls rather than manual config
//// loading and plumbing.
////

import glimr/cache/cache.{type CachePool}
import glimr/cache/driver.{type CacheStore}
import glimr/config/cache as cache_config
import glimr/session/session.{type Session}
import glimr/session/store
import glimr_redis/cache/cache as redis_cache
import glimr_redis/cache/pool.{type Pool}
import glimr_redis/session/session_store

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

/// Session middleware needs to load, save, and destroy sessions
/// on every request, but it shouldn't care whether Redis,
/// PostgreSQL, or SQLite is behind it. Registering the store in
/// persistent_term at boot means the middleware just asks "give
/// me the session store" and gets one — no config lookups on
/// every request. Redis is nice here because key TTLs handle
/// expiration automatically, so unlike the database stores
/// there's no garbage collection callback to worry about.
///
pub fn start_session(redis_pool: Pool) -> Session {
  let session = session_store.create(redis_pool)
  store.cache_store(session)

  session.empty()
}

// ------------------------------------------------------------- Internal Public Functions

/// The public start() function wraps the pool into a CachePool
/// immediately, but session setup needs the raw Pool type
/// because the session store talks to Redis directly rather
/// than going through the cache abstraction. Exposing this as
/// @internal lets both paths share the same config loading
/// without duplicating it.
///
@internal
pub fn start_pool(name: String) -> Pool {
  let stores = cache_config.load()
  let store = driver.find_by_name(name, stores)

  pool.start_pool(store)
}

/// Console commands like `glimr cache:clear` discover which
/// cache backend to use at runtime from the config. If the
/// Redis URL is wrong, start() would panic and crash the CLI
/// with a stack trace — not a great developer experience. This
/// variant returns a Result so the console can show a friendly
/// error message instead.
///
@internal
pub fn try_start_cache(store: CacheStore) -> Result(CachePool, String) {
  case pool.try_start_pool(store) {
    Ok(internal) -> Ok(wrap_pool(internal))
    Error(msg) -> Error(msg)
  }
}

/// The framework's CachePool is backend-agnostic — it just
/// holds closures for each operation. This is where we wire the
/// Redis-specific implementations into those closures,
/// capturing the internal pool so callers never need to know
/// Redis is involved. Both start() and try_start_cache() funnel
/// through here to avoid duplicating the wiring.
///
@internal
pub fn wrap_pool(internal: Pool) -> CachePool {
  cache.new_pool(
    get: fn(key) { redis_cache.get(internal, key) },
    put: fn(key, value, ttl) { redis_cache.put(internal, key, value, ttl) },
    put_forever: fn(key, value) {
      redis_cache.put_forever(internal, key, value)
    },
    forget: fn(key) { redis_cache.forget(internal, key) },
    flush: fn() { redis_cache.flush(internal) },
    increment: fn(key, by) { redis_cache.increment(internal, key, by) },
    has: fn(key) { redis_cache.has(internal, key) },
    stop: fn() { pool.stop_pool(internal) },
  )
}
