//// Redis Entry Point
////
//// Application boot needs to wire up cache pools and session
//// stores, but the underlying config parsing and pool
//// construction are spread across several internal modules.
//// This module is the public entry point that ties them
//// together — each function takes a name or pool and returns
//// a ready-to-use resource, so the app's main module reads as
//// a simple sequence of start calls rather than manual config
//// loading and plumbing.
////

import glimr/cache/driver
import glimr/config/cache
import glimr/session/store
import glimr_redis/cache/pool.{type Pool}
import glimr_redis/session/session_store

// ------------------------------------------------------------- Public Functions

/// Loads the cache config, finds the named store, and starts
/// a pool in one call so the app boot code doesn't need to
/// touch config parsing or driver types directly. A missing
/// store name crashes at boot — failing fast here gives a
/// clear error instead of propagating a broken pool through
/// every downstream function.
///
pub fn start(name: String) -> Pool {
  let stores = cache.load()
  let store = driver.find_by_name(name, stores)

  pool.start_pool(store)
}

/// Registers the Redis session store in persistent_term so the
/// session middleware can load, save, and destroy sessions
/// without knowing which backend is active. Redis handles
/// expiration natively via key TTLs, so unlike the PostgreSQL
/// and SQLite stores the GC callback is a no-op — no manual
/// cleanup needed. This must be called at boot before any
/// requests arrive.
///
pub fn start_session(pool: Pool) -> Nil {
  let session = session_store.create(pool)

  store.cache_store(session)
}
