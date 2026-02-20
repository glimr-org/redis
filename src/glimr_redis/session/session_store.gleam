//// Redis Session Store
////
//// Redis is ideal for session storage in multi-instance
//// deployments — all app servers share the same Redis and
//// sessions survive restarts without disk I/O. Unlike the
//// PostgreSQL and SQLite stores, Redis handles expiration
//// natively via key TTLs, so the GC callback is a no-op and
//// stale sessions are evicted automatically without periodic
//// cleanup scans.
////

import gleam/dict
import gleam/option.{None, Some}
import glimr/config/session as session_config
import glimr/session/payload
import glimr/session/store.{type SessionStore}
import glimr_redis/cache/pool.{type Pool}
import valkyrie

// ------------------------------------------------------------- Internal Public Functions

/// Captures the pool and lifetime in closures so the session
/// middleware never touches Redis directly — it just calls the
/// store interface. The GC callback is a no-op because Redis
/// evicts expired keys automatically via TTL. The cookie_value
/// callback returns the bare session ID because the actual data
/// lives in Redis, not in the cookie. Config is read once here
/// at boot rather than on every request.
///
@internal
pub fn create(pool: Pool) -> SessionStore {
  let config = session_config.load()
  let lifetime = config.lifetime

  store.new(
    load: fn(session_id) { load(pool, session_id) },
    save: fn(session_id, data, flash) {
      save(pool, session_id, data, flash, lifetime)
    },
    destroy: fn(session_id) { destroy(pool, session_id) },
    gc: fn() { Nil },
    cookie_value: fn(id, _, _) { id },
  )
}

// ------------------------------------------------------------- Private Functions

/// Prefixes the session ID with the pool's namespace and a
/// ":session:" segment so session keys don't collide with
/// cache keys or other data stored in the same Redis instance.
/// The prefix comes from the cache config, ensuring each app
/// or environment gets its own keyspace.
///
fn session_key(pool: Pool, session_id: String) -> String {
  pool.get_prefix(pool) <> ":session:" <> session_id
}

/// A simple GET — Redis returns nil for expired or missing keys,
/// so there's no need for a separate expiration check like the
/// SQL stores do. Falling back to empty dicts on any error means
/// a missing or expired session degrades to a fresh one rather
/// than crashing the request.
///
fn load(
  pool: Pool,
  session_id: String,
) -> #(dict.Dict(String, String), dict.Dict(String, String)) {
  let conn = pool.get_connection(pool)
  let timeout = pool.get_timeout(pool)
  let key = session_key(pool, session_id)

  case valkyrie.get(conn, key, timeout) {
    Ok(payload_json) -> payload.decode(payload_json)
    Error(_) -> #(dict.new(), dict.new())
  }
}

/// SET with EX (expiry in seconds) is atomic — it writes the
/// payload and sets the TTL in one command, so there's no window
/// where the key exists without an expiration. The TTL is reset
/// on every save, so active sessions stay alive while idle ones
/// expire automatically. No upsert logic needed because Redis
/// SET overwrites by default.
///
fn save(
  pool: Pool,
  session_id: String,
  data: dict.Dict(String, String),
  flash: dict.Dict(String, String),
  lifetime: Int,
) -> Nil {
  let conn = pool.get_connection(pool)
  let timeout = pool.get_timeout(pool)
  let key = session_key(pool, session_id)
  let encoded = payload.encode(data, flash)
  let ttl_seconds = lifetime * 60

  let options =
    Some(valkyrie.SetOptions(
      existence_condition: None,
      return_old: False,
      expiry_option: Some(valkyrie.ExpirySeconds(ttl_seconds)),
    ))

  let _ = valkyrie.set(conn, key, encoded, options, timeout)
  Nil
}

/// Deletes the key immediately so the old session ID can never
/// be reused — important after invalidation to prevent session
/// fixation attacks. DEL is idempotent; if the key already
/// expired or was never created, the command simply returns
/// zero keys deleted.
///
fn destroy(pool: Pool, session_id: String) -> Nil {
  let conn = pool.get_connection(pool)
  let timeout = pool.get_timeout(pool)
  let key = session_key(pool, session_id)

  let _ = valkyrie.del(conn, [key], timeout)
  Nil
}
