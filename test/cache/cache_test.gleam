//// Redis Cache Tests
////
//// These tests require a running Redis server at localhost:6379.
//// Set REDIS_URL environment variable to override the default.
//// Skip tests if Redis is not available.

import gleam/dynamic/decode
import gleam/json
import gleeunit/should
import glimr/cache/cache.{ComputeError, NotFound, SerializationError}
import glimr_redis/cache/cache as redis_cache
import glimr_redis/redis
import simplifile

const test_redis_url = "redis://localhost:6379"

const config_dir = "config"

const config_file = "config/cache.toml"

fn setup_config() -> Nil {
  let _ = simplifile.create_directory_all(config_dir)
  let _ = simplifile.write(config_file, "[stores.test]
  driver = \"redis\"
  url = \"" <> test_redis_url <> "\"
  pool_size = 5

[stores.other]
  driver = \"redis\"
  url = \"" <> test_redis_url <> "\"
  pool_size = 5
")
  Nil
}

fn setup_test_pool() {
  setup_config()
  redis.start("test")
}

// ------------------------------------------------------------- get/put

pub fn put_and_get_test() {
  let pool = setup_test_pool()

  redis_cache.put(pool, "test_key", "test_value", 3600)
  |> should.be_ok()

  redis_cache.get(pool, "test_key")
  |> should.be_ok()
  |> should.equal("test_value")

  // Cleanup
  let _ = redis_cache.forget(pool, "test_key")
}

pub fn get_not_found_test() {
  let pool = setup_test_pool()

  redis_cache.get(pool, "nonexistent_key_abc123")
  |> should.be_error()
  |> should.equal(NotFound)
}

pub fn put_overwrites_existing_test() {
  let pool = setup_test_pool()

  redis_cache.put(pool, "overwrite_key", "first_value", 3600)
  |> should.be_ok()

  redis_cache.put(pool, "overwrite_key", "second_value", 3600)
  |> should.be_ok()

  redis_cache.get(pool, "overwrite_key")
  |> should.be_ok()
  |> should.equal("second_value")

  // Cleanup
  let _ = redis_cache.forget(pool, "overwrite_key")
}

// ------------------------------------------------------------- put_forever

pub fn put_forever_test() {
  let pool = setup_test_pool()

  redis_cache.put_forever(pool, "permanent_key", "permanent_value")
  |> should.be_ok()

  redis_cache.get(pool, "permanent_key")
  |> should.be_ok()
  |> should.equal("permanent_value")

  // Cleanup
  let _ = redis_cache.forget(pool, "permanent_key")
}

// ------------------------------------------------------------- forget

pub fn forget_existing_key_test() {
  let pool = setup_test_pool()

  redis_cache.put(pool, "to_delete", "value", 3600)
  |> should.be_ok()

  redis_cache.forget(pool, "to_delete")
  |> should.be_ok()

  redis_cache.get(pool, "to_delete")
  |> should.be_error()
  |> should.equal(NotFound)
}

pub fn forget_nonexistent_key_test() {
  let pool = setup_test_pool()

  // Should not error when deleting non-existent key
  redis_cache.forget(pool, "never_existed_xyz789")
  |> should.be_ok()
}

// ------------------------------------------------------------- has

pub fn has_existing_key_test() {
  let pool = setup_test_pool()

  redis_cache.put(pool, "exists_key", "value", 3600)
  |> should.be_ok()

  redis_cache.has(pool, "exists_key")
  |> should.equal(True)

  // Cleanup
  let _ = redis_cache.forget(pool, "exists_key")
}

pub fn has_nonexistent_key_test() {
  let pool = setup_test_pool()

  redis_cache.has(pool, "does_not_exist_456")
  |> should.equal(False)
}

// ------------------------------------------------------------- pull

pub fn pull_existing_key_test() {
  let pool = setup_test_pool()

  redis_cache.put(pool, "pull_key", "pull_value", 3600)
  |> should.be_ok()

  redis_cache.pull(pool, "pull_key")
  |> should.be_ok()
  |> should.equal("pull_value")

  // Key should be gone after pull
  redis_cache.has(pool, "pull_key")
  |> should.equal(False)
}

pub fn pull_nonexistent_key_test() {
  let pool = setup_test_pool()

  redis_cache.pull(pool, "nonexistent_pull_key")
  |> should.be_error()
  |> should.equal(NotFound)
}

// ------------------------------------------------------------- increment/decrement

pub fn increment_new_key_test() {
  let pool = setup_test_pool()

  redis_cache.increment(pool, "counter_new", 1)
  |> should.be_ok()
  |> should.equal(1)

  // Cleanup
  let _ = redis_cache.forget(pool, "counter_new")
}

pub fn increment_existing_key_test() {
  let pool = setup_test_pool()

  redis_cache.increment(pool, "counter_inc", 1) |> should.be_ok()
  redis_cache.increment(pool, "counter_inc", 1) |> should.be_ok()

  redis_cache.increment(pool, "counter_inc", 1)
  |> should.be_ok()
  |> should.equal(3)

  // Cleanup
  let _ = redis_cache.forget(pool, "counter_inc")
}

pub fn increment_by_amount_test() {
  let pool = setup_test_pool()

  redis_cache.increment(pool, "counter_amt", 5)
  |> should.be_ok()
  |> should.equal(5)

  redis_cache.increment(pool, "counter_amt", 10)
  |> should.be_ok()
  |> should.equal(15)

  // Cleanup
  let _ = redis_cache.forget(pool, "counter_amt")
}

pub fn decrement_test() {
  let pool = setup_test_pool()

  redis_cache.increment(pool, "counter_dec", 10) |> should.be_ok()

  redis_cache.decrement(pool, "counter_dec", 3)
  |> should.be_ok()
  |> should.equal(7)

  // Cleanup
  let _ = redis_cache.forget(pool, "counter_dec")
}

pub fn decrement_below_zero_test() {
  let pool = setup_test_pool()

  redis_cache.decrement(pool, "counter_neg", 5)
  |> should.be_ok()
  |> should.equal(-5)

  // Cleanup
  let _ = redis_cache.forget(pool, "counter_neg")
}

// ------------------------------------------------------------- JSON operations

type User {
  User(name: String, age: Int)
}

fn user_encoder(user: User) -> json.Json {
  json.object([
    #("name", json.string(user.name)),
    #("age", json.int(user.age)),
  ])
}

fn user_decoder() -> decode.Decoder(User) {
  use name <- decode.field("name", decode.string)
  use age <- decode.field("age", decode.int)
  decode.success(User(name:, age:))
}

pub fn put_json_and_get_json_test() {
  let pool = setup_test_pool()
  let user = User(name: "Alice", age: 30)

  redis_cache.put_json(pool, "json_user", user, user_encoder, 3600)
  |> should.be_ok()

  redis_cache.get_json(pool, "json_user", user_decoder())
  |> should.be_ok()
  |> should.equal(user)

  // Cleanup
  let _ = redis_cache.forget(pool, "json_user")
}

pub fn put_json_forever_test() {
  let pool = setup_test_pool()
  let user = User(name: "Bob", age: 25)

  redis_cache.put_json_forever(pool, "permanent_json_user", user, user_encoder)
  |> should.be_ok()

  redis_cache.get_json(pool, "permanent_json_user", user_decoder())
  |> should.be_ok()
  |> should.equal(user)

  // Cleanup
  let _ = redis_cache.forget(pool, "permanent_json_user")
}

pub fn get_json_invalid_format_test() {
  let pool = setup_test_pool()

  // Store invalid JSON for User type
  redis_cache.put(pool, "invalid_json", "not valid json", 3600)
  |> should.be_ok()

  redis_cache.get_json(pool, "invalid_json", user_decoder())
  |> should.be_error()
  |> should.equal(SerializationError("Failed to decode JSON"))

  // Cleanup
  let _ = redis_cache.forget(pool, "invalid_json")
}

// ------------------------------------------------------------- remember

pub fn remember_returns_cached_value_test() {
  let pool = setup_test_pool()

  redis_cache.put(pool, "cached_remember", "existing_value", 3600)
  |> should.be_ok()

  // Compute function should not be called
  redis_cache.remember(pool, "cached_remember", 3600, fn() {
    Ok("computed_value")
  })
  |> should.be_ok()
  |> should.equal("existing_value")

  // Cleanup
  let _ = redis_cache.forget(pool, "cached_remember")
}

pub fn remember_computes_when_missing_test() {
  let pool = setup_test_pool()

  redis_cache.remember(pool, "missing_remember", 3600, fn() {
    Ok("computed_value")
  })
  |> should.be_ok()
  |> should.equal("computed_value")

  // Value should now be cached
  redis_cache.get(pool, "missing_remember")
  |> should.be_ok()
  |> should.equal("computed_value")

  // Cleanup
  let _ = redis_cache.forget(pool, "missing_remember")
}

pub fn remember_handles_compute_error_test() {
  let pool = setup_test_pool()

  redis_cache.remember(pool, "will_fail_remember", 3600, fn() {
    Error("compute failed")
  })
  |> should.be_error()
  |> should.equal(ComputeError("Compute function failed"))
}

pub fn remember_forever_test() {
  let pool = setup_test_pool()

  redis_cache.remember_forever(pool, "permanent_remember", fn() {
    Ok("computed")
  })
  |> should.be_ok()
  |> should.equal("computed")

  redis_cache.get(pool, "permanent_remember")
  |> should.be_ok()
  |> should.equal("computed")

  // Cleanup
  let _ = redis_cache.forget(pool, "permanent_remember")
}

// ------------------------------------------------------------- remember_json

pub fn remember_json_returns_cached_test() {
  let pool = setup_test_pool()
  let user = User(name: "Cached", age: 40)

  redis_cache.put_json(pool, "user_cached_json", user, user_encoder, 3600)
  |> should.be_ok()

  redis_cache.remember_json(
    pool,
    "user_cached_json",
    3600,
    user_decoder(),
    fn() { Ok(User(name: "Computed", age: 99)) },
    user_encoder,
  )
  |> should.be_ok()
  |> should.equal(user)

  // Cleanup
  let _ = redis_cache.forget(pool, "user_cached_json")
}

pub fn remember_json_computes_when_missing_test() {
  let pool = setup_test_pool()
  let user = User(name: "New", age: 20)

  redis_cache.remember_json(
    pool,
    "new_user_json",
    3600,
    user_decoder(),
    fn() { Ok(user) },
    user_encoder,
  )
  |> should.be_ok()
  |> should.equal(user)

  // Should now be cached
  redis_cache.get_json(pool, "new_user_json", user_decoder())
  |> should.be_ok()
  |> should.equal(user)

  // Cleanup
  let _ = redis_cache.forget(pool, "new_user_json")
}

// ------------------------------------------------------------- flush

pub fn flush_removes_all_pool_keys_test() {
  let pool = setup_test_pool()

  // Add multiple keys
  redis_cache.put(pool, "flush_key1", "value1", 3600) |> should.be_ok()
  redis_cache.put(pool, "flush_key2", "value2", 3600) |> should.be_ok()
  redis_cache.put(pool, "flush_key3", "value3", 3600) |> should.be_ok()

  // Verify they exist
  redis_cache.has(pool, "flush_key1") |> should.equal(True)
  redis_cache.has(pool, "flush_key2") |> should.equal(True)
  redis_cache.has(pool, "flush_key3") |> should.equal(True)

  // Flush
  redis_cache.flush(pool)
  |> should.be_ok()

  // Verify they're gone
  redis_cache.has(pool, "flush_key1") |> should.equal(False)
  redis_cache.has(pool, "flush_key2") |> should.equal(False)
  redis_cache.has(pool, "flush_key3") |> should.equal(False)
}

pub fn flush_empty_cache_succeeds_test() {
  let pool = setup_test_pool()

  // Flush when nothing exists should succeed
  redis_cache.flush(pool)
  |> should.be_ok()
}

pub fn flush_only_affects_pool_prefix_test() {
  let pool = setup_test_pool()

  // Create a second pool with different name (config already has "other" store)
  let other_pool = redis.start("other")

  // Add keys to both pools
  redis_cache.put(pool, "test_flush_key", "test_value", 3600) |> should.be_ok()
  redis_cache.put(other_pool, "other_flush_key", "other_value", 3600)
  |> should.be_ok()

  // Flush only the first pool
  redis_cache.flush(pool)
  |> should.be_ok()

  // First pool's key should be gone
  redis_cache.has(pool, "test_flush_key") |> should.equal(False)

  // Other pool's key should still exist
  redis_cache.has(other_pool, "other_flush_key") |> should.equal(True)

  // Cleanup
  let _ = redis_cache.forget(other_pool, "other_flush_key")
}
