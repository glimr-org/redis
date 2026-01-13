# Glimr Redis Driver ✨

The official Redis cache driver for the Glimr web framework, providing connection pooling and cache operations. Compatible with Redis, Valkey, KeyDB, and Dragonfly. This package is meant to be used alongside the `valkyrie` Redis client and the `glimr-org/framework` package.

If you'd like to stay updated on Glimr's development, Follow [@migueljarias](https://x.com/migueljarias) on X (that's me) for updates.

## About

> **Note:** This repository contains the Redis cache driver for Glimr. If you want to build an application using Glimr, visit the main [Glimr repository](https://github.com/glimr-org/glimr).

## Features

- **Connection Pooling** - Efficient connection management with automatic checkout/checkin
- **Cache Operations** - Full cache API (get, put, forget, increment, remember, etc.)
- **TTL Support** - Automatic expiration with configurable time-to-live
- **JSON Serialization** - Store and retrieve complex data structures
- **HTTP Context** - Easy pool access in web request handlers

## Installation

Add the Redis driver to your Gleam project:

```sh
gleam add glimr_redis
```

## Learn More

- [Glimr](https://github.com/glimr-org/glimr) - Main Glimr repository
- [Glimr Framework](https://github.com/glimr-org/framework) - Core framework
- [Valkyrie](https://hexdocs.pm/valkyrie/) - Redis client for Gleam

### Built With

- [**valkyrie**](https://hexdocs.pm/valkyrie/) - Redis client library for Gleam
- [**bath**](https://hexdocs.pm/bath/) - Connection pooling for Gleam
- [**gleam_otp**](https://hexdocs.pm/gleam_otp/) - OTP support for process management

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

The Glimr Redis driver is open-sourced software licensed under the [MIT](https://opensource.org/license/MIT) license.
