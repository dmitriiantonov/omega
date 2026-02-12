# Omega

A high-performance, concurrent cache for Rust, featuring lock-free slot management, epoch-based memory reclamation, and
TTL support.

## Overview

Omega is a flexible, concurrent caching library designed for high-performance applications in Rust. It provides a
generic cache interface with pluggable engines (e.g., Clock) and admission policies (e.g., Frequent), allowing
customization for different use cases. The cache supports lock-free operations, efficient memory management using
epochs, and time-to-live (TTL) for entries.

## Features

- **Lock-free slot management**: Ensures high concurrency without blocking.
- **Epoch-based memory reclamation**: Safe and efficient handling of memory in concurrent environments using
  `crossbeam-epoch`.
- **TTL support**: Automatic expiration of cache entries.
- **Pluggable components**: Customizable cache engines and admission policies.
- **High performance**: Optimized for low-latency access and insertion.
- **Macros for easy configuration**: Use the `cache!` macro to instantiate caches with declarative syntax.

## Installation

Add Omega to your `Cargo.toml` as a Git dependency:

```toml
[dependencies]
omega = { git = "https://github.com/dmitriiantonov/omega.git" }
```

## Usage

Here's a basic example using the `cache!` macro:

```rust
use omega::cache;

let cache = cache!(
    engine: Clock {
        capacity: 100,
        backoff: { policy: BackoffPolicy::Exponential, limit: 10 }
    },
    admission: Frequent {
        count_min_sketch: { width: 1024, height: 4 },
        decay_threshold: 1000
    }
);

cache.insert(1, "value".to_string());

if let Some(entry) = cache.get(&1) {
    assert_eq!("value", entry.value());
}

cache.remove(&1);
```

### Key Methods

- `insert(key: K, value: V)`: Inserts a key-value pair, potentially evicting another based on policy.
- `get(&key: Q) -> Option<EntryRef>`: Retrieves an entry reference.
- `remove(&key: Q) -> bool`: Removes and returns if present.

## Dependencies

- `rand`
- `smallvec`
- `crossbeam`
- `dashmap`
- `twox-hash`
- `fastrand`
- `crossbeam-epoch`

## Development

The project is actively developed, with recent updates including rewritten tests and simplified clock cache logic (as of
February 11, 2026).

## Building and Testing

```sh
cargo build
cargo test
```

## Contributing

Fork, make changes, and submit a pull request. Follow Rust best practices and add tests.

## License

MIT License. See [LICENSE](LICENSE) for details.