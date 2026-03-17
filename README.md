# ⚡️ Omega Cache

A high-performance, concurrent cache for Rust, featuring lock-free slot management, epoch-based memory reclamation, and
TTL support.

[![Omega Cache CI](https://github.com/dmitriiantonov/omega-cache/actions/workflows/ci.yml/badge.svg)](https://github.com/dmitriiantonov/omega-cache/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/rust-1.80%2B-blue.svg)](https://www.rust-lang.org)

## 🚀 Overview

Omega Cache is a flexible, concurrent caching library designed for high-performance applications in Rust. It provides a
generic cache interface with pluggable engines, allowing
customization for different use cases. The cache supports lock-free operations, efficient memory management using
epochs, and time-to-live (TTL) for entries.

## ✨ Features

- **🔒 Lock-free slot management**: High concurrency with lock-free data structures, ensuring minimal contention.
- **⚙️ Pluggable Engines**:
    - **Clock**: An efficient approximation of LRU using a clock hand and concurrent state management.
    - **S3FIFO**: A state-of-the-art eviction algorithm that combines three FIFO queues with a ghost cache for superior
      hit rates.
- **♻️ Epoch-based memory reclamation**: Safe and efficient memory management using `crossbeam-epoch`.
- **⏳ TTL support**: Automatic expiration of cache entries based on time.
- **📊 High performance**: Multi-sharded metrics and optimized data structures for low-latency.
- **📝 Declarative Macros**: Simple `cache!` macro for complex configurations.

## 📦 Installation

Add Omega Cache to your `Cargo.toml`:

```toml
[dependencies]
omega-cache = "0.2.1"
```

## 🛠 Usage

Omega Cache provides a powerful `cache!` macro to configure cache engines and admission policies.

### 🧩 Example: S3FIFO Cache

```rust
use omega_cache::core::backoff::BackoffPolicy;
use omega_cache::cache;

let cache = cache!(
    engine: S3FIFO {
        capacity: 10000,
        metrics: { shards: 16, latency_samples: 1024 }
    },
    backoff: { policy: BackoffPolicy::Exponential, limit: 10 }
);

cache.insert("key".to_string(), "value".to_string());

if let Some(entry) = cache.get(&"key".to_string()) {
    assert_eq!("value", entry.value());
}
```

### 🧩 Example: Clock Cache

```rust
use omega_cache::core::backoff::BackoffPolicy;
use omega_cache::cache;

let cache = cache!(
    engine: Clock {
        capacity: 1024,
        metrics: { shards: 4, latency_samples: 512 }
    },
    backoff: { policy: BackoffPolicy::Linear, limit: 5 }
);
```

## 🔍 Key Methods

- `insert(key: K, value: V)`: Inserts a key-value pair.
- `get(&key: Q) -> Option<Ref<K, V>>`: Retrieves a guarded reference (`Ref`) to an entry.
- `remove(&key: Q) -> bool`: Removes an entry if it exists.
- `metrics() -> MetricsSnapshot`: Returns a snapshot of performance metrics.

### ⚓ Entry Reference (`Ref<K, V>`)

The `Ref` type is a smart pointer that keeps the entry's memory pinned during its lifetime using epoch-based reclamation.
- `key()`: Returns a reference to the entry's key.
- `value()`: Returns a reference to the entry's value.
- `is_expired()`: Checks if the entry has timed out.

## 📈 Metrics

Omega Cache tracks comprehensive performance metrics with low overhead using sharded counters and HDR histograms for
latency:

- **Hit/Miss Rate**: Track the effectiveness of your eviction policy.
- **Eviction Count**: Monitor how often entries are being pushed out.
- **Latency Percentiles**: P50, P90, P99, and P99.9 latency tracking for both read and write operations.

```rust
let snapshot = cache.metrics();
println!("Hit rate: {:.2}%", snapshot.hit_rate() * 100.0);
println!("P99 Latency: {}ns", snapshot.latency(LatencyPercentile::P99));
```

## ⚖️ License

MIT License. See [LICENSE](LICENSE) for details.