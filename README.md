# ⚡️ Omega

A high-performance, concurrent cache for Rust, featuring lock-free slot management, epoch-based memory reclamation, and
TTL support.

[![Omega CI](https://github.com/dmitriiantonov/omega/actions/workflows/ci.yml/badge.svg)](https://github.com/dmitriiantonov/omega/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/rust-1.80%2B-blue.svg)](https://www.rust-lang.org)

## 🚀 Overview

Omega is a flexible, concurrent caching library designed for high-performance applications in Rust. It provides a
generic cache interface with pluggable engines and admission policies, allowing
customization for different use cases. The cache supports lock-free operations, efficient memory management using
epochs, and time-to-live (TTL) for entries.

## ✨ Features

- **🔒 Lock-free slot management**: High concurrency with lock-free data structures, ensuring minimal contention.
- **⚙️ Pluggable Engines**:
    - **Clock**: An efficient approximation of LRU using a clock hand.
    - **S3FIFO**: A state-of-the-art eviction algorithm that combines FIFO queues with a ghost cache for superior hit rates.
- **🛡️ Admission Policies**: Prevent cache pollution with policies like:
    - **Frequent**: Uses a Count-Min Sketch to only admit entries that are frequently accessed.
    - **Always**: Admits all entries.
- **♻️ Epoch-based memory reclamation**: Safe and efficient memory management using `crossbeam-epoch`.
- **⏳ TTL support**: Automatic expiration of cache entries based on time.
- **📊 High performance**: Multi-sharded metrics and optimized data structures for low-latency.
- **📝 Declarative Macros**: Simple `cache!` macro for complex configurations.

## 📦 Installation

Add Omega to your `Cargo.toml`:

```toml
[dependencies]
omega = "0.1.0"
```

## 🛠 Usage

Omega provides a powerful `cache!` macro to configure cache engines and admission policies.

### 🧩 Example: S3FIFO with Frequent Admission

```rust
use omega::core::backoff::BackoffPolicy;
use omega::cache;

let cache = cache!(
    engine: S3FIFO {
        capacity: 10000,
        backoff: { policy: BackoffPolicy::Exponential, limit: 10 },
        metrics: { shards: 8, latency_samples: 1024 }
    },
    admission: Frequent {
        count_min_sketch: { width: 1024, depth: 4 },
        decay_threshold: 1000
    }
);

cache.insert("key".to_string(), "value".to_string());

if let Some(entry) = cache.get(&"key".to_string()) {
    assert_eq!("value", entry.value());
}
```

### 🧩 Example: Clock with Always Admission

```rust
use omega::core::backoff::BackoffPolicy;
use omega::cache;

let cache = cache!(
    engine: Clock {
        capacity: 100,
        backoff: { policy: BackoffPolicy::Linear, limit: 5 },
        metrics: { shards: 4, latency_samples: 512 }
    },
    admission: Always
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

Omega tracks comprehensive performance metrics with low overhead using sharded counters and HDR histograms for latency:

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