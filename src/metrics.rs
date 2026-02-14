//! # High-Performance Sharded Metrics
//!
//! This module provides a lock-free, sharded metrics collection system optimized for
//! throughput-intensive applications where cache contention is the primary bottleneck.
//!
//! ## Design Principles
//! 1. **Contention Mitigation:** Uses `THREAD_ID` based sharding to ensure that
//!    concurrent increments likely hit different cache lines.
//! 2. **False Sharing Prevention:** Explicitly pads internal structures to 64-byte
//!    boundaries to prevent CPU cache line invalidation.
//! 3. **Non-Blocking Writes:** Uses `Relaxed` atomic ordering to ensure metrics
//!    collection adds negligible overhead to the hot path.
use crossbeam::utils::CachePadded;
use hdrhistogram::Histogram;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicU64, AtomicUsize};

/// Default number of shards if not specified.
pub const DEFAULT_SHARDS: usize = 4;

/// Default capacity for the circular latency buffer.
pub const DEFAULT_LATENCY_SAMPLES: usize = 256;

static NEXT_ID: AtomicUsize = AtomicUsize::new(1);

thread_local! {
    /// Each thread is assigned a unique ID on its first metrics call to determine
    /// its shard affinity.
    static THREAD_ID: usize = NEXT_ID.fetch_add(1, Relaxed);
}

/// Computes the shard index for the current thread.
///
/// Uses a bitwise AND mask, requiring `shards` to be a power of two.
#[inline]
fn get_shard_index(shards: usize) -> usize {
    let thread_id = THREAD_ID.with(|id| *id);
    let mask = shards - 1;
    thread_id & mask
}

/// Configuration parameters for the [`Metrics`] collector.
#[derive(Debug)]
pub struct MetricsConfig {
    /// Total shards for metrics distribution. Must be a power of two.
    shards: usize,
    /// Capacity of the circular buffer per shard. Must be a power of two.
    latency_samples: usize,
}

impl MetricsConfig {
    /// Creates a new configuration, enforcing power-of-two constraints.
    ///
    /// # Parameters
    /// - `shards`: Target shard count (rounded up to nearest $2^n$).
    /// - `latency_samples`: Target buffer size (rounded up to nearest $2^n$).
    #[inline]
    pub fn new(shards: usize, latency_samples: usize) -> Self {
        Self {
            shards: shards.next_power_of_two(),
            latency_samples: latency_samples.next_power_of_two(),
        }
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            shards: DEFAULT_SHARDS,
            latency_samples: DEFAULT_LATENCY_SAMPLES,
        }
    }
}

/// The telemetry engine for the cache, providing high-concurrency event tracking.
///
/// `Metrics` is designed to be owned by the cache and serves as a thread-safe
/// sink for all operational telemetry. It supports high-frequency invocation
/// across multiple threads by utilizing a sharded, lock-free architecture.
///
/// # Concurrency & Multi-Threaded Ownership
/// Although owned by a single cache instance, `Metrics` is designed to be
/// accessed through shared references (`&self`) across all threads interacting
/// with the cache. It leverages internal mutability via atomics to allow
/// concurrent updates without requiring a `Mutex` or `RwLock` at the cache level.
///
/// # Throughput Scalability
/// To prevent metrics from becoming a point of contention in multi-core
/// environments, writes are distributed across independent [`MetricsStorage`]
/// shards. A thread-local affinity mechanism maps worker threads to specific
/// shards, localizing atomic increments and minimizing cross-core
/// synchronization overhead.
///
///
///
/// # Cache-Line Isolation
/// Each shard is explicitly wrapped in [`CachePadded`] to ensure it occupies
/// unique cache lines. This physical isolation prevents "false sharing,"
/// where unrelated updates to different shards would otherwise trigger
/// expensive CPU cache-coherency protocols and degrade cache performance.
///
/// # Operational Profile
/// - **Wait-Free Writes:** All recording operations are wait-free, ensuring
///   telemetry collection never blocks cache lookups or insertions.
/// - **Eventually Consistent Snapshots:** The [`snapshot()`] method provides
///   a point-in-time aggregation of all shards. While snapshots are linear
///   in complexity relative to shard count, recording remains $O(1)$.
#[derive(Debug)]
pub struct Metrics {
    shards: Vec<CachePadded<MetricsStorage>>,
    config: MetricsConfig,
}

impl Metrics {
    /// Initializes the metrics engine with sharded storage.
    #[inline]
    pub fn new(config: MetricsConfig) -> Self {
        let shards = (0..config.shards)
            .map(|_| CachePadded::new(MetricsStorage::new(&config)))
            .collect::<Vec<_>>();

        Self { shards, config }
    }

    /// Increments the cache hit counter for the current thread's assigned shard.
    ///
    /// # Concurrency
    /// This is a **wait-free** $O(1)$ operation. It uses `Relaxed` atomic ordering,
    /// providing high throughput at the cost of strict sequential consistency.
    #[inline]
    pub fn record_hit(&self) {
        let shard_index = get_shard_index(self.config.shards);
        self.shards[shard_index].record_hit();
    }

    /// Increments the cache miss counter for the current thread's assigned shard.
    ///
    /// # Concurrency
    /// This is a **wait-free** $O(1)$ operation. It uses `Relaxed` atomic ordering,
    /// ensuring that telemetry collection does not stall cache lookups.
    #[inline]
    pub fn record_miss(&self) {
        let shard_index = get_shard_index(self.config.shards);
        self.shards[shard_index].record_miss();
    }

    /// Records a cache entry eviction for the current thread's assigned shard.
    ///
    /// This should be invoked whenever an item is removed from the cache to
    /// satisfy capacity constraints.
    #[inline]
    pub fn record_eviction(&self) {
        let shard_index = get_shard_index(self.config.shards);
        self.shards[shard_index].record_eviction();
    }

    /// Records a latency measurement into the sampler assigned to the current thread's shard.
    ///
    /// This method captures execution timing (e.g., lookup duration or insertion time)
    /// without blocking the calling thread or incurring the overhead of a global lock.
    ///
    /// # Concurrency & Progress
    /// This is a **wait-free** operation. It utilizes a thread-local shard lookup followed
    /// by an atomic fetch-and-add on the shard's write cursor. This ensures that even
    /// under extreme write contention, every thread makes independent progress.
    ///
    /// # Sampling Behavior
    /// Latency is recorded into a lossy, circular buffer ([`Sampler`]). If the buffer
    /// for the current shard is full, the oldest sample is overwritten. This "lossy"
    /// property is a deliberate design choice to bound memory usage and prioritize
    /// write throughput over absolute data retention.
    ///
    ///
    ///
    /// # Arguments
    /// * `latency` - The raw timing value to record. Units (ns, us, ms) should remain
    ///   consistent across all calls. A value of `0` is ignored during the [`snapshot()`]
    ///   aggregation to prevent uninitialized data from skewing percentiles.
    #[inline]
    pub fn record_latency(&self, latency: u64) {
        let shard_index = get_shard_index(self.config.shards);
        self.shards[shard_index].record_latency(latency);
    }

    /// Performs a non-destructive aggregation of all sharded metrics into a
    /// consistent [`MetricsSnapshot`].
    ///
    /// # Performance
    /// This method is $O(S + N)$, where $S$ is the number of shards and $N$ is
    /// the total capacity of the latency samplers. Because it iterates over
    /// all shards and populates an [`HdrHistogram`], it is significantly more
    /// expensive than recording methods and should typically be called from
    /// a background reporting thread.
    ///
    /// # Consistency
    /// The snapshot provides a **point-in-time** view. However, because shards
    /// are read sequentially without a global lock, the snapshot may not represent
    /// a single atomic instant across the entire cache. This is a standard
    /// trade-off in high-performance telemetry systems.
    #[inline]
    pub fn snapshot(&self) -> MetricsSnapshot {
        let mut hit_count: u64 = 0;
        let mut miss_count: u64 = 0;
        let mut eviction_count: u64 = 0;
        let mut latency_histogram: Histogram<u64> =
            Histogram::new_with_bounds(1, 3600, 2).expect("arguments are invalid");

        for metrics_storage in &self.shards {
            hit_count = hit_count.saturating_add(metrics_storage.hit_count());
            miss_count = miss_count.saturating_add(metrics_storage.miss_count());
            eviction_count = eviction_count.saturating_add(metrics_storage.eviction_count());

            metrics_storage.write_latency_samples(&mut latency_histogram);
        }

        MetricsSnapshot {
            hit_count,
            miss_count,
            eviction_count,
            latency_histogram,
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        let config = MetricsConfig {
            shards: DEFAULT_SHARDS,
            latency_samples: DEFAULT_LATENCY_SAMPLES,
        };

        Metrics::new(config)
    }
}

/// Common latency percentiles used for performance analysis.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LatencyPercentile {
    /// The 50th percentile, or the median value.
    P50,
    /// The 90th percentile.
    P90,
    /// The 99th percentile (standard tail latency metric).
    P99,
    /// The 99.9th percentile (extreme tail latency).
    P999,
}

impl LatencyPercentile {
    /// Returns the float value (0.0 - 1.0) required by HdrHistogram.
    fn as_quantile(&self) -> f64 {
        match self {
            Self::P50 => 0.50,
            Self::P90 => 0.90,
            Self::P99 => 0.99,
            Self::P999 => 0.999,
        }
    }
}

/// A read-only, point-in-time representation of [`Metrics`].
///
/// Created via [`Metrics::snapshot`] or [`Metrics::into`].
#[derive(Debug)]
pub struct MetricsSnapshot {
    hit_count: u64,
    miss_count: u64,
    eviction_count: u64,
    latency_histogram: Histogram<u64>,
}

impl MetricsSnapshot {
    /// Returns the total number of hits recorded at the time of the snapshot.
    #[inline]
    pub fn hit_count(&self) -> u64 {
        self.hit_count
    }

    /// Returns the total number of misses recorded at the time of the snapshot.
    #[inline]
    pub fn miss_count(&self) -> u64 {
        self.miss_count
    }

    /// Returns the total number of evictions recorded at the time of the snapshot.
    #[inline]
    pub fn eviction_count(&self) -> u64 {
        self.eviction_count
    }

    /// Calculates the miss rate ($misses / (hits + misses)$).
    ///
    /// Returns `0.0` if no activity was recorded to prevent division-by-zero errors.
    pub fn miss_rate(&self) -> f64 {
        let total = self.hit_count + self.miss_count;
        if total == 0 {
            0.0
        } else {
            self.miss_count as f64 / total as f64
        }
    }

    /// Calculates the hit rate ($hits / (hits + misses)$).
    ///
    /// Returns `0.0` if no activity was recorded.
    pub fn hit_rate(&self) -> f64 {
        let total = self.hit_count + self.miss_count;
        if total == 0 {
            0.0
        } else {
            self.hit_count as f64 / total as f64
        }
    }

    /// Returns the latency value at a specific percentile using a type-safe enum.
    ///
    /// This is the preferred method for monitoring cache performance, as it
    /// explicitly categorizes measurements into median, tail, and extreme tail latencies.
    ///
    /// # Performance
    /// The lookup is $O(1)$ relative to the number of samples in the histogram.
    #[inline]
    pub fn latency(&self, percentile: LatencyPercentile) -> u64 {
        self.latency_histogram
            .value_at_quantile(percentile.as_quantile())
    }
}

impl From<&Metrics> for MetricsSnapshot {
    fn from(metrics: &Metrics) -> Self {
        metrics.snapshot()
    }
}

/// Internal thread-safe container for a single shard's metric data.
///
/// `MetricsStorage` manages the primary counters (hits, misses, evictions) and
/// the latency sampler for a subset of the cache's traffic. It is designed to
/// be held within a [`CachePadded`] wrapper to ensure physical isolation
/// from other shards.
///
/// # Field-Level Isolation
/// To prevent internal contention between different types of events (e.g.,
/// a hit and an eviction happening on the same shard), each `AtomicU64`
/// counter is individually wrapped in [`CachePadded`]. This ensures that
/// `hit_count`, `miss_count`, and `eviction_count` reside on distinct
/// cache lines.
///
///
///
/// # Memory Ordering
/// All operations utilize [`Relaxed`] memory ordering. This is appropriate
/// for metrics where the absolute global order of increments across shards
/// is less important than the total cumulative throughput.
#[derive(Debug)]
struct MetricsStorage {
    /// Atomic counter for cache hits. Isolated to its own cache line.
    hit_count: CachePadded<AtomicU64>,
    /// Atomic counter for cache misses. Isolated to its own cache line.
    miss_count: CachePadded<AtomicU64>,
    /// Atomic counter for entry evictions. Isolated to its own cache line.
    eviction_count: CachePadded<AtomicU64>,
    /// Circular buffer for wait-free latency sampling.
    latency_sampler: Sampler,
}

impl MetricsStorage {
    /// Initializes a new shard of metric counters and a latency sampler.
    ///
    /// Each atomic counter is initialized to zero and isolated within
    /// its own [`CachePadded`] block to ensure that concurrent updates to
    /// hits, misses, and evictions do not interfere with each other.
    fn new(config: &MetricsConfig) -> Self {
        Self {
            hit_count: CachePadded::new(AtomicU64::default()),
            miss_count: CachePadded::new(AtomicU64::default()),
            eviction_count: CachePadded::new(AtomicU64::default()),
            latency_sampler: Sampler::new(config.latency_samples),
        }
    }

    /// Loads the current hit count from this shard.
    ///
    /// # Memory Ordering
    /// Uses [`Relaxed`] ordering, providing an eventually consistent
    /// view of the counter without memory fence overhead.
    #[inline]
    fn hit_count(&self) -> u64 {
        self.hit_count.load(Relaxed)
    }

    /// Atomically increments the hit counter.
    ///
    ///
    #[inline]
    fn record_hit(&self) {
        self.hit_count.fetch_add(1, Relaxed);
    }

    /// Loads the current miss count from this shard.
    #[inline]
    fn miss_count(&self) -> u64 {
        self.miss_count.load(Relaxed)
    }

    /// Atomically increments the miss counter.
    #[inline]
    fn record_miss(&self) {
        self.miss_count.fetch_add(1, Relaxed);
    }

    /// Loads the current eviction count from this shard.
    #[inline]
    fn eviction_count(&self) -> u64 {
        self.eviction_count.load(Relaxed)
    }

    /// Atomically increments the eviction counter.
    #[inline]
    fn record_eviction(&self) {
        self.eviction_count.fetch_add(1, Relaxed);
    }

    /// Forwards a latency measurement to the shard's internal [`Sampler`].
    ///
    /// This is a wait-free operation that records timing data into
    /// a circular buffer.
    #[inline]
    fn record_latency(&self, latency: u64) {
        self.latency_sampler.record(latency);
    }

    /// Aggregates all non-zero latency samples from this shard into the
    /// provided [`Histogram`].
    ///
    /// This is a non-destructive read; the sampler's state is preserved
    /// for potential subsequent snapshots.
    #[inline]
    fn write_latency_samples(&self, histogram: &mut Histogram<u64>) {
        self.latency_sampler.write_samples(histogram);
    }
}

/// A wait-free, lossy circular buffer for capturing telemetry samples.
///
/// `Sampler` provides a high-throughput mechanism for recording numeric data
/// in environments with massive write contention. It prioritizes progress
/// for producers (threads recording data) over absolute data retention.
///
/// # Concurrency Design
/// The buffer is **wait-free**. It uses a single atomic [`head`] pointer
/// to reserve slots in the `samples` vector. Multiple producers can
/// concurrently claim indices and store values without blocking or
/// needing to coordinate beyond a single `fetch_add`.
///
///
///
/// # Lossy Buffer Mechanics
/// Once the buffer reaches its capacity ($2^n$), the `head` index wraps
/// around, and new samples overwrite the oldest data. This ensures
/// memory usage remains constant regardless of the volume of events.
///
/// # Significant Implementation Details
/// - **Masking:** Indexing uses bitwise `& mask` instead of the remainder
///   operator `%`. This requires the internal capacity to be a power of two,
///   a constraint enforced during initialization.
/// - **Zero-Value Sentinels:** During aggregation ([`write_samples`]),
///   values of `0` are ignored. This identifies uninitialized slots or
///   intentionally skipped samples.
/// - **Read Consistency:** Snapshotting iterates over the buffer with
///   [`Relaxed`] loads. While a snapshot is being taken, producers may
///   be overwriting values, making the snapshot an "eventually consistent"
///   view of the buffer's state.
#[derive(Debug)]
pub struct Sampler {
    /// The underlying storage for samples.
    /// Note: The values themselves are atomic to allow concurrent reads/writes.
    samples: Vec<AtomicU64>,
    /// The monotonic write cursor. Padded to prevent the "Hot Head"
    /// contention from affecting adjacent memory.
    head: CachePadded<AtomicUsize>,
    /// Bitmask for fast index calculation ($capacity - 1$).
    mask: usize,
}

impl Sampler {
    /// Creates a new `Sampler` with a capacity rounded to the nearest power of two.
    ///
    /// The actual capacity is $2^n$, ensuring that bitwise masking can be used
    /// for fast index calculation.
    pub fn new(capacity: usize) -> Self {
        let len = capacity.next_power_of_two();
        let mut values = Vec::with_capacity(len);
        for _ in 0..len {
            values.push(AtomicU64::new(0));
        }

        Self {
            samples: values,
            head: CachePadded::new(AtomicUsize::new(0)),
            mask: len - 1,
        }
    }

    /// Records a value into the circular buffer using a bit-packed generation tag.
    ///
    /// This operation is **wait-free**. It uniquely identifies each sample with
    /// a "lap" (generation) count to prevent stale data from being read in
    /// subsequent snapshots.
    ///
    /// # Bit Packing Layout
    /// To store both the generation and the value in a single atomic operation:
    /// - **Bits 63-48 (16 bits):** Generation/Lap count.
    /// - **Bits 47-0 (48 bits):** Latency value (supports values up to $\approx 281$ trillion).
    ///
    ///
    ///
    /// # Performance
    /// The use of a bitmask ($head \ \& \ mask$) avoids the overhead of integer
    /// division, making this suitable for high-frequency event loops.
    #[inline]
    pub fn record(&self, value: u64) {
        let head = self.head.fetch_add(1, Relaxed);
        let index = head & self.mask;
        // Optimization: since len is a power of two, we could use bitshifts
        // if capacity is known, but division here is on the hot path only once.
        let lap = (head / self.samples.len()) as u64;

        let packed = (lap << 48) | (value & 0x0000FFFFFFFFFFFF);
        self.samples[index].store(packed, Relaxed);
    }

    /// Returns the power-of-two capacity of the sampler.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.samples.len()
    }

    /// Aggregates samples that belong only to the current buffer generation.
    ///
    /// # Ghost Data Prevention
    /// This method compares the generation tag of each stored sample against
    /// the current `head`'s generation. If a sample's tag is older than the
    /// current lap, it is ignored as "ghost" data from a previous cycle.
    ///
    /// # Concurrency
    /// This is a non-blocking read. While the loop is eventually consistent,
    /// the generation check ensures that the resulting histogram contains
    /// only data from the most recent $N$ operations.
    ///
    ///
    #[inline]
    pub fn write_samples(&self, histogram: &mut Histogram<u64>) {
        let current_head = self.head.load(Relaxed);
        let current_lap = (current_head / self.samples.len()) as u64;

        for sample_atomic in &self.samples {
            let packed = sample_atomic.load(Relaxed);
            let lap = packed >> 48;
            let val = packed & 0x0000FFFFFFFFFFFF;

            if val > 0 && lap == current_lap {
                let _ = histogram.record(val);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::sync::Barrier;

    /// Verifies that configuration values for shards and samples are automatically
    /// rounded up to the nearest power of two to support bitmask indexing.
    #[test]
    fn test_config_rounding() {
        let config = MetricsConfig::new(7, 1000);

        assert_eq!(config.shards, 8);
        assert_eq!(config.latency_samples, 1024);
    }

    /// Ensures that the hit counter is thread-safe and accurately aggregates
    /// increments from multiple concurrent producers.
    #[test]
    fn test_multithreaded_hit_counter() {
        let metrics = Metrics::default();

        thread::scope(|s| {
            for _ in 0..10 {
                s.spawn(|| {
                    for _ in 0..1000 {
                        metrics.record_hit();
                    }
                });
            }
        });

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.hit_count(), 10000);
    }

    /// Ensures that the miss counter is thread-safe across concurrent updates.
    #[test]
    fn test_multithreaded_miss_counter() {
        let metrics = Metrics::default();

        thread::scope(|s| {
            for _ in 0..10 {
                s.spawn(|| {
                    for _ in 0..1000 {
                        metrics.record_miss();
                    }
                });
            }
        });

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.miss_count(), 10000);
    }

    /// Ensures that the eviction counter is thread-safe across concurrent updates.
    #[test]
    fn test_multithreaded_eviction_counter() {
        let metrics = Metrics::default();

        thread::scope(|s| {
            for _ in 0..10 {
                s.spawn(|| {
                    for _ in 0..1000 {
                        metrics.record_eviction();
                    }
                });
            }
        });

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.eviction_count(), 10000);
    }

    /// Tests the sharding mechanism under heavy contention.
    /// Uses a Barrier to ensure all threads begin writing simultaneously.
    #[test]
    fn test_high_contention_counters() {
        let metrics = Metrics::new(MetricsConfig::new(4, 1024));
        let num_threads = 8;
        let barrier = Barrier::new(num_threads);

        thread::scope(|s| {
            for _ in 0..num_threads {
                s.spawn(|| {
                    barrier.wait();
                    for _ in 0..1000 {
                        metrics.record_hit();
                    }
                });
            }
        });

        assert_eq!(metrics.snapshot().hit_count(), 8000);
    }

    /// Verifies that the snapshot correctly maps raw latency samples to
    /// statistical percentiles using the HdrHistogram backend.
    #[test]
    fn test_percentile_logic() {
        let metrics = Metrics::default();
        for i in 1..=100 {
            metrics.record_latency(i);
        }
        let snap = metrics.snapshot();

        // Allow for small rounding errors inherent in histogram bucketing
        assert!(snap.latency(LatencyPercentile::P50) >= 49);
        assert!(snap.latency(LatencyPercentile::P99) >= 98);
    }

    /// Validates the floating-point calculations for hit and miss rates.
    #[test]
    fn test_hit_and_miss_rates() {
        let metrics = Metrics::new(MetricsConfig::new(4, 1024));

        for _ in 0..80 { metrics.record_hit(); }
        for _ in 0..20 { metrics.record_miss(); }

        let snap = metrics.snapshot();

        assert_eq!(snap.hit_count(), 80);
        assert_eq!(snap.miss_count(), 20);
        assert!((snap.hit_rate() - 0.8).abs() < f64::EPSILON);
        assert!((snap.miss_rate() - 0.2).abs() < f64::EPSILON);
    }

    /// Simple validation for the eviction counter recording.
    #[test]
    fn test_eviction_tracking() {
        let metrics = Metrics::default();

        for _ in 0..15 { metrics.record_eviction(); }

        let snap = metrics.snapshot();
        assert_eq!(snap.eviction_count(), 15);
    }

    /// Verifies that the latency distribution accurately reflects the recorded samples.
    #[test]
    fn test_latency_distribution() {
        let metrics = Metrics::new(MetricsConfig::new(1, 1024));

        for i in 1..=10 {
            metrics.record_latency(i * 10);
        }

        let snap = metrics.snapshot();

        let p50 = snap.latency(LatencyPercentile::P50);
        let p99 = snap.latency(LatencyPercentile::P99);

        assert!((49..=51).contains(&p50));
        assert!((99..=101).contains(&p99));
    }

    /// Ensures that taking a snapshot when no data has been recorded
    /// does not result in panics or invalid math (NaN).
    #[test]
    fn test_empty_snapshot_safety() {
        let metrics = Metrics::default();
        let snap = metrics.snapshot();

        assert_eq!(snap.hit_count(), 0);
        assert_eq!(snap.hit_rate(), 0.0);
        assert_eq!(snap.miss_rate(), 0.0);
        assert_eq!(snap.latency(LatencyPercentile::P50), 0);
    }

    /// Confirms that snapshots are independent "point-in-time" views
    /// and do not clear or mutate the underlying metric state.
    #[test]
    fn test_snapshot_independence() {
        let metrics = Metrics::default();

        metrics.record_hit();
        let snap1 = metrics.snapshot();

        metrics.record_hit();
        let snap2 = metrics.snapshot();

        assert_eq!(snap1.hit_count(), 1);
        assert_eq!(snap2.hit_count(), 2);
    }
}
