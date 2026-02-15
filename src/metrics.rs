use crossbeam::utils::CachePadded;
use hdrhistogram::Histogram;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicU16, AtomicU64, AtomicUsize};

/// Default number of shards if not specified.
pub const DEFAULT_SHARDS: usize = 4;

/// Default capacity for the circular latency buffer.
pub const DEFAULT_LATENCY_SAMPLES: usize = 256;

/// A bitmask used to isolate the data portion of a packed 64-bit sample.
///
/// This mask covers the lower 48 bits (bits 0–47), providing a valid range
/// for values from 0 up to 281,474,976,710,655. Any bits above index 47
/// are zeroed out by this mask.
pub const SAMPLE_DATA_MASK: u64 = (1 << 48) - 1;

/// The number of bits to shift a 16-bit sequence ID to place it in the
/// most significant bits of a 64-bit word.
///
/// This shift positions the `sequence_id` (generation tag) in bits 48–63,
/// effectively separating the metadata from the sample value for atomic
/// updates and ghost data filtering.
pub const SAMPLE_SEQUENCE_ID_SHIFT: usize = 48;

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
        let mut latency_histogram = create_latency_histogram();

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
    sequence_id: CachePadded<AtomicU16>,
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
            sequence_id: CachePadded::new(AtomicU16::new(1)),
            mask: len - 1,
        }
    }

    /// Records a value into the circular buffer using a bit-packed generation tag.
    ///
    /// This operation is **wait-free**, ensuring that high-frequency writers are never
    /// blocked by concurrent readers or other writers. It uniquely tags each sample
    /// with a `sequence_id` (lap count) to prevent "ghost reads"—where stale data
    /// from a previous rotation of the buffer is incorrectly included in a new snapshot.
    ///
    /// # Bit-Packing Layout
    /// The 64-bit atomic slot is partitioned to allow single-word updates:
    /// * **Bits 63–48 (16 bits):** `sequence_id`. Acts as a filter to validate data "freshness."
    /// * **Bits 47–0 (48 bits):** `data`. Supports measurements up to $2^{48} - 1$ (e.g., $\approx 281$ TB or 281 trillion nanoseconds).
    ///
    ///
    ///
    /// # Performance & Safety
    /// * **Efficiency:** Uses a bitwise mask (`head & mask`) for indexing, avoiding
    ///   expensive integer division. This requires the buffer size to be a power of two.
    /// * **Memory Ordering:** Uses `Relaxed` for the index increment to minimize
    ///   cache-line contention, while `Acquire` on the `sequence_id` ensures the writer
    ///   is synchronized with the current global lap.
    ///
    /// # Examples
    /// Since the value is masked by `SAMPLER_VALUE_MASK`, any bits higher than 47
    /// provided in the `value` argument will be truncated to ensure the `sequence_id`
    /// remains uncorrupted.
    #[inline]
    pub fn record(&self, data: u64) {
        let head = self.head.fetch_add(1, Relaxed);
        let sequence_id = self.sequence_id.load(Acquire);

        let index = head & self.mask;
        let packed = sample_pack(sequence_id, data);
        self.samples[index].store(packed, Relaxed);
    }

    /// Returns the power-of-two capacity of the sampler.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.samples.len()
    }

    /// Aggregates samples from the buffer that match the generation active at the start of the call.
    ///
    /// # Ghost Data Prevention
    /// This method implements a **generation-flip snapshot**. By atomically incrementing
    /// the `sequence_id` at the entry point, it captures the ID of the just-completed
    /// generation. During iteration, it filters the buffer to prevent:
    /// * **Stale Data (Ghosts):** Samples with a tag smaller than `sequency_id` are ignored.
    /// * **Future Data:** Samples with a tag larger than `sequency_id` (from writers that
    ///   started after this read began) trigger an early `break`.
    ///
    /// # Concurrency & Performance
    /// * **Wait-Free:** Writers are never blocked. They simply begin tagging new
    ///   samples with the next generation ID while this reader processes the previous one.
    /// * **Early Exit:** The `break` condition optimizes for cases where writers
    ///   rapidly overtake the reader, preventing unnecessary iteration over "future" slots.
    /// * **Memory Ordering:** Uses `Release` on the increment to ensure subsequent
    ///   writes in the next generation are ordered after this snapshot's boundary.
    ///
    /// # Panics
    /// Does not panic, though it assumes the buffer is not so large that the reader
    /// cannot complete a pass before the 16-bit `sequence_id` wraps around.
    #[inline]
    pub fn write_samples(&self, histogram: &mut Histogram<u64>) {
        let global_sequence_id = self.sequence_id.fetch_add(1, Release);

        for sample in &self.samples {
            let (sample_sequence_id, data) = sample_unpack(sample.load(Relaxed));

            if sample_sequence_id > global_sequence_id {
                break;
            }

            if data > 0 && sample_sequence_id == global_sequence_id {
                let _ = histogram.record(data);
            }
        }
    }
}

/// Packs a 16-bit sequence ID and a 48-bit data value into a single 64-bit word.
///
/// # Invariants
/// If the provided `data` exceeds the 48-bit range ($> 2^{48}-1$), the high bits
/// are truncated via `SAMPLE_DATA_MASK` to prevent corruption of the `sequence_id`.
fn sample_pack(sequence_id: u16, data: u64) -> u64 {
    (data & SAMPLE_DATA_MASK) | ((sequence_id as u64) << SAMPLE_SEQUENCE_ID_SHIFT)
}

/// Unpacks a 64-bit word into its constituent 16-bit sequence ID and 48-bit data value.
///
/// This is the inverse of [`sample_pack`]. It extracts the generation tag used
/// for ghost data prevention and the actual metric value.
fn sample_unpack(packed: u64) -> (u16, u64) {
    let sequence_id = (packed >> SAMPLE_SEQUENCE_ID_SHIFT) as u16;
    let data = packed & SAMPLE_DATA_MASK;
    (sequence_id, data)
}

/// Creates a new latency histogram with a standardized range and precision.
///
/// # Configuration
/// - **Range:** 1ms to 10,000ms (10 seconds).
/// - **Precision:** 2 significant figures (guarantees $\le 1\%$ error).
///
/// # Returns
/// A configured [`Histogram<u64>`].
///
/// # Panics
/// Panics if the internal bounds are invalid (though 1, 10000, 2 are verified constants).
#[inline]
pub fn create_latency_histogram() -> Histogram<u64> {
    const MIN_LATENCY: u64 = 1;
    const MAX_LATENCY: u64 = 10_000;
    const PRECISION: u8 = 2;

    Histogram::new_with_bounds(MIN_LATENCY, MAX_LATENCY, PRECISION)
        .expect("Failed to initialize latency histogram with standard bounds")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::RngExt;
    use std::sync::Barrier;
    use std::sync::{Arc, Mutex};
    use std::thread;

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

        for _ in 0..80 {
            metrics.record_hit();
        }
        for _ in 0..20 {
            metrics.record_miss();
        }

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

        for _ in 0..15 {
            metrics.record_eviction();
        }

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

    /// Integration test to verify concurrent latency accumulation, snapshot isolation,
    /// and the efficacy of the ghost data prevention algorithm.
    ///
    /// This test confirms that:
    /// 1. **Snapshot Independence:** Snapshots are point-in-time views that do not
    ///    clear or mutate the underlying metric state, yet remain isolated by generation.
    /// 2. **Ghost Prevention:** The 16-bit generation tag correctly identifies and
    ///    excludes stale data from previous buffer rotations.
    /// 3. **Statistical Integrity:** Percentiles (P50, P90, P99, P999) calculated from
    ///    sharded, lock-free storage match a Mutex-protected ground truth.
    /// 4. **Wait-Free Progress:** High-contention writes via a `Barrier` do not result
    ///    in data corruption or lost updates due to bit-packing race conditions.
    #[test]
    fn test_metrics_collect_latency_metrics() {
        let config = MetricsConfig::new(4, 1024);
        let metrics = Arc::new(Metrics::new(config));

        let num_threads = 10;
        let samples_per_thread = 200;
        let barrier = Arc::new(Barrier::new(num_threads));

        for _ in 0..10 {
            let histogram: Mutex<Histogram<u64>> = Mutex::new(create_latency_histogram());

            thread::scope(|s| {
                for _ in 0..num_threads {
                    s.spawn({
                        let metrics = metrics.clone();
                        let barrier = barrier.clone();
                        let histogram = &histogram;

                        move || {
                            let mut rng = rand::rng();
                            let mut written_values = Vec::with_capacity(samples_per_thread);
                            barrier.wait();

                            for _ in 0..samples_per_thread {
                                // Simulate latency between 100us and 1000us
                                let latency = rng.random_range(100..1000);
                                metrics.record_latency(latency);
                                written_values.push(latency);
                            }

                            let mut guard = histogram.lock().expect("cannot acquire lock");

                            for value in written_values {
                                guard.record(value).expect("cannot write value");
                            }

                            drop(guard)
                        }
                    });
                }
            });

            let snapshot = metrics.snapshot();

            let percentiles = [
                LatencyPercentile::P50,
                LatencyPercentile::P90,
                LatencyPercentile::P99,
                LatencyPercentile::P999,
            ];

            let guard = histogram.lock().expect("cannot acquire the lock");

            for p in percentiles {
                let actual = snapshot.latency(p);
                let expected = guard.value_at_quantile(p.as_quantile());

                // Assert that the lock-free sampler matches the controlled histogram
                assert!(
                    (actual as i64 - expected as i64).abs() <= 1,
                    "Percentile {:?} mismatch: actual {}, expected {}",
                    p,
                    actual,
                    expected
                );
            }
        }
    }
}
