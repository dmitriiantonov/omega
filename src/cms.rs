use smallvec::SmallVec;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
use twox_hash::XxHash64;

/// Maximum value a counter can reach before saturating.
///
/// Using `u32::MAX >> 1` prevents wraparound in hot paths and
/// allows `fetch_add` to be used without CAS loops.
const COUNT_THRESHOLD: u32 = u32::MAX >> 1;

/// Count-Min Sketch (CMS) for approximate frequency counting.
///
/// A CMS is a probabilistic data structure used to estimate the frequency
/// of elements in a stream with sublinear memory.
///
/// # Features
/// - Flattened 2D counter table for cache efficiency
/// - `AtomicU32` counters (thread-safe, `Relaxed` ordering)
/// - SmallVec to store seeds inline (maximum height 8)
/// - XXH3 64-bit seeded hash functions for each row
///
/// # Saturation
/// Counters stop incrementing when reaching `COUNT_THRESHOLD` (~2^31).
///
/// # Decay
/// Call `decay()` periodically to halve counters and prevent saturation
/// over time, keeping frequency estimates meaningful.
#[derive(Debug)]
pub(crate) struct CountMinSketch {
    /// Number of counters per row (must be power of two)
    width: usize,
    /// Number of hash functions / rows (max 8)
    height: usize,
    /// Flattened 2D counter table
    counters: Vec<AtomicU32>,
    /// Random seeds for each row
    seeds: SmallVec<[u64; 8]>,
}

impl CountMinSketch {
    /// Creates a new Count-Min Sketch with the specified `width` and `height`.
    ///
    /// # Panics
    ///
    /// Panics if `height > 8` or if `width` is not a power of two.
    ///
    /// # Parameters
    ///
    /// - `width`: number of counters per row (power-of-two)
    /// - `height`: number of rows / hash functions (≤ 8)
    pub(crate) fn new(width: usize, height: usize) -> Self {
        assert!(height <= 8, "the max height is 8");
        assert!(width.is_power_of_two(), "width must be a power of two");

        let n = width * height;
        let counters = (0..n).map(|_| AtomicU32::default()).collect::<Vec<_>>();

        let mut seeds = SmallVec::new();
        (0..height).for_each(|_| seeds.push(rand::random()));

        Self {
            width,
            height,
            counters,
            seeds,
        }
    }

    /// Increments the estimated frequency count of `key` by 1.
    ///
    /// Counters saturate at `COUNT_THRESHOLD` to prevent wraparound.
    ///
    /// # Parameters
    ///
    /// - `key`: the element to count; must implement `AsRef<[u8]>`
    pub(crate) fn inc<K: Hash>(&self, key: K) {
        let width = self.width;

        for (row, &seed) in self.seeds.iter().enumerate() {
            let hash = hash(&key, seed) as usize;

            let index = (row * width) + (hash & (width - 1));

            let count = self.counters[index].load(Relaxed);
            if count >= COUNT_THRESHOLD {
                continue;
            }

            self.counters[index].fetch_add(1, Relaxed);
        }
    }

    /// Returns the estimated frequency count of `key`.
    ///
    /// The returned value is the **minimum** counter across all rows.
    ///
    /// # Parameters
    ///
    /// - `key`: the element to query; must implement `AsRef<[u8]>`
    ///
    /// # Returns
    ///
    /// The estimated count of `key` as a `u32`.
    pub(crate) fn frequency<K: Hash>(&self, key: &K) -> u32 {
        let mut count = u32::MAX;
        let width = self.width;

        for (row, &seed) in self.seeds.iter().enumerate() {
            let hash = hash(key, seed) as usize;
            let index = (row * width) + (hash & (width - 1));
            count = count.min(self.counters[index].load(Relaxed));
        }

        if count == u32::MAX { 0 } else { count }
    }

    /// Halves all counters to decay frequency estimates.
    ///
    /// This method should be called periodically to prevent counters from saturating,
    /// keeping relative frequency information accurate over time.
    pub(crate) fn decay(&self) {
        for counter in &self.counters {
            let old = counter.load(Relaxed);
            counter.store(old >> 1, Relaxed);
        }
    }
}

fn hash<K: Hash>(key: &K, seed: u64) -> u64 {
    let mut hasher = XxHash64::with_seed(seed);
    key.hash(&mut hasher);
    hasher.finish()
}
