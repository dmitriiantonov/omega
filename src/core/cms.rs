use crate::core::thread_context::ThreadContext;
use std::hash::{Hash, Hasher};
use std::sync::atomic::AtomicU16;
use std::sync::atomic::Ordering::{Acquire, Relaxed};
use twox_hash::xxhash64::Hasher as XxHash64;

/// A collection of high-entropy 64-bit prime seeds for row-level hashing independence.
///
/// These seeds initialize the `XxHash64` state for each row. Using static primes ensures:
/// 1. **Independence**: Minimizes the probability of "secondary collisions" across rows.
/// 2. **Bit Distribution**: Spreads hash output evenly across the column bitmask.
static SEEDS: [u64; 8] = [
    0x9e3779b97f4a7c15,
    0xbf58476d1ce4e5b9,
    0x94d049bb133111eb,
    0xff51afd7ed558ccd,
    0x6a09e667f3bcc908,
    0xbb67ae8584caa73b,
    0x3c6ef372fe94f82b,
    0xa54ff53a5f1d36f1,
];

/// A high-concurrency, memory-efficient frequency estimator.
///
/// Uses `AtomicU16` counters in a 2D matrix to provide a probabilistic upper bound
/// on item frequency. Designed for high-throughput environments where a small
/// overestimation error is acceptable in exchange for wait-free/lock-free performance.
pub struct CountMinSketch {
    counters: Box<[AtomicU16]>,
    columns: usize,
    rows: usize,
}

impl CountMinSketch {
    /// Creates a new `CountMinSketch` with the specified logical dimensions.
    ///
    /// # Arguments
    /// * `columns` - The logical width per row. The actual storage is doubled and
    ///   rounded to the next power of two to optimize indexing via bitwise masking.
    /// * `rows` - The number of independent hash functions.
    ///
    /// # Panics
    /// Panics if `rows` > 8, as it exceeds the available static prime seeds.
    #[inline]
    pub fn new(columns: usize, rows: usize) -> Self {
        assert!(rows <= 8, "Depth exceeds available static seeds (8)");
        let columns = (columns * 2).next_power_of_two();

        let counts = (0..columns * rows)
            .map(|_| AtomicU16::default())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            counters: counts,
            columns,
            rows,
        }
    }

    /// Increments the frequency estimate for a key across all rows.
    ///
    /// Uses a saturating atomic CAS loop to ensure counters never overflow.
    /// If contention is detected, the provided `backoff` is utilized to
    /// reduce CPU cache-coherency traffic.
    pub fn increment<K>(&self, key: &K, context: &ThreadContext)
    where
        K: Eq + Hash + ?Sized,
    {
        let mut skip = 0;

        for seed in self.seeds() {
            let hash = self.hash(key, seed) as usize;

            let column = hash & (self.columns - 1);
            let index = skip + column;

            let mut counter = self.counters[index].load(Acquire);

            while counter < u16::MAX {
                match self.counters[index].compare_exchange_weak(
                    counter,
                    counter + 1,
                    Relaxed,
                    Relaxed,
                ) {
                    Ok(_) => {
                        context.decay();
                        break;
                    }
                    Err(latest) => {
                        counter = latest;
                        context.wait();
                    }
                }
            }

            skip += self.columns;
        }
    }

    /// Internal helper to map row indices to their respective static seeds.
    #[inline(always)]
    fn seeds(&self) -> Vec<u64> {
        (0..self.rows).map(|index| SEEDS[index]).collect()
    }

    /// Decrements the frequency estimate for a key, saturating at zero.
    ///
    /// Useful for manual aging or correction. The CAS loop prevents
    /// integer underflow, which would otherwise erroneously transform
    /// a "cold" item into an "ultra-hot" item (65,535).
    pub fn decrement<K>(&self, key: &K, context: &ThreadContext)
    where
        K: Eq + Hash + ?Sized,
    {
        let mut skip = 0;

        for seed in self.seeds() {
            let hash = self.hash(key, seed) as usize;

            let column = hash & (self.columns - 1);
            let index = skip + column;

            let mut counter = self.counters[index].load(Acquire);

            while counter > 0 {
                match self.counters[index].compare_exchange_weak(
                    counter,
                    counter - 1,
                    Relaxed,
                    Relaxed,
                ) {
                    Ok(_) => {
                        context.decay();
                        break;
                    }
                    Err(latest) => {
                        counter = latest;
                        context.wait();
                    }
                }
            }

            skip += self.columns;
        }
    }

    /// Performs a global aging operation by halving every counter in the sketch.
    ///
    /// This reduces the "weight" of historical data, allowing the sketch
    /// to adapt to changes in key distribution over time. Uses a CAS loop
    /// per counter to maintain atomicity during the bit-shift.
    pub fn decay(&self, context: &ThreadContext) {
        for counter in &self.counters {
            let mut counter_value = counter.load(Relaxed);

            if counter_value > 0 {
                match counter.compare_exchange_weak(
                    counter_value,
                    counter_value >> 1,
                    Relaxed,
                    Relaxed,
                ) {
                    Ok(_) => {
                        context.decay();
                    }
                    Err(latest) => {
                        counter_value = latest;
                        context.wait();
                    }
                }
            }
        }
    }

    /// Checks if an item is likely present in the sketch (estimate > 0).
    pub fn contains<K: Eq + Hash>(&self, key: &K) -> bool {
        self.get(key) > 0
    }

    /// Retrieves the estimated frequency of a key.
    ///
    /// The estimate is the minimum value found across all rows for the given key.
    /// This is a mathematically guaranteed upper bound of the true frequency.
    pub fn get<K>(&self, key: &K) -> u16
    where
        K: Eq + Hash + ?Sized,
    {
        let mut skip = 0;
        let mut frequency = u32::MAX;

        for seed in (0..self.rows).map(|index| SEEDS[index]) {
            let hash = self.hash(key, seed) as usize;
            let index = skip + (hash & (self.columns - 1));

            let counter = self.counters[index].load(Relaxed) as u32;
            frequency = frequency.min(counter);

            skip += self.columns
        }

        if frequency == u32::MAX {
            0
        } else {
            frequency as u16
        }
    }

    /// Hashes a key with a specific seed using the XxHash64 algorithm.
    #[inline(always)]
    fn hash<K: Eq + Hash + ?Sized>(&self, key: &K, seed: u64) -> u64 {
        let mut hasher = XxHash64::with_seed(seed);
        key.hash(&mut hasher);
        hasher.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::distr::{Alphanumeric, SampleString};
    use std::thread::scope;

    fn random_key(len: usize) -> String {
        Alphanumeric.sample_string(&mut rand::rng(), len)
    }

    #[test]
    fn test_count_min_sketch_should_increment_and_retrieve_frequency() {
        let cms = CountMinSketch::new(128, 4);
        let key = random_key(10);
        let context = ThreadContext::default();

        cms.increment(&key, &context);
        cms.increment(&key, &context);
        cms.increment(&key, &context);

        assert_eq!(
            cms.get(&key),
            3,
            "Frequency should reflect the exact number of increments."
        );
    }

    #[test]
    fn test_count_min_sketch_should_saturate_at_max_logical_value() {
        let cms = CountMinSketch::new(64, 2);
        let key = random_key(10);
        let context = ThreadContext::default();

        for _ in 0..100000 {
            cms.increment(&key, &context);
        }

        assert_eq!(
            cms.get(&key),
            u16::MAX,
            "Counters must cap at MAX_FREQUENCY to prevent wrap-around."
        );
    }

    #[test]
    fn test_count_min_sketch_should_halve_all_counters_on_decay() {
        let cms = CountMinSketch::new(1024, 4);
        let key = random_key(10);
        let context = ThreadContext::default();

        for _ in 0..20 {
            cms.increment(&key, &context);
        }

        assert_eq!(cms.get(&key), 20);

        cms.decay(&context);
        assert_eq!(cms.get(&key), 10);

        cms.decay(&context);
        assert_eq!(cms.get(&key), 5);

        cms.decay(&context);
        assert_eq!(cms.get(&key), 2); // 5 >> 1 = 2
    }

    #[test]
    fn test_count_min_sketch_should_saturate_at_zero_on_decrement() {
        let cms = CountMinSketch::new(128, 4);
        let key = random_key(10);
        let context = ThreadContext::default();

        cms.increment(&key, &context);
        cms.decrement(&key, &context);
        assert_eq!(cms.get(&key), 0);

        cms.decrement(&key, &context);
        assert_eq!(cms.get(&key), 0, "Counter must not underflow below zero.");
    }

    #[test]
    fn test_count_min_sketch_should_maintain_consistent_state_under_contention() {
        let cms = CountMinSketch::new(16, 4); // Small width to force collisions
        let num_threads = 8;
        let ops_per_thread = 100;
        let key = random_key(10);

        scope(|s| {
            for _ in 0..num_threads {
                s.spawn(|| {
                    let context = ThreadContext::default();
                    for _ in 0..ops_per_thread {
                        cms.increment(&key, &context);
                    }
                });
            }
        });

        assert_eq!(
            cms.get(&key),
            (num_threads * ops_per_thread) as u16,
            "Atomic increments must be consistent across multiple threads."
        );
    }

    #[test]
    fn test_count_min_sketch_should_return_zero_for_unknown_keys() {
        let cms = CountMinSketch::new(2048, 4);
        let key = random_key(10);

        assert_eq!(cms.get(&key), 0);
        assert!(!cms.contains(&key));
    }

    #[test]
    fn test_count_min_sketch_should_tolerate_collisions_within_probabilistic_bounds() {
        let cms = CountMinSketch::new(2048, 4);
        let key_a = random_key(10);
        let key_b = random_key(20);
        let context = ThreadContext::default();

        for _ in 0..50 {
            cms.increment(&key_a, &context);
        }
        for _ in 0..5 {
            cms.increment(&key_b, &context);
        }

        assert!(cms.get(&key_a) >= 50);
        assert!(cms.get(&key_b) >= 5);
    }
}
