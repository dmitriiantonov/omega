use crossbeam::utils::CachePadded;
use std::hash::{Hash, Hasher};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use twox_hash::xxhash64::Hasher as XxHash64;

/// A collection of high-entropy 64-bit prime seeds.
///
/// These seeds are used to initialize the `XxHash64` state for each row
/// of the `CountMinSketch`. Using static primes ensures:
///
/// 1. **Deterministic Hashing**: Identical keys map to identical physical
///    blocks across different instances of the sketch.
/// 2. **Independence**: Minimizes the probability of "secondary collisions"
///    where keys collide across multiple rows simultaneously.
/// 3. **Bit Distribution**: Ensures the hash output is spread evenly across
///    the `blocks_mask` and the 3-bit internal counter `shift`.
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

/// A lock-free probabilistic data structure for frequency estimation.
///
/// This implementation optimizes for CPU cache locality and multithreaded throughput
/// by bit-packing 8-bit saturating counters into 64-bit atomic blocks.
pub struct CountMinSketch {
    /// Bit-packed counter storage. Each `AtomicU64` contains 8 x 8-bit counters.
    data: CachePadded<Box<[AtomicU64]>>,
    /// Number of `AtomicU64` blocks per row.
    blocks: usize,
    /// Number of independent rows (hash functions) in the sketch.
    depth: usize,
    /// Bitmask for power-of-two indexing within a row.
    blocks_mask: usize,
}

impl CountMinSketch {
    /// Creates a new probabilistic estimator with a specified geometry.
    ///
    /// The physical storage is calculated by mapping the requested logical width
    /// to 64-bit atomic blocks (8 counters per block).
    ///
    /// # Arguments
    /// * `width` - The logical number of counters per row. This is rounded to the next
    ///   power of two to enable bitwise indexing.
    /// * `depth` - The number of independent hash functions (rows) used to minimize
    ///   the probability of overestimation.
    ///
    /// # Panics
    /// Panics if `depth` exceeds 8, as the implementation relies on a fixed set
    /// of static prime seeds for hashing independence.
    #[inline]
    pub fn new(width: usize, depth: usize) -> Self {
        assert!(depth <= 8, "depth must not exceed 8");
        let blocks = (width / 8).next_power_of_two();
        let blocks_mask = blocks - 1;

        let data = (0..(blocks * depth))
            .map(|_| AtomicU64::default())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            data: CachePadded::new(data),
            blocks,
            depth,
            blocks_mask,
        }
    }

    /// Increments the frequency counters for the given key.
    ///
    /// This operation is performed across all rows (defined by `depth`) using a
    /// saturating add. Counters will not exceed 255.
    ///
    /// # Arguments
    /// * `key` - The item whose frequency should be increased.
    pub fn increment<K: Eq + Hash>(&self, key: &K) {
        let mut skip = 0;

        for seed in (0..self.depth).map(|index| SEEDS[index]) {
            let hash = self.hash(key, seed) as usize;

            let block_index = skip + (hash & self.blocks_mask);

            let shift = ((hash >> 32) & 0x7) * 8;

            let _ = self.data[block_index].fetch_update(Relaxed, Relaxed, |block| {
                let frequency = (block >> shift) & 0xFF;
                match frequency {
                    255 => None,
                    _ => Some(block + (1 << shift)),
                }
            });

            skip += self.blocks;
        }
    }

    /// Decrements the frequency counters for the given key.
    ///
    /// This is used for manual aging or item removal within the sketch.
    /// Counters will not underflow below 0.
    ///
    /// # Arguments
    /// * `key` - The item whose frequency should be decreased.
    pub fn decrement<K: Eq + Hash>(&self, key: &K) {
        let mut skip = 0;

        for seed in (0..self.depth).map(|index| SEEDS[index]) {
            let hash = self.hash(key, seed) as usize;

            let block_index = skip + (hash & self.blocks_mask);

            let shift = ((hash >> 32) & 0x7) * 8;

            let _ = self.data[block_index].fetch_update(Relaxed, Relaxed, |block| {
                let frequency = (block >> shift) & 0xFF;
                match frequency {
                    0 => None,
                    _ => Some(block - (1 << shift)),
                }
            });

            skip += self.blocks;
        }
    }

    /// Performs an aging operation by halving all counters in the sketch.
    ///
    /// This uses a bitwise trick to divide eight 8-bit counters by 2
    /// simultaneously within each 64-bit word.
    pub fn decay(&self) {
        let mask: u64 = 0xFEFEFEFEFEFEFEFE;

        for i in 0..self.data.len() {
            let _ = self.data[i].fetch_update(Relaxed, Relaxed, |block| {
                if block == 0 {
                    None
                } else {
                    Some((block & mask) >> 1)
                }
            });
        }
    }

    /// Returns `true` if the key has an estimated frequency greater than zero.
    ///
    /// Derived by taking the minimum frequency observed across all rows.
    ///
    /// # Arguments
    /// * `key` - The item to check for presence in the sketch.
    pub fn contains<K: Eq + Hash>(&self, key: &K) -> bool {
        self.get(key) > 0
    }

    /// Returns the estimated frequency of the provided key.
    ///
    /// The estimate is derived by taking the minimum value found across all rows
    /// (defined by `depth`). Due to the probabilistic nature of the sketch, this
    /// value is an upper bound of the actual frequency.
    ///
    /// # Arguments
    /// * `key` - The item whose frequency estimate is being requested.
    pub fn get<K: Eq + Hash>(&self, key: &K) -> u8 {
        let mut skip = 0;
        let mut frequency: u16 = u16::MAX;

        for seed in (0..self.depth).map(|index| SEEDS[index]) {
            let hash = self.hash(key, seed) as usize;

            let block_index = skip + (hash & self.blocks_mask);

            let shift = ((hash >> 32) & 0x7) * 8;

            let block = self.data[block_index].load(Relaxed);

            let current_frequency = (block >> shift) as u16;

            frequency = frequency.min(current_frequency);

            skip += self.blocks
        }

        if frequency == u16::MAX {
            0
        } else {
            frequency as u8
        }
    }

    /// Generates a 64-bit fingerprint for a specific row index.
    ///
    /// This uses a seeded hashing strategy to ensure that each row in the
    /// sketch provides an independent observation of the key's frequency.
    ///
    /// # Arguments
    /// * `key` - The item to be hashed.
    /// * `seed` - The row-specific random seed to initialize the hasher.
    fn hash<K: Eq + Hash>(&self, key: K, seed: u64) -> u64 {
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

        cms.increment(&key);
        cms.increment(&key);
        cms.increment(&key);

        assert_eq!(
            cms.get(&key),
            3,
            "Frequency should reflect the exact number of increments."
        );
    }

    #[test]
    fn test_count_min_sketch_should_saturate_at_max_u8_without_overflow() {
        let cms = CountMinSketch::new(64, 2);
        let key = random_key(10);

        // Increment past 255 to test bit-slot protection
        for _ in 0..300 {
            cms.increment(&key);
        }

        assert_eq!(
            cms.get(&key),
            255,
            "Counters must cap at 255 to protect adjacent bit-packed slots."
        );
    }

    #[test]
    fn test_count_min_sketch_should_halve_all_counters_on_decay() {
        let cms = CountMinSketch::new(1024, 4);
        let key = random_key(10);

        for _ in 0..20 {
            cms.increment(&key);
        }
        assert_eq!(cms.get(&key), 20);

        cms.decay();
        assert_eq!(cms.get(&key), 10);

        cms.decay();
        assert_eq!(cms.get(&key), 5);

        cms.decay();
        assert_eq!(cms.get(&key), 2);
    }

    #[test]
    fn test_count_min_sketch_should_saturate_at_zero_on_decrement() {
        let cms = CountMinSketch::new(128, 4);
        let key = random_key(10);

        cms.increment(&key);
        cms.decrement(&key);
        assert_eq!(cms.get(&key), 0);

        // Ensure no underflow (wrapping to 255)
        cms.decrement(&key);
        assert_eq!(cms.get(&key), 0, "Counter must not underflow below zero.");
    }

    #[test]
    fn test_count_min_sketch_should_maintain_consistent_state_under_contention() {
        let cms = CountMinSketch::new(16, 4); // Small width to force block collisions
        let num_threads = 10;
        let ops_per_thread = 20;
        let key = random_key(10);

        scope(|s| {
            for _ in 0..num_threads {
                s.spawn(|| {
                    for _ in 0..ops_per_thread {
                        cms.increment(&key);
                    }
                });
            }
        });

        assert_eq!(
            cms.get(&key),
            200,
            "Lock-free increments must be atomic across bit-packed blocks."
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

        for _ in 0..50 {
            cms.increment(&key_a);
        }
        for _ in 0..5 {
            cms.increment(&key_b);
        }

        // CMS estimates are always upper bounds
        assert!(cms.get(&key_a) >= 50);
        assert!(cms.get(&key_b) >= 5);
    }
}
