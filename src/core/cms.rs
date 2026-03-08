use crossbeam::utils::CachePadded;
use rand::random;
use std::hash::{Hash, Hasher};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use twox_hash::xxhash64::Hasher as XxHash64;

/// A lock-free probabilistic data structure for frequency estimation.
///
/// `GhostFilter` tracks the residency of fingerprints evicted from the probationary
/// segment of a cache. It uses 4 rows of counters to minimize false positives.
pub struct CountMinSketch {
    /// Bit-packed counter storage. Each `AtomicU64` contains 8 x 8-bit counters.
    data: CachePadded<Box<[AtomicU64]>>,
    /// Hashing seeds for each of the 4 rows.
    seeds: Box<[u64]>,
    /// Number of `AtomicU64` blocks per row.
    capacity: usize,
    /// Bitmask for power-of-two indexing within a row.
    mask: usize,
}

impl CountMinSketch {
    /// Creates a new `GhostFilter` with a capacity scaled to the physical cache size.
    ///
    /// # Arguments
    /// * `capacity` - The target number of unique items to track. This value will be
    ///   divided by 8 and rounded up to the next power of two to determine block counts
    #[inline]
    pub fn new(capacity: usize) -> Self {
        let capacity = (capacity / 8).next_power_of_two();
        let mask = capacity - 1;

        let seeds: Box<[u64]> = (0..4)
            .map(|_| random())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        let data = (0..(capacity * 4))
            .map(|_| AtomicU64::default())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            data: CachePadded::new(data),
            seeds,
            capacity,
            mask,
        }
    }

    /// Increments the frequency counters for the given fingerprint.
    ///
    /// This operation is performed across all 4 rows using a saturating add.
    /// Counters will not exceed 255.
    pub fn increment<K: Eq + Hash>(&self, key: &K) {
        let mut skip = 0;

        for i in 0..4 {
            let hash = self.hash(key, i) as usize;

            let block_index = skip + (hash & self.mask);

            let shift = ((hash >> 32) & 0x7) * 8;

            let _ = self.data[block_index].fetch_update(Relaxed, Relaxed, |block| {
                let frequency = (block >> shift) & 0xFF;
                match frequency {
                    255 => None,
                    _ => Some(block + (1 << shift)),
                }
            });

            skip += self.capacity;
        }
    }

    /// Decrements the frequency counters for the given fingerprint.
    ///
    /// This is used when an entry is completely removed from the Ghost Queue.
    /// Counters will not underflow below 0.
    pub fn decrement<K: Eq + Hash>(&self, key: &K) {
        let mut skip = 0;

        for i in 0..4 {
            let hash = self.hash(key, i) as usize;

            let block_index = skip + (hash & self.mask);

            let shift = ((hash >> 32) & 0x7) * 8;

            let _ = self.data[block_index].fetch_update(Relaxed, Relaxed, |block| {
                let frequency = (block >> shift) & 0xFF;
                match frequency {
                    0 => None,
                    _ => Some(block - (1 << shift)),
                }
            });

            skip += self.capacity;
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

    /// Returns `true` if the fingerprint has a frequency greater than zero.
    ///
    /// In the context of S3-FIFO, a return of `true` indicates a "Ghost Hit,"
    /// suggesting the item should be promoted to the Main Queue.
    pub fn contains<K: Eq + Hash>(&self, key: &K) -> bool {
        let mut skip = 0;
        let mut frequency: u8 = u8::MAX;

        for seed_index in 0..4 {
            let hash = self.hash(key, seed_index) as usize;

            let block_index = skip + (hash & self.mask);

            let shift = ((hash >> 32) & 0x7) as usize * 8;

            let block = self.data[block_index].load(Relaxed);

            let current_frequency = (block >> shift) as u8;

            frequency = frequency.min(current_frequency);

            skip += self.capacity
        }

        frequency > 0
    }

    /// Generates a 64-bit hash for a specific row index.
    fn hash<K: Eq + Hash>(&self, key: K, index: usize) -> u64 {
        let index = self.seeds[index];
        let mut hasher = XxHash64::with_seed(index);
        key.hash(&mut hasher);
        hasher.finish()
    }
}
