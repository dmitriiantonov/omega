use crate::core::utils::random_string;
use bytes::Bytes;
use dashmap::DashMap;
use rand::Rng;
use rand::prelude::*;
use rand_distr::Zipf;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};

/// A Zipfian workload generator for cache benchmarking.
///
/// This structure generates a fixed set of random keys and samples them
/// according to a Power Law distribution. This mimics real-world scenarios
/// where a small subset of "hot" keys receives the majority of traffic.
pub struct WorkloadGenerator {
    /// The pool of pre-generated random strings.
    keys: Vec<Bytes>,
    /// The mathematical distribution used to pick indices from `keys`.
    distribution: Zipf<f64>,
}

impl WorkloadGenerator {
    /// Creates a new workload with a specified number of unique keys and skew.
    ///
    /// # Arguments
    /// * `count` - The number of unique logical keys in the workload universe.
    /// * `skew` - The Zipfian exponent ($s$).
    ///   * `0.5`: Approaching uniform (flatter distribution).
    ///   * `1.0`: Standard Zipfian (classic "Power Law").
    ///   * `1.5`: Highly skewed (very few keys receive almost all traffic).
    ///
    /// # Panics
    /// Panics if `skew` is not within the statistically significant range of `0.5` to `1.5`.
    #[inline(always)]
    pub fn new(count: usize, skew: f64) -> Self {
        assert!(
            (0.5..=1.5).contains(&skew),
            "skew must be in range from 0.5 to 1.5"
        );

        let keys = (0..count).map(|_| Bytes::from(random_string())).collect();

        let distribution = Zipf::new(count as f64, skew).expect("incorrect args");

        Self { keys, distribution }
    }

    /// Samples a key from the workload using the provided random number generator.
    ///
    /// This method is designed for high-concurrency environments; by passing a
    /// mutable reference to a thread-local `Rng`, it avoids global lock contention.
    ///
    /// # Performance
    /// This operation is $O(1)$ relative to the size of the key strings but follows
    /// the complexity of the `rand_distr::Zipf` sampling algorithm (typically rejection inversion).
    #[inline(always)]
    pub fn key<D: Rng>(&self, distribution: &mut D) -> Bytes {
        let index = self.distribution.sample(distribution) as usize - 1;
        self.keys[index].clone()
    }
}

/// A thread-safe statistical observer used to validate cache efficiency.
///
/// It captures the "Ground Truth" of a workload by counting every key access.
/// This allows tests to compare the cache's internal state against the
/// mathematically ideal set of frequent items.
pub struct WorkloadStatistics {
    counts: DashMap<Bytes, usize>,
}

impl Default for WorkloadStatistics {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkloadStatistics {
    /// Creates a new, empty statistics tracker.
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            counts: Default::default(),
        }
    }

    /// Records access to a specific key.
    ///
    /// In a concurrent test, multiple threads call this to build a global
    /// view of key popularity.
    pub fn record(&self, key: Bytes) {
        let mut entry = self.counts.entry(key).or_default();
        *entry += 1;
    }

    /// Retrieves the `count` most frequently accessed keys, ordered from
    /// hottest to coldest.
    ///
    /// This uses a Min-Heap (via `Reverse`) to maintain a sliding window of the
    /// top elements, ensuring $O(N \log K)$ time complexity where $N$ is total
    /// unique keys and $K$ is the requested count.
    ///
    /// # Performance
    /// This method is intended for use at the *end* of a test run, as it
    /// iterates over the entire frequency map.
    pub fn frequent_keys(&self, count: usize) -> Vec<Bytes> {
        let mut top_frequent = BinaryHeap::new();

        for entry in &self.counts {
            let key = entry.key();
            let frequency = *entry.value();

            if top_frequent.len() < count {
                top_frequent.push(Reverse((frequency, key.clone())));
            } else if let Some(&Reverse((other_frequency, _))) = top_frequent.peek()
                && other_frequency < frequency
            {
                top_frequent.pop();
                top_frequent.push(Reverse((frequency, key.clone())))
            }
        }

        let mut deque = VecDeque::with_capacity(top_frequent.len());

        while let Some(Reverse((_, key))) = top_frequent.pop() {
            deque.push_front(key);
        }

        deque.into_iter().collect()
    }
}
