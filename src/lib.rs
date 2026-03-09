pub use crate::admission::{AdmissionPolicy, AlwaysAdmission, FrequentPolicy};
use crate::core::engine::CacheEngine;
use crate::core::entry_ref::Ref;
use crate::core::key::Key;
use crate::metrics::MetricsSnapshot;
use std::borrow::Borrow;
use std::hash::Hash;
use std::marker::PhantomData;

mod clock;

mod admission;
pub mod core;
pub mod metrics;
mod s3fifo;

pub struct Cache<E, K, V, P>
where
    E: CacheEngine<K, V>,
    K: Eq + Hash,
    P: AdmissionPolicy<K>,
{
    engine: E,
    admission_policy: P,
    _phantom: PhantomData<(K, V)>,
}

impl<E, K, V, A> Cache<E, K, V, A>
where
    E: CacheEngine<K, V>,
    K: Eq + Hash,
    A: AdmissionPolicy<K>,
{
    pub fn new(engine: E, admission_policy: A) -> Self {
        Self {
            engine,
            admission_policy,
            _phantom: Default::default(),
        }
    }

    /// Retrieves a value from the cache.
    ///
    /// If the key exists, the admission policy is notified of the access.
    ///
    /// Returns a [`EntryRef`] that provides controlled access to the entry.
    pub fn get<Q>(&self, key: &Q) -> Option<Ref<K, V>>
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.admission_policy.record(key);
        self.engine.get(key)
    }

    /// Inserts a key-value pair into the cache.
    ///
    /// When the cache is full, an eviction candidate is selected by the engine.
    /// The admission policy then decides whether the incoming entry should
    /// replace the candidate.
    ///
    /// Returns a [`EntryRef`] if an existing entry was replaced.
    pub fn insert(&self, key: K, value: V) {
        self.engine
            .insert_with(key, value, None, |incoming, victim| {
                self.admission_policy.admit(incoming, victim)
            })
    }

    /// Removes an entry from the cache.
    ///
    /// Returns a [`EntryRef`] if the entry was present.
    pub fn remove<Q>(&self, key: &Q) -> bool
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.engine.remove(key)
    }

    pub fn metrics(&self) -> MetricsSnapshot {
        self.engine.metrics()
    }
}

#[cfg(test)]
mod tests {
    use crate::core::backoff::BackoffPolicy;
    use crate::core::utils::random_string;
    use macros::cache;
    use std::thread::scope;

    #[test]
    fn test_cache_should_create_s3fifo_and_run_workload() {
        let num_threads = 16;
        let op_per_thread = 10000;

        let cache = cache!(
            engine: S3FIFO {
                capacity: 10000,
                backoff: { policy: BackoffPolicy::Exponential, limit: 10 },
                metrics: { shards: 8, latency_samples: 1024 },
            },
            admission: Frequent {
                count_min_sketch: { width: 1024, depth: 4 },
                decay_threshold: 1000
            }
        );

        scope(|scope| {
            for _ in 0..num_threads {
                scope.spawn(|| {
                    for _ in 0..op_per_thread {
                        let key = random_string(10);
                        let value = random_string(255);
                        cache.insert(key, value);
                    }
                });
            }
        });
    }

    #[test]
    fn test_cache_should_create_clock_cache_and_run_workload() {
        let num_threads = 16;
        let op_per_thread = 10000;

        let cache = cache!(
            engine: S3FIFO {
                capacity: 10000,
                backoff: { policy: BackoffPolicy::Exponential, limit: 10 },
                metrics: { shards: 8, latency_samples: 1024 },
            },
            admission: Frequent {
                count_min_sketch: { width: 1024, depth: 4 },
                decay_threshold: 1000
            }
        );

        scope(|scope| {
            for _ in 0..num_threads {
                scope.spawn(|| {
                    for _ in 0..op_per_thread {
                        let key = random_string(10);
                        let value = random_string(255);
                        cache.insert(key, value);
                    }
                });
            }
        });
    }
}
