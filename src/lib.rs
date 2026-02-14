pub use crate::admission::{AdmissionPolicy, AlwaysAdmission, FrequentPolicy};
use crate::core::engine::CacheEngine;
use crate::core::handler::Ref;
use crate::core::key::Key;
use crate::metrics::MetricsSnapshot;
use std::borrow::Borrow;
use std::hash::Hash;
use std::marker::PhantomData;

mod clock;
mod cms;

mod admission;
pub mod core;
pub mod metrics;

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
    use macros::cache;
    use std::thread::scope;

    #[test]
    fn test_cache_get_and_insert() {
        let cache = cache!(
            engine: Clock {
                capacity: 10,
                backoff: { policy: BackoffPolicy::Exponential, limit: 10 },
            },
            admission: Always
        );

        cache.insert("key1", "value1");
        cache.insert("key2", "value2");

        assert_eq!(cache.get(&"key1").map(|r| *r.value()), Some("value1"));
        assert_eq!(cache.get(&"key2").map(|r| *r.value()), Some("value2"));
        assert!(cache.get(&"key3").is_none());
    }

    #[test]
    fn test_eviction_on_full_capacity() {
        let cache = cache!(
            engine: Clock {
                capacity: 2,
                backoff: { policy: BackoffPolicy::Exponential, limit: 10 }
            },
            admission: Always
        );

        cache.insert(1, 1);
        cache.insert(2, 2);
        cache.insert(3, 3);

        let presence = (cache.get(&1).is_some() as u8)
            + (cache.get(&2).is_some() as u8)
            + (cache.get(&3).is_some() as u8);

        assert_eq!(presence, 2, "Cache should only contain 2 items");
    }

    #[test]
    fn test_metrics_tracking() {
        let cache = cache!(
            engine: Clock {
                capacity: 100,
                backoff: { policy: BackoffPolicy::Exponential, limit: 10 },
                metrics: { shards: 8, latency_samples: 1024 },
            },
            admission: Frequent {
                count_min_sketch: { width: 1024, height: 4 },
                decay_threshold: 1000
            }
        );

        cache.insert(1, 1);

        scope(|s| {
            s.spawn(|| {
                for _ in 0..80 {
                    let _ = cache.get(&1);
                }
            });

            s.spawn(|| {
                for _ in 0..20 {
                    let _ = cache.get(&2);
                }
            });
        });

        let metrics = cache.metrics();

        assert_eq!(80, metrics.hit_count());
        assert_eq!(20, metrics.miss_count());
        assert_eq!(0.8, metrics.hit_rate());
        assert_eq!(0.2, metrics.miss_rate());
    }
}
