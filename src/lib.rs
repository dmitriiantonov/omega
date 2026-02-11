pub use crate::admission::{AdmissionPolicy, AlwaysAdmission, FrequentPolicy};
use crate::core::engine::CacheEngine;
use crate::core::handler::EntryRef;
use crate::core::key::Key;
use std::borrow::Borrow;
use std::hash::Hash;
use std::marker::PhantomData;

mod clock;
mod cms;

mod admission;
pub mod core;

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
    pub fn get<Q>(&self, key: &Q) -> Option<EntryRef<K, V>>
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
}

#[cfg(test)]
mod tests {
    use crate::core::backoff::BackoffPolicy;
    use macros::cache;

    #[test]
    fn test() {
        let cache = cache!(
            engine: Clock {
                capacity: 100,
                backoff: { policy: BackoffPolicy::Exponential, limit: 10 }
            },
            admission: Frequent {
                count_min_sketch: { width: 1024, height: 4 },
                decay_threshold: 1000
            }
        );

        cache.insert(1, 1);

        if let Some(handler) = cache.get(&1) {
            assert_eq!(&1, handler.value())
        }
    }
}
