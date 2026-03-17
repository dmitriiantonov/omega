pub mod clock;
pub mod core;
pub mod metrics;
pub mod s3fifo;

use crate::core::backoff::BackoffConfig;
use crate::core::engine::CacheEngine;
use crate::core::entry::Entry;
use crate::core::entry_ref::Ref;
use crate::core::key::Key;
use crate::core::request_quota::RequestQuota;
use crate::core::thread_context::ThreadContext;
use crate::metrics::MetricsSnapshot;
pub use omega_cache_macros::cache;
use std::borrow::Borrow;
use std::hash::Hash;
use std::marker::PhantomData;
use thread_local::ThreadLocal;

pub struct Cache<E, K, V>
where
    E: CacheEngine<K, V>,
    K: Eq + Hash,
{
    engine: E,
    backoff_config: BackoffConfig,
    context: ThreadLocal<ThreadContext>,
    _phantom: PhantomData<(K, V)>,
}

impl<E, K, V> Cache<E, K, V>
where
    E: CacheEngine<K, V>,
    K: Eq + Hash,
{
    pub fn new(engine: E, backoff_config: BackoffConfig) -> Self {
        Self {
            engine,
            backoff_config,
            context: ThreadLocal::new(),
            _phantom: Default::default(),
        }
    }

    /// Retrieves a value from the cache.
    ///
    /// If the key exists, the admission policy is notified of the access.
    ///
    /// Returns a [`Ref`] handle that provides controlled access to the entry.
    ///
    /// The entry is pinned in memory for the duration of the handle's life using
    /// epoch-based memory reclamation.
    pub fn get<Q>(&self, key: &Q) -> Option<Ref<K, V>>
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let context = self.context();
        self.engine.get(key, context)
    }

    /// Inserts a key-value pair into the cache.
    ///
    /// When the cache is full, an eviction candidate is selected by the engine.
    /// The admission policy then decides whether the incoming entry should
    /// replace the candidate.
    ///
    /// Returns a [`EntryRef`] if an existing entry was replaced.
    pub fn insert(&self, key: K, value: V) {
        let entry = Entry::new(key, value);

        self.engine
            .insert(entry, self.context(), &mut RequestQuota::default())
    }

    /// Removes an entry from the cache.
    ///
    /// Returns `true` if the entry was found and successfully removed.
    pub fn remove<Q>(&self, key: &Q) -> bool
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.engine.remove(key, self.context())
    }

    pub fn metrics(&self) -> MetricsSnapshot {
        self.engine.metrics()
    }

    #[inline]
    fn context(&self) -> &ThreadContext {
        self.context
            .get_or(|| ThreadContext::new(self.backoff_config.build()))
    }
}
