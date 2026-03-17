use crate::core::entry::Entry;
use crate::core::entry_ref::Ref;
use crate::core::key::Key;
use crate::core::request_quota::RequestQuota;
use crate::core::thread_context::ThreadContext;
use crate::metrics::MetricsSnapshot;
use std::borrow::Borrow;
use std::hash::Hash;

/// A thread-safe, fixed-capacity caching interface.
///
/// This trait abstracts the underlying storage and eviction mechanics,
/// allowing callers to interact with different cache implementations
/// through a consistent API.
///
/// # Type Parameters
/// * `K`: The key type, which must implement [`Eq`] and [`Hash`].
/// * `V`: The value type stored in the cache.
pub trait CacheEngine<K, V>
where
    K: Eq + Hash,
{
    /// Retrieves a protected reference to a cached value.
    ///
    /// Returns `Some(Ref<K, V>)` if the key exists and is not expired. Accessing
    /// a key typically updates internal frequency metadata, which the eviction
    /// algorithm uses to protect "hot" data from removal.
    ///
    /// # Memory Safety
    /// The returned [`Ref`] pins the entry in memory using an epoch guard.
    /// The data will remain valid and allocated until the reference is dropped
    /// and the global epoch advances.
    fn get<Q>(&self, key: &Q, context: &ThreadContext) -> Option<Ref<K, V>>
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized;

    /// Inserts a key-value pair into the cache.
    ///
    /// This is a convenience wrapper around [`insert_with`] that always
    /// admits the new entry. If the cache is full, an existing entry
    /// is evicted based on the engine's internal policy.
    fn insert(&self, entry: Entry<K, V>, context: &ThreadContext, quota: &mut RequestQuota);

    /// Removes an entry from the cache.
    ///
    /// Returns `true` if the entry was found and successfully marked for
    /// removal.
    ///
    /// # Consistency
    /// This method performs an atomic "unlinking." The entry is immediately
    /// made unreachable for new `get` requests, while existing readers
    /// can continue to access the data safely until their [`Ref`] is dropped.
    fn remove<Q>(&self, key: &Q, context: &ThreadContext) -> bool
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized;

    /// Returns the total number of slots allocated for the cache.
    fn capacity(&self) -> usize;

    /// Returns a snapshot of internal cache statistics.
    fn metrics(&self) -> MetricsSnapshot;
}
