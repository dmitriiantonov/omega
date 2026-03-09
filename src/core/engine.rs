use crate::core::entry_ref::Ref;
use crate::core::key::Key;
use crate::metrics::MetricsSnapshot;
use std::borrow::Borrow;
use std::hash::Hash;
use std::time::Instant;

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
    fn get<Q>(&self, key: &Q) -> Option<Ref<K, V>>
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized;

    /// Inserts a key-value pair into the cache.
    ///
    /// This is a convenience wrapper around [`insert_with`] that always
    /// admits the new entry. If the cache is full, an existing entry
    /// is evicted based on the engine's internal policy.
    #[inline]
    fn insert(&self, key: K, value: V, expired_at: Option<Instant>) {
        self.insert_with(key, value, expired_at, |_, _| true);
    }

    /// Inserts a key-value pair with a custom admission policy.
    ///
    /// When the cache is full, the `admission` closure is invoked with the
    /// `(incoming_key, potential_victim_key)`. If the closure returns `false`,
    /// the insertion is aborted to preserve the current cache state.
    ///
    /// # Guarantees
    /// - **Updates**: If the key already exists, the value is updated and
    ///   the admission policy is typically bypassed.
    /// - **Expiration**: Expired entries in a target slot are evicted
    ///   regardless of the admission policy.
    fn insert_with<A>(&self, key: K, value: V, expired_at: Option<Instant>, admission: A)
    where
        A: Fn(&K, &K) -> bool;

    /// Removes an entry from the cache.
    ///
    /// Returns `true` if the entry was found and successfully marked for
    /// removal.
    ///
    /// # Consistency
    /// This method performs an atomic "unlinking." The entry is immediately
    /// made unreachable for new `get` requests, while existing readers
    /// can continue to access the data safely until their [`Ref`] is dropped.
    fn remove<Q>(&self, key: &Q) -> bool
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized;

    /// Returns the total number of slots allocated for the cache.
    fn capacity(&self) -> usize;

    /// Returns a snapshot of internal cache statistics.
    fn metrics(&self) -> MetricsSnapshot;
}
