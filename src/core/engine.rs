use crate::core::handler::Ref;
use crate::core::key::Key;
use crate::metrics::MetricsSnapshot;
use std::borrow::Borrow;
use std::hash::Hash;
use std::time::Instant;

/// A high-performance, concurrent cache engine.
///
/// `CacheEngine` provides a thread-safe interface for fixed-capacity caching.
/// It is designed for systems where throughput is critical, utilizing atomic
/// state machines and epoch-based memory reclamation to minimize synchronization
/// overhead.
///
/// # Eviction & Expiration
/// Implementations typically follow a "Second Chance" (Clock) or LRU policy.
/// Entries are eligible for eviction if they are marked as 'Cold' or if their
/// TTL has expired.
pub trait CacheEngine<K, V>
where
    K: Eq + Hash,
{
    /// Retrieves a protected reference to a cached value.
    ///
    /// Returns `Some(EntryRef)` if the key exists and is not expired. Accessing
    /// a key may update its internal recency metadata (e.g., promoting a
    /// 'Cold' entry to 'Hot').
    ///
    /// # Memory Safety
    /// The returned [`EntryRef`] pins the entry in memory using an epoch guard.
    /// The entry will not be deallocated until the reference is dropped.
    fn get<Q>(&self, key: &Q) -> Option<Ref<K, V>>
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized;

    /// Inserts a key-value pair into the cache.
    ///
    /// If the cache is at capacity, an existing entry will be evicted based
    /// on the engine's policy. This is a convenience wrapper around
    /// [`insert_with`] that always admits the new entry.
    #[inline]
    fn insert(&self, key: K, value: V, expired_at: Option<Instant>) {
        self.insert_with(key, value, expired_at, |_, _| true);
    }

    /// Inserts a key-value pair with a custom admission policy.
    ///
    /// The `admission` closure is called when the engine identifies a potential
    /// eviction candidate. It compares the `(incoming_key, victim_key)`.
    /// If it returns `false`, the insertion is rejected.
    ///
    /// # Guarantees
    /// - If the key already exists in the cache, it is updated (admission is
    ///   usually bypassed for updates).
    /// - If an entry in a target slot is expired, it is evicted regardless
    ///   of the admission policy.
    fn insert_with<F>(&self, key: K, value: V, expired_at: Option<Instant>, admission: F)
    where
        F: Fn(&K, &K) -> bool;

    /// Removes an entry from the cache.
    ///
    /// Returns `true` if the entry was found and removed, `false` otherwise.
    ///
    /// # Performance
    /// This method is "fire-and-forget." It triggers the internal state machine
    /// to transition the slot to a 'Vacant' state and schedules the memory
    /// for reclamation without returning the underlying data.
    fn remove<Q>(&self, key: &Q) -> bool
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized;

    /// Returns the maximum number of entries the cache can hold.
    ///
    /// This represents the total number of slots allocated at initialization.
    fn capacity(&self) -> usize;

    fn metrics(&self) -> MetricsSnapshot;
}
