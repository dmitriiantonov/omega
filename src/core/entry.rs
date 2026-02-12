use crate::core::key::Key;
use std::hash::Hash;
use std::time::Instant;

/// A container representing a single cached item.
///
/// `Entry` holds the mapping between a [Key] and its associated value,
/// along with optional TTL (Time To Live) metadata to handle expiration.
///
/// ### Generic Constraints
/// * `K`: The raw key type, which must implement [Eq] and [Hash].
/// * `V`: The type of the value being stored.
#[derive(Debug)]
pub struct Entry<K, V>
where
    K: Eq + Hash,
{
    /// The wrapped key of the entry.
    key: Key<K>,
    /// The value stored in the cache.
    value: V,
    /// An optional timestamp indicating when this entry becomes invalid.
    expired_at: Option<Instant>,
}

impl<K, V> Entry<K, V>
where
    K: Eq + Hash,
{
    /// Creates a new cache entry with a specified expiration.
    ///
    /// # Parameters
    /// * `key`: The raw key to be wrapped in a [Key].
    /// * `value`: The data to store.
    /// * `expired_at`: An [Instant] in the future when the entry expires, or `None` for no limit.
    #[inline]
    pub fn new(key: K, value: V, expired_at: Option<Instant>) -> Self {
        Self {
            key: Key::new(key),
            value,
            expired_at,
        }
    }

    /// Returns a reference to the entry's [Key].
    #[inline]
    pub fn key(&self) -> &Key<K> {
        &self.key
    }

    /// Returns a reference to the stored value.
    #[inline]
    pub fn value(&self) -> &V {
        &self.value
    }

    /// Checks if the entry has passed its expiration deadline.
    ///
    /// Returns `true` if `expired_at` is a time in the past relative to [Instant::now].
    /// If no expiration was set, this always returns `false`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::{Duration, Instant};
    /// use omega::core::entry::Entry;
    /// // Entry that expired 1 second ago
    /// let past = Instant::now() - Duration::from_secs(1);
    /// let entry = Entry::new("key", "value", Some(past));
    /// assert!(entry.is_expired());
    /// ```
    #[inline]
    pub fn is_expired(&self) -> bool {
        self.expired_at
            .is_some_and(|expired_at| Instant::now() > expired_at)
    }
}
