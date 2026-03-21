use crate::core::key::Key;
use std::cmp::Ordering;
use std::hash::Hash;
use std::time::{Duration, Instant};

/// A container representing a single cached item.
///
/// `Entry` holds the mapping between a [Key] and its associated value,
/// along with optional TTL (Time To Live) metadata to handle expiration.
///
/// ### Generic Constraints
/// * `K`: The raw key type, which must implement [Eq] and [Hash].
/// * `V`: The type of the value being stored.
pub struct Entry<K, V>
where
    K: Eq + Hash,
{
    /// The wrapped key of the entry.
    key: Key<K>,
    /// The value stored in the cache.
    value: V,
    /// An optional timestamp indicating when this entry becomes invalid.
    expiration: Expiration,
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
    pub fn new(key: K, value: V) -> Self {
        Self {
            key: Key::new(key),
            value,
            expiration: Expiration::Never,
        }
    }

    #[inline(always)]
    pub fn with_ttl(key: K, value: V, ttl: Duration) -> Self {
        Self {
            key: Key::new(key),
            value,
            expiration: Expiration::ttl(ttl),
        }
    }

    #[inline(always)]
    pub fn with_custom_expiration(
        key: K,
        value: V,
        is_expire: impl Fn() -> bool + 'static + Send + Sync,
    ) -> Self {
        Self {
            key: Key::new(key),
            value,
            expiration: Expiration::Custom(CustomExpiration {
                is_expired: Box::new(is_expire),
            }),
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
    #[inline]
    pub fn is_expired(&self) -> bool {
        self.expiration.is_expired()
    }
}

pub enum Expiration {
    Never,
    TTL(Instant),
    Custom(CustomExpiration),
}

pub struct CustomExpiration {
    is_expired: Box<dyn Fn() -> bool + Send + Sync>,
}

impl Expiration {
    #[inline(always)]
    pub fn never() -> Self {
        Self::Never
    }

    #[inline(always)]
    pub fn ttl(ttl: Duration) -> Self {
        Self::TTL(Instant::now() + ttl)
    }

    #[inline(always)]
    pub fn is_expired(&self) -> bool {
        match self {
            Expiration::Never => false,
            Expiration::TTL(deadline) => Instant::now().cmp(deadline) == Ordering::Greater,
            Expiration::Custom(custom) => (custom.is_expired)(),
        }
    }
}
