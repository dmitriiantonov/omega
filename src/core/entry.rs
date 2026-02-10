use std::hash::Hash;
use crate::core::key::Key;

/// A cache entry storing a key-value pair.
#[derive(Debug)]
pub struct Entry<K, V>
where
    K: Eq + Hash,
{
    /// The key of the entry.
    key: Key<K>,
    /// The value stored in the cache.
    value: V,
}

impl<K, V> Entry<K, V>
where
    K: Eq + Hash,
{
    /// Creates a new cache entry.
    #[inline]
    pub fn new(key: K, value: V) -> Self {
        Self {
            key: Key::new(key),
            value,
        }
    }

    #[inline]
    pub fn key(&self) -> &Key<K> {
        &self.key
    }

    #[inline]
    pub fn value(&self) -> &V {
        &self.value
    }
}