use std::hash::Hash;
use std::ptr::NonNull;
use crossbeam::epoch::Guard;
use crate::core::entry::Entry;

/// A handle to a cache entry in the Clock cache.
///
/// Allows access to both key and value, and ensures memory safety with a pinned epoch guard.
#[derive(Debug)]
pub struct Handler<K, V>
where
    K: Eq + Hash,
{
    ptr: NonNull<Entry<K, V>>,
    guard: Guard,
}

impl<K, V> Handler<K, V>
where
    K: Eq + Hash,
{
    
    #[inline]
    pub fn new(ptr: NonNull<Entry<K, V>>, guard: Guard) -> Self {
        Self {
            ptr,
            guard,
        }
    }

    /// Returns a reference to the key.
    #[inline]
    pub fn key(&self) -> &K {
        unsafe { &self.ptr.as_ref().key().as_ref() }
    }

    /// Returns a reference to the value.
    #[inline]
    pub fn value(&self) -> &V {
        unsafe { &self.ptr.as_ref().value() }
    }
}