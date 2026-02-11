use crate::core::entry::Entry;
use crossbeam::epoch::Guard;
use std::hash::Hash;
use std::ptr::NonNull;

/// A guarded pointer to a cache [Entry].
///
/// `EntryRef` provides safe access to an entry stored in the Clock cache.
/// It binds the entry's memory location to an epoch [Guard], ensuring the
/// entry is not reclaimed by the cache's eviction policy while this
/// handle exists.
///
/// Because it acts as a smart pointer to the [Entry] container rather than
/// the value itself, it provides methods to access both the key and the value.
#[derive(Debug)]
pub struct Ref<K, V>
where
    K: Eq + Hash,
{
    ptr: NonNull<Entry<K, V>>,
    /// This guard ensures the entry remains valid for the lifetime of this struct.
    _guard: Guard,
}

impl<K, V> Ref<K, V>
where
    K: Eq + Hash,
{
    /// Creates a new `EntryRef`.
    ///
    /// # Safety
    /// The `ptr` must be valid and must have been pinned by the provided `guard`.
    #[inline]
    pub fn new(ptr: NonNull<Entry<K, V>>, guard: Guard) -> Self {
        Self { ptr, _guard: guard }
    }

    /// Returns a shared reference to the entry's key.
    ///
    /// # Safety
    /// This is safe because the internal epoch guard prevents the entry
    /// from being reclaimed while this `EntryRef` exists.
    #[inline]
    pub fn key(&self) -> &K {
        // SAFETY: The internal _guard ensures the memory at ptr is not deallocated.
        unsafe { self.ptr.as_ref() }.key().as_ref()
    }

    /// Returns a shared reference to the entry's value.
    ///
    /// # Safety
    /// Safe to call as long as the `EntryRef` is alive, as the memory
    /// is pinned by the epoch guard.
    #[inline]
    pub fn value(&self) -> &V {
        // SAFETY: The internal _guard ensures the memory at ptr is not deallocated.
        unsafe { self.ptr.as_ref() }.value()
    }

    /// Returns `true` if the underlying entry has passed its expiration deadline.
    #[inline]
    pub fn is_expired(&self) -> bool {
        // SAFETY: The internal _guard ensures the memory at ptr is not deallocated.
        unsafe { self.ptr.as_ref() }.is_expired()
    }
}
