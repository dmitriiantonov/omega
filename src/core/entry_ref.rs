use crate::core::entry::Entry;
use crossbeam::epoch::Guard;
use std::hash::Hash;
use std::ops::Deref;
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
    pub(crate) fn new(ptr: NonNull<Entry<K, V>>, guard: Guard) -> Self {
        Self { ptr, _guard: guard }
    }

    /// Access the underlying entry.
    /// Using a private method helps centralize the unsafe dereference.
    #[inline]
    fn as_entry(&self) -> &Entry<K, V> {
        // SAFETY: The existence of _guard ensures the epoch is pinned,
        // preventing the cache from reclaiming this Entry.
        unsafe { self.ptr.as_ref() }
    }

    /// Returns a shared reference to the entry's key.
    #[inline]
    pub fn key(&self) -> &K {
        self.as_entry().key()
    }

    /// Returns a shared reference to the entry's value.
    #[inline]
    pub fn value(&self) -> &V {
        self.as_entry().value()
    }

    /// Returns `true` if the underlying entry has passed its expiration deadline.
    #[inline]
    pub fn is_expired(&self) -> bool {
        self.as_entry().is_expired()
    }
}

impl<K, V> Deref for Ref<K, V>
where
    K: Eq + Hash,
{
    type Target = Entry<K, V>;

    fn deref(&self) -> &Self::Target {
        unsafe { self.ptr.as_ref() }
    }
}
