use bytes::Bytes;
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;

/// A key wrapper that uses `Arc` for cheap cloning and equality comparisons.
#[repr(transparent)]
#[derive(Debug)]
pub struct Key<K>(Arc<K>)
where
    K: Eq + Hash;

impl<K> Key<K>
where
    K: Eq + Hash,
{
    /// Wraps a key in an Arc for shared ownership.
    pub(crate) fn new(key: K) -> Self {
        Self(Arc::new(key))
    }
}

impl<K> Clone for Key<K>
where
    K: Eq + Hash,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<K> PartialEq<Self> for Key<K>
where
    K: Eq + Hash,
{
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<K> Eq for Key<K> where K: Eq + Hash {}

impl<K> Hash for Key<K>
where
    K: Eq + Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state)
    }
}

impl<K> AsRef<K> for Key<K>
where
    K: Eq + Hash,
{
    fn as_ref(&self) -> &K {
        self.0.as_ref()
    }
}

impl<K> Borrow<K> for Key<K>
where
    K: Eq + Hash,
{
    fn borrow(&self) -> &K {
        self.0.as_ref()
    }
}

impl Borrow<str> for Key<String> {
    fn borrow(&self) -> &str {
        self.0.as_ref().borrow()
    }
}

impl Borrow<[u8]> for Key<String> {
    fn borrow(&self) -> &[u8] {
        self.0.as_ref().as_bytes()
    }
}

impl<T> Borrow<[T]> for Key<Vec<T>>
where
    T: Eq + Hash,
{
    fn borrow(&self) -> &[T] {
        self.0.as_ref().as_slice()
    }
}

impl Borrow<[u8]> for Key<Bytes> {
    fn borrow(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl<K> Deref for Key<K>
where
    K: Eq + Hash,
{
    type Target = K;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}
