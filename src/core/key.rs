use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// A key wrapper that uses `Arc` for cheap cloning and equality comparisons.
#[repr(transparent)]
#[derive(Debug)]
pub struct Key<K>(Arc<K>);

impl<K> Key<K> {
    /// Wraps a key in an Arc for shared ownership.
    pub(crate) fn new(key: K) -> Self {
        Self(Arc::new(key))
    }
}

impl<K> Clone for Key<K> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<K> PartialEq<Self> for Key<K>
where
    K: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<K> Eq for Key<K> where K: Eq {}

impl<K> Hash for Key<K>
where
    K: Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state)
    }
}

impl<K> AsRef<K> for Key<K> {
    fn as_ref(&self) -> &K {
        self.0.as_ref()
    }
}

impl<K> Borrow<K> for Key<K> {
    fn borrow(&self) -> &K {
        self.0.as_ref()
    }
}
