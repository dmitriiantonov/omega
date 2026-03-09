use crate::core::key::Key;
use dashmap::DashMap;
use std::borrow::Borrow;
use std::hash::Hash;

pub struct IndexTable<K>
where
    K: Eq + Hash,
{
    table: DashMap<Key<K>, u64>,
}

impl<K> IndexTable<K>
where
    K: Eq + Hash,
{
    pub fn new() -> Self {
        Self {
            table: DashMap::new(),
        }
    }

    #[inline]
    pub fn get<Q>(&self, key: &Q) -> Option<u64>
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.table.get(key).map(|entry| *entry.value())
    }

    #[inline]
    pub fn insert(&self, key: Key<K>, index: u64) {
        self.table.insert(key, index);
    }

    #[inline]
    pub fn remove<Q>(&self, key: &Q) -> Option<u64>
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.table.remove(key).map(|(_, index)| index)
    }
}

impl<K> Default for IndexTable<K>
where
    K: Eq + Hash,
{
    fn default() -> Self {
        Self::new()
    }
}
