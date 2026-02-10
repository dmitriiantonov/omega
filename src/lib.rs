use crate::admission::{AdmissionPolicy, FrequentPolicy, NoAdmission};
use crate::clock::ClockCache;
use crate::core::engine::CacheEngine;
use crate::core::handler::Handler;
use crate::core::key::Key;
use std::borrow::Borrow;
use std::hash::Hash;
use std::marker::PhantomData;

mod clock;
mod cms;

mod admission;
mod config;
mod core;

pub struct Cache<E, K, V, P>
where
    E: CacheEngine<K, V>,
    K: Eq + Hash,
    P: AdmissionPolicy<K>,
{
    engine: E,
    admission_policy: P,
    _phantom: PhantomData<(K, V)>,
}

impl<E, K, V, P> Cache<E, K, V, P>
where
    E: CacheEngine<K, V>,
    K: Eq + Hash,
    P: AdmissionPolicy<K>,
{
    pub fn get<Q>(&self, key: &Q) -> Option<Handler<K, V>>
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.admission_policy.record(key);
        self.engine.get(key)
    }

    pub fn insert(&self, key: K, value: V) -> Option<Handler<K, V>> {
        self.engine.insert_with(key, value, |incoming, victim| {
            self.admission_policy.admit(incoming, victim)
        })
    }

    pub fn remove<Q>(&self, key: &Q) -> Option<Handler<K, V>>
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.engine.remove(key)
    }
}

struct RecentClockCache<K, V>
where
    K: Eq + Hash,
{
    cache: Cache<ClockCache<K, V>, K, V, NoAdmission<K>>,
}

impl<K, V> RecentClockCache<K, V>
where
    K: Eq + Hash,
{
    pub fn get<Q>(&self, key: &Q) -> Option<Handler<K, V>>
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.cache.get(key)
    }

    pub fn insert(&self, key: K, value: V) -> Option<Handler<K, V>> {
        self.cache.insert(key, value)
    }

    pub fn remove<Q>(&self, key: &Q) -> Option<Handler<K, V>>
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.cache.remove(key)
    }
}

struct FrequentClockCache<K, V>
where
    K: Eq + Hash,
{
    cache: Cache<ClockCache<K, V>, K, V, FrequentPolicy<K>>,
}

impl<K, V> FrequentClockCache<K, V> where K: Eq + Hash {

    pub fn get<Q>(&self, key: &Q) -> Option<Handler<K, V>>
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.cache.get(key)
    }

    pub fn insert(&self, key: K, value: V) -> Option<Handler<K, V>> {
        self.cache.insert(key, value)
    }

    pub fn remove<Q>(&self, key: &Q) -> Option<Handler<K, V>>
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.cache.remove(key)
    }
}
