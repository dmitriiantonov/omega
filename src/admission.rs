use crate::cms::CountMinSketch;
use crate::core::key::Key;
use std::borrow::Borrow;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};

pub trait AdmissionPolicy<K>
where
    K: Eq + Hash,
{
    fn record<Q>(&self, key: &Q)
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash;

    fn admit<Q>(&self, candidate: &Q, victim: &Q) -> bool
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash;
}

pub struct FrequentPolicy<K>
where
    K: Eq + Hash,
{
    cms: CountMinSketch,
    count: AtomicUsize,
    last_decay: AtomicUsize,
    threshold: usize,
    _phantom: PhantomData<K>,
}

impl<K> FrequentPolicy<K>
where
    K: Eq + Hash,
{
    #[inline]
    pub fn new(cms_width: usize, cms_height: usize, threshold: usize) -> Self {
        Self {
            cms: CountMinSketch::new(cms_width, cms_height),
            count: Default::default(),
            last_decay: Default::default(),
            threshold,
            _phantom: Default::default(),
        }
    }
}

impl<K> FrequentPolicy<K>
where
    K: Eq + Hash,
{
    fn maybe_decay(&self) {
        let counter = self.count.fetch_add(1, AcqRel) + 1;
        let last_decay = self.last_decay.load(Acquire);

        if (counter < last_decay || counter - last_decay >= self.threshold)
            && self
                .last_decay
                .compare_exchange_weak(last_decay, counter, Release, Relaxed)
                .is_ok()
        {
            self.cms.decay();
        }
    }
}

impl<K> AdmissionPolicy<K> for FrequentPolicy<K>
where
    K: Eq + Hash,
{
    fn record<Q>(&self, key: &Q)
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.cms.inc(key);
        self.maybe_decay();
    }

    fn admit<Q>(&self, candidate: &Q, victim: &Q) -> bool
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash,
    {
        let candidate_frequency = self.cms.frequency(candidate);
        let victim_frequency = self.cms.frequency(victim);
        candidate_frequency >= victim_frequency
    }
}

pub struct AlwaysAdmission<K>
where
    K: Eq + Hash,
{
    _phantom: PhantomData<K>,
}

impl<K> Default for AlwaysAdmission<K>
where
    K: Eq + Hash,
 {
    fn default() -> Self {
        Self::new()
    }
}

impl<K> AlwaysAdmission<K>
where
    K: Eq + Hash,
{
    pub fn new() -> Self {
        Self {
            _phantom: Default::default(),
        }
    }
}

impl<K> AdmissionPolicy<K> for AlwaysAdmission<K>
where
    K: Eq + Hash,
{
    fn record<Q>(&self, _key: &Q)
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash,
    {
    }

    fn admit<Q>(&self, _: &Q, _: &Q) -> bool
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash,
    {
        true
    }
}
