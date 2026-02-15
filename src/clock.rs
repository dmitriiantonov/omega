use crate::clock::WriteResult::{Rejected, Retry, Written};
use crate::core::backoff::BackoffConfig;
use crate::core::engine::CacheEngine;
use crate::core::entry::Entry;
use crate::core::handler::Ref;
use crate::core::key::Key;
use crate::metrics::{Metrics, MetricsConfig, MetricsSnapshot};
use SlotState::{Claimed, Cold, Hot, Vacant};
use crossbeam::epoch::{Atomic, Guard, Owned, pin};
use crossbeam_epoch::Shared;
use dashmap::DashMap;
use std::borrow::Borrow;
use std::hash::Hash;
use std::ptr::NonNull;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicU8, AtomicUsize};
use std::time::Instant;

/// Represents the state of a slot in the Clock cache.
///
/// # Invariants
/// - A slot always has exactly one state.
/// - `Vacant` → slot has no entry.
/// - `Cold` → slot has an entry that is eligible for eviction.
/// - `Hot` → slot has an entry recently accessed.
/// - `Claimed` → temporary state during write; readers treat it as empty.
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum SlotState {
    Vacant = 0,
    Cold = 1,
    Hot = 2,
    Claimed = 3,
}

impl From<SlotState> for u8 {
    fn from(clock: SlotState) -> Self {
        match clock {
            Vacant => 0,
            Cold => 1,
            Hot => 2,
            Claimed => 3,
        }
    }
}

impl From<u8> for SlotState {
    fn from(clock: u8) -> Self {
        match clock {
            0 => Vacant,
            1 => Cold,
            2 => Hot,
            3 => Claimed,
            _ => unreachable!("only values 0-3 are supported"),
        }
    }
}

#[derive(Debug)]
enum WriteResult<K, V>
where
    K: Eq + Hash,
{
    Written,
    Retry(Entry<K, V>),
    Rejected,
}

/// A single slot in the Clock cache.
///
/// Holds an atomic pointer to an `Entry` and a clock state.
#[derive(Debug)]
struct Slot<K, V>
where
    K: Eq + Hash,
{
    /// The atomic entry stored in the slot.
    entry: Atomic<Entry<K, V>>,
    /// The clock state for eviction logic.
    state: AtomicU8,
}

impl<K, V> Slot<K, V>
where
    K: Eq + Hash,
{
    /// Creates a new, empty slot.
    fn empty() -> Self {
        Self {
            entry: Atomic::default(),
            state: AtomicU8::new(Vacant.into()),
        }
    }

    /// Load a value from the slot, returning a handler if present.
    ///
    /// - Upgrades `Cold` → `Hot` if accessed.
    /// - Returns `None` if slot is `Vacant` or `Claimed`, or if the key does not match.
    ///
    /// # Safety
    /// The returned handler holds a pinned guard ensuring safe access.
    fn load<Q>(&self, key: &Q, guard: Guard) -> Option<Ref<K, V>>
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let state = self.state();

        match state {
            Vacant | Claimed => None,
            Cold | Hot => {
                let shared_entry = self.entry.load(Acquire, &guard);
                if shared_entry.is_null() {
                    return None;
                }

                let entry = unsafe { shared_entry.deref() };

                if entry.key().borrow() != key || entry.is_expired() {
                    return None;
                }

                if state == Cold {
                    self.upgrade();
                }

                Some(Ref::new(NonNull::from(entry), guard))
            }
        }
    }

    /// Attempt to write a new entry into the slot.
    ///
    /// # Parameters
    /// - `entry`: reference to a `MaybeUninit<Entry<K,V>>`. Ownership is moved only on success.
    /// - `guard`: epoch guard for memory safety.
    /// - `on_evict`: called if an old entry is replaced.
    /// - `on_insert`: called after a successful insertion.
    ///
    /// # Returns
    /// - `Inserted`: successfully inserted new entry.
    /// - `Replaced`: old entry replaced, returned as `Handler`.
    /// - `Retry`: slot unavailable; caller should retry.
    ///
    /// # Safety
    /// - `entry.assume_init_read()` is called only after successfully claiming the slot.
    /// - Old entry is either deferred for destruction or returned as a handler.
    fn try_write<A, E, I>(
        &self,
        entry: Entry<K, V>,
        guard: Guard,
        admission: A,
        on_evict: E,
        on_insert: I,
    ) -> WriteResult<K, V>
    where
        A: for<'a> Fn(&'a K, &'a K) -> bool,
        E: for<'a> FnOnce(&'a Key<K>),
        I: for<'a> FnOnce(&'a Key<K>),
    {
        let state = self.state();

        if !self.is_writable(state, &guard) {
            if state == Hot {
                let _ = self.downgrade();
            }

            return Retry(entry);
        }

        if !self.claim(state) {
            return Retry(entry);
        }

        let shared_current_entry = self.entry.load(Acquire, &guard);

        if let Some(current_entry) = unsafe { shared_current_entry.as_ref() } {
            if !(current_entry.is_expired() || admission(entry.key(), current_entry.key())) {
                self.store_state(state);
                return Rejected;
            }

            on_evict(current_entry.key());
            unsafe { guard.defer_destroy(shared_current_entry) };
        }

        let key = entry.key().clone();

        self.entry.store(Owned::new(entry), Relaxed);
        on_insert(&key);
        self.state.store(Cold.into(), Release);

        Written
    }

    #[inline]
    fn is_writable(&self, state: SlotState, guard: &Guard) -> bool {
        let shared_entry = self.entry.load(Acquire, guard);

        match unsafe { shared_entry.as_ref() } {
            Some(entry) if entry.is_expired() => true,
            None => true,
            _ => match state {
                Vacant | Cold => true,
                Hot | Claimed => false,
            },
        }
    }

    #[inline]
    fn claim(&self, state: SlotState) -> bool {
        self.state
            .compare_exchange_weak(state.into(), Claimed.into(), Relaxed, Relaxed)
            .is_ok()
    }

    /// Load the current clock state atomically.
    fn state(&self) -> SlotState {
        self.state.load(Acquire).into()
    }

    /// Store a new clock state.
    fn store_state(&self, state: SlotState) {
        self.state.store(state.into(), Release);
    }

    /// Upgrade a `Cold` entry to `Hot`.
    fn upgrade(&self) -> bool {
        self.state
            .compare_exchange_weak(Cold.into(), Hot.into(), Release, Relaxed)
            .is_ok()
    }

    /// Downgrade a `Hot` entry to `Cold`.
    fn downgrade(&self) -> bool {
        self.state
            .compare_exchange_weak(Hot.into(), Cold.into(), Release, Relaxed)
            .is_ok()
    }
}

/// Concurrent fixed-size Clock cache.
///
/// Maintains a mapping from keys → slot indices and uses
/// a clock-hand eviction policy.
pub struct ClockCache<K, V>
where
    K: Eq + Hash,
{
    /// Key → slot index mapping.
    index: DashMap<Key<K>, usize>,
    /// Fixed-size array of slots.
    slots: Box<[Slot<K, V>]>,
    /// Clock hand for eviction.
    hand: AtomicUsize,
    /// Capacity mask (capacity must be power-of-two).
    capacity_mask: usize,
    /// Maximum number of elements.
    capacity: usize,
    backoff_config: BackoffConfig,
    metrics: Metrics,
}

impl<K, V> ClockCache<K, V>
where
    K: Eq + Hash,
{
    /// Create a new Clock cache with the given capacity.
    ///
    /// # Panics
    /// Capacity must be a power-of-two.
    pub fn new(
        capacity: usize,
        backoff_config: BackoffConfig,
        metrics_config: MetricsConfig,
    ) -> Self {
        let slots = (0..capacity)
            .map(|_| Slot::empty())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        let capacity_mask = capacity - 1;

        Self {
            index: DashMap::new(),
            slots,
            hand: Default::default(),
            capacity_mask,
            capacity,
            backoff_config,
            metrics: Metrics::new(metrics_config),
        }
    }
}

impl<K, V> CacheEngine<K, V> for ClockCache<K, V>
where
    K: Eq + Hash,
{
    /// Get a value by key.
    ///
    /// Returns a `Handler` that pins the entry in memory.
    fn get<Q>(&self, key: &Q) -> Option<Ref<K, V>>
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let called_at = Instant::now();
        let guard = pin();

        self.index
            .get(key)
            .and_then(|entry| {
                let index = *entry.value();

                match self.slots[index].load(key, guard) {
                    None => {
                        self.metrics.record_miss();
                        let elapsed = called_at.elapsed().as_millis() as u64;
                        self.metrics.record_latency(elapsed);
                        None
                    }
                    Some(reference) => {
                        self.metrics.record_hit();
                        let elapsed = called_at.elapsed().as_millis() as u64;
                        self.metrics.record_latency(elapsed);
                        Some(reference)
                    }
                }
            })
            .or_else(|| {
                self.metrics.record_miss();
                let elapsed = called_at.elapsed().as_millis() as u64;
                self.metrics.record_latency(elapsed);
                None
            })
    }

    fn insert_with<F>(&self, key: K, value: V, expired_at: Option<Instant>, admission: F)
    where
        F: Fn(&K, &K) -> bool,
    {
        let called_at = Instant::now();
        let mut entry = Entry::new(key, value, expired_at);
        let mut iter = SlotIter::new(self);
        let mut backoff = self.backoff_config.build();

        loop {
            let (index, slot) = match self.index.get(entry.key()) {
                None => iter.next().expect("cache has at least one slot"),
                Some(reference) => {
                    let index = *reference.value();
                    let slot = &self.slots[index];
                    (index, slot)
                }
            };

            let guard = pin();

            match slot.try_write(
                entry,
                guard,
                &admission,
                |key| {
                    self.index.remove(key);
                    self.metrics.record_eviction();
                },
                |key| {
                    self.index.insert(key.clone(), index);
                },
            ) {
                Written | Rejected => {
                    let elapsed = called_at.elapsed().as_millis() as u64;
                    self.metrics.record_latency(elapsed);
                    break;
                }
                Retry(passed_entry) => {
                    backoff.backoff();
                    entry = passed_entry
                }
            };
        }
    }

    fn remove<Q>(&self, key: &Q) -> bool
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.index.remove(key).is_some()
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline]
    fn metrics(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }
}

impl<K, V> Drop for ClockCache<K, V>
where
    K: Eq + Hash,
{
    fn drop(&mut self) {
        let guard = pin();

        for slot in &self.slots {
            let shared_old = slot.entry.swap(Shared::null(), AcqRel, &guard);

            if !shared_old.is_null() {
                unsafe { guard.defer_destroy(shared_old) }
            }
        }

        guard.flush();
    }
}

/// Iterator over slots for eviction.
///
/// Skips `Claimed` slots and advances the clock-hand atomically.
struct SlotIter<'a, K, V>
where
    K: Eq + Hash,
{
    slots: &'a [Slot<K, V>],
    hand: &'a AtomicUsize,
    capacity_mask: usize,
}

impl<'a, K, V> SlotIter<'a, K, V>
where
    K: Eq + Hash,
{
    fn new(cache: &'a ClockCache<K, V>) -> Self {
        Self {
            slots: &cache.slots,
            hand: &cache.hand,
            capacity_mask: cache.capacity_mask,
        }
    }
}

impl<'a, K, V> Iterator for SlotIter<'a, K, V>
where
    K: Eq + Hash,
{
    type Item = (usize, &'a Slot<K, V>);

    fn next(&mut self) -> Option<Self::Item> {
        let mut current = self.hand.load(Acquire);

        loop {
            let next = (current + 1) & self.capacity_mask;

            match self
                .hand
                .compare_exchange_weak(current, next, Release, Acquire)
            {
                Ok(_) => {
                    let slot = &self.slots[current];
                    let clock = slot.state();

                    if clock == Claimed {
                        current = next;
                        continue;
                    }

                    return Some((current, slot));
                }
                Err(value) => {
                    current = value;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fake::Fake;

    use fake::faker::lorem::en::Word;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{Duration, Instant};

    fn backoff_config() -> BackoffConfig {
        BackoffConfig {
            policy: crate::core::backoff::BackoffPolicy::Exponential,
            limit: 10,
        }
    }

    fn generate_data() -> String {
        Word().fake()
    }

    /// 1. Ghost Reference Safety
    /// Verifies that EntryRef (Epoch Guard) protects memory even after eviction.
    #[test]
    fn test_ghost_reference_safety() {
        let cache = Arc::new(ClockCache::new(
            2,
            backoff_config(),
            MetricsConfig::default(),
        ));
        let (key, value) = (generate_data(), generate_data());

        cache.insert(key.clone(), value.clone(), None);

        let entry_ref = cache.get(&key).expect("key should present");

        for _ in 0..10000 {
            let (key, value) = (generate_data(), generate_data());
            cache.insert(key, value, None);
        }

        assert!(cache.get(&key).is_none());

        assert_eq!(entry_ref.value(), &value);
    }

    /// 2. Second-Chance (Clock) Logic
    /// Verifies that accessed (Hot) items survive eviction longer than Cold items.
    #[test]
    fn test_clock_eviction_priority() {
        let cache = ClockCache::new(2, backoff_config(), MetricsConfig::default());

        cache.insert(1, generate_data(), None);
        cache.insert(2, generate_data(), None);

        let _ = cache.get(&1);

        cache.insert(3, generate_data(), None);

        assert!(
            cache.get(&1).is_some(),
            "K1 should have been protected by Hot state"
        );
        assert!(
            cache.get(&2).is_none(),
            "K2 should have been the first choice for eviction"
        );
    }

    /// 3. Admission Policy Rejection
    /// Verifies that the cache respects the closure's decision to skip an insertion.
    #[test]
    fn test_admission_policy_rejection() {
        let cache = ClockCache::new(10, backoff_config(), MetricsConfig::default());

        cache.insert(1, "important".to_string(), None);

        cache.insert_with(2, "garbage".to_string(), None, |_, _| false);

        for i in 2..=10 {
            cache.insert(i, generate_data(), None);
        }

        cache.insert_with(11, "rejected".to_string(), None, |_, _| false);

        assert!(
            cache.get(&11).is_none(),
            "Item should have been rejected by policy"
        );
    }

    /// 4. TTL Expiration
    /// Verifies that the public API hides expired items.
    #[test]
    fn test_ttl_expiration() {
        let cache = ClockCache::new(10, backoff_config(), MetricsConfig::default());
        let short_lived = Duration::from_millis(50);
        let (key, value) = ("key".to_string(), "value".to_string());

        cache.insert(
            key.clone(),
            value.clone(),
            Some(Instant::now() + short_lived),
        );
        assert!(cache.get(&key).is_some());

        thread::sleep(Duration::from_millis(100));

        assert!(
            cache.get(&key).is_none(),
            "Expired item should not be accessible"
        );
    }

    #[test]
    fn test_concurrent_hammer() {
        let cache = Arc::new(ClockCache::new(
            64,
            backoff_config(),
            MetricsConfig::default(),
        ));
        let threads = 8;
        let ops = 1000;
        let barrier = Arc::new(Barrier::new(threads));

        let mut handles = vec![];
        for t in 0..threads {
            let c = Arc::clone(&cache);
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                b.wait();
                for i in 0..ops {
                    let key = (t * 100) + (i % 50); // Mix of private and shared keys
                    c.insert(key, i, None);
                    let _ = c.get(&key);
                    if i % 10 == 0 {
                        c.remove(&key);
                    }
                }
            }));
        }

        for h in handles {
            h.join().expect("Thread panicked");
        }
    }
}
