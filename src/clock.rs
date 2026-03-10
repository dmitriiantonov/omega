use crate::clock::WriteResult::{Rejected, Retry, Written};
use crate::core::backoff::BackoffConfig;
use crate::core::engine::CacheEngine;
use crate::core::entry::Entry;
use crate::core::entry_ref::Ref;
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

    /// Attempts to retrieve a value from the slot.
    ///
    /// If the slot contains a valid, non-expired entry matching the provided key,
    /// it returns a `Ref` handle. This handle consumes the provided `Guard` to
    /// ensure the entry's memory remains valid for the duration of the handle's life.
    ///
    /// # State Transitions
    /// - Accessing a `Cold` entry triggers an asynchronous upgrade to `Hot`.
    /// - Returns `None` if the slot is `Vacant`, `Claimed`, or contains a different/expired key.
    fn get<Q>(&self, key: &Q, guard: Guard) -> Option<Ref<K, V>>
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

    /// Performs a synchronized write operation on the slot.
    ///
    /// This method manages the insertion or replacement of entries using a
    /// Compare-and-Swap (CAS) on the slot state to prevent race conditions
    /// between readers and writers.
    ///
    /// # Safety
    /// When an old entry is replaced, its memory is reclaimed via `guard.defer_destroy`
    /// to ensure any concurrent readers can finish their operations safely.
    ///
    /// # Returns
    /// - `Written`: The new entry was stored.
    /// - `Rejected`: Admission policy or expiration check denied the write.
    /// - `Retry`: The slot was hot or claimed; the caller should attempt another slot.
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

    /// Attempts to transition the slot to the `Claimed` state to secure exclusive write access.
    ///
    /// This is a "lock-free" acquisition. If the current state has drifted from the
    /// provided `state` parameter since it was last read, the exchange will fail,
    /// signaling that another thread has either updated or claimed the slot.
    ///
    /// # Returns
    /// - `true`: The slot is now `Claimed` by the current thread.
    /// - `false`: The state changed externally; the caller must re-evaluate.
    #[inline]
    fn claim(&self, state: SlotState) -> bool {
        self.state
            .compare_exchange_weak(state.into(), Claimed.into(), Relaxed, Relaxed)
            .is_ok()
    }

    /// Evaluates whether the slot is eligible for eviction or insertion.
    ///
    /// A slot is considered writable if:
    /// 1. It is currently `Vacant` (empty).
    /// 2. It is `Cold` (eligible for replacement in the Clock algorithm).
    /// 3. The existing entry has passed its expiration deadline, regardless of state.
    ///
    /// If the slot is `Hot` or already `Claimed`, this returns `false`.
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
    pub fn new(
        capacity: usize,
        backoff_config: BackoffConfig,
        metrics_config: MetricsConfig,
    ) -> Self {
        let capacity = capacity.next_power_of_two();

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

                match self.slots[index].get(key, guard) {
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
    use crate::core::utils::{random_string, random_string_with_len};
    use crate::core::workload::{WorkloadGenerator, WorkloadStatistics};
    use rand::distr::{Alphanumeric, SampleString};
    use rand::{RngExt, rng};
    use std::thread::scope;
    use std::time::{Duration, Instant};

    #[inline(always)]
    fn create_cache<K, V>(capacity: usize) -> ClockCache<K, V>
    where
        K: Eq + Hash,
    {
        ClockCache::new(
            capacity,
            BackoffConfig::exponential(1000),
            MetricsConfig::default(),
        )
    }

    #[inline(always)]
    fn random_alphanumeric(len: usize) -> String {
        Alphanumeric.sample_string(&mut rand::rng(), len)
    }

    #[test]
    fn test_clock_cache_insert_should_retrieve_stored_value() {
        let cache = create_cache(10);

        let key = random_alphanumeric(32);
        let value = random_alphanumeric(255);

        cache.insert(key.clone(), value.clone(), None);

        let entry = cache.get(&key).expect("must present");

        assert_eq!(entry.key(), &key);
        assert_eq!(entry.value(), &value);
    }

    #[test]
    fn test_clock_cache_insert_should_overwrite_existing_key() {
        let cache = create_cache(10);

        let key = random_alphanumeric(32);
        let value1 = random_alphanumeric(255);
        let value2 = random_alphanumeric(255);

        cache.insert(key.clone(), value1, None);
        cache.insert(key.clone(), value2.clone(), None);

        let entry = cache.get(&key).expect("must present");

        assert_eq!(entry.key(), &key);
        assert_eq!(entry.value(), &value2);
    }

    #[test]
    fn test_clock_cache_remove_should_invalidate_entry() {
        let cache = create_cache(100);

        let key = random_alphanumeric(32);

        cache.insert(key.clone(), random_alphanumeric(255), None);

        assert!(cache.get(&key).is_some());

        assert!(cache.remove(&key));

        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn test_clock_cache_ghost_reference_safety_should_protect_memory() {
        let cache = create_cache(2);

        let key = random_alphanumeric(32);
        let value = random_alphanumeric(255);

        cache.insert(key.clone(), value.clone(), None);

        let entry_ref = cache.get(&key).expect("key should present");

        for _ in 0..10000 {
            let (key, value) = (random_alphanumeric(32), random_alphanumeric(255));
            cache.insert(key, value, None);
        }

        assert!(cache.get(&key).is_none());

        assert_eq!(entry_ref.value(), &value);
    }

    #[test]
    fn test_clock_cache_hot_entry_should_resist_eviction() {
        let cache = create_cache(2);

        cache.insert(1, random_alphanumeric(32), None);
        cache.insert(2, random_alphanumeric(32), None);

        let _ = cache.get(&1);

        cache.insert(3, random_alphanumeric(32), None);

        assert!(
            cache.get(&1).is_some(),
            "K1 should have been protected by Hot state"
        );
        assert!(
            cache.get(&2).is_none(),
            "K2 should have been the first choice for eviction"
        );
    }

    #[test]
    fn test_clock_cache_ttl_expiration_should_hide_expired_items() {
        let cache = create_cache(10);
        let short_lived = Duration::from_millis(50);
        let (key, value) = ("key".to_string(), "value".to_string());

        cache.insert(
            key.clone(),
            value.clone(),
            Some(Instant::now() + short_lived),
        );
        assert!(cache.get(&key).is_some());

        std::thread::sleep(Duration::from_millis(100));

        assert!(
            cache.get(&key).is_none(),
            "Expired item should not be accessible"
        );
    }

    #[test]
    fn test_clock_cache_concurrent_hammer_should_not_crash_or_hang() {
        let cache = create_cache(64);
        let num_threads = 8;
        let ops_per_thread = 1000;

        scope(|s| {
            for t in 0..num_threads {
                let cache = &cache;
                s.spawn(move || {
                    for i in 0..ops_per_thread {
                        let key = (t * 100) + (i % 50); // Mix of private and shared keys
                        cache.insert(key, i, None);
                        let _ = cache.get(&key);
                        if i % 10 == 0 {
                            cache.remove(&key);
                        }
                    }
                });
            }
        });
    }

    #[test]
    fn test_clock_cache_should_preserve_hot_set() {
        let capacity = 1000;
        let cache = create_cache(capacity);

        let num_threads = 8;
        let ops_per_thread = 15_000;
        let workload_generator = WorkloadGenerator::new(10000, 1.2);
        let workload_statistics = WorkloadStatistics::new();

        let mut rand = rng();

        for _ in 0..capacity {
            let key = workload_generator.key(&mut rand);
            cache.insert(key.to_string(), random_string(), None);
            workload_statistics.record(key.to_string());
        }

        scope(|scope| {
            for _ in 0..num_threads {
                scope.spawn(|| {
                    let mut rand = rng();
                    for _ in 0..ops_per_thread {
                        let key = workload_generator.key(&mut rand);

                        if cache.get(key).is_none() {
                            let value = random_string_with_len(rand.random_range(100..255));
                            cache.insert(key.to_string(), value, None);
                            workload_statistics.record(key.to_string());
                        }
                    }
                });
            }
        });

        let count = workload_statistics
            .frequent_keys(500)
            .iter()
            .fold(0, |acc, key| {
                if cache.get(key).is_some() {
                    acc + 1
                } else {
                    acc
                }
            });

        assert!(count >= 250)
    }
}
