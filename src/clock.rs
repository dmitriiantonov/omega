use crate::clock::SlotState::{Claimed, Cold, Hot, Vacant};
use crate::core::engine::CacheEngine;
use crate::core::entry::Entry;
use crate::core::entry_ref::Ref;
use crate::core::index::IndexTable;
use crate::core::key::Key;
use crate::core::request_quota::RequestQuota;
use crate::core::thread_context::ThreadContext;
use crate::metrics::{Metrics, MetricsConfig, MetricsSnapshot};
use crossbeam::epoch::{Atomic, Guard, Owned, pin};
use crossbeam_epoch::Shared;
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

/// An implementation of a concurrent Clock cache.
///
/// The Clock algorithm is an efficient approximation of Least Recently Used (LRU).
/// It uses a circular "clock hand" to iterate over slots and evict entries that
/// haven't been recently accessed.
///
/// This implementation is lock-free for reads and uses fine-grained state
/// transitions (Vacant, Cold, Hot, Claimed) for concurrent writes and evictions.
pub struct ClockCache<K, V>
where
    K: Eq + Hash,
{
    /// Key → slot index mapping.
    index: IndexTable<K>,
    /// Fixed-size array of slots.
    slots: Box<[Slot<K, V>]>,
    /// Clock hand for eviction.
    hand: AtomicUsize,
    /// Capacity mask (capacity must be power-of-two).
    capacity_mask: usize,
    /// Maximum number of elements.
    capacity: usize,
    metrics: Metrics,
}

impl<K, V> ClockCache<K, V>
where
    K: Eq + Hash,
{
    /// Create a new Clock cache with the given capacity.
    pub fn new(capacity: usize, metrics_config: MetricsConfig) -> Self {
        let capacity = capacity.next_power_of_two();

        let slots = (0..capacity)
            .map(|_| Slot::empty())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        let capacity_mask = capacity - 1;

        Self {
            index: IndexTable::new(),
            slots,
            hand: Default::default(),
            capacity_mask,
            capacity,
            metrics: Metrics::new(metrics_config),
        }
    }

    /// Retrieves a reference to an entry associated with the provided key.
    ///
    /// If the entry exists and is valid, its state is upgraded to `Hot` if it was
    /// `Cold`. This protects the entry from eviction in the next clock cycle.
    ///
    /// # Parameters
    /// * `key`: The key to look up.
    ///
    /// # Returns
    /// Returns `Some(Ref<K, V>)` if the key is found, `None` otherwise.
    pub fn get<Q>(&self, key: &Q) -> Option<Ref<K, V>>
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let called_at = Instant::now();
        let guard = pin();

        self.index
            .get(key)
            .map(|index| index as usize)
            .and_then(|index| match self.slots[index].get(key, guard) {
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
            })
            .or_else(|| {
                self.metrics.record_miss();
                let elapsed = called_at.elapsed().as_millis() as u64;
                self.metrics.record_latency(elapsed);
                None
            })
    }

    /// Inserts a key-value pair into the cache.
    ///
    /// If the key already exists, its value is updated. If the cache is full,
    /// the clock hand advances to find a `Cold` entry to evict. `Hot` entries
    /// encountered during the search are downgraded to `Cold`.
    ///
    /// # Parameters
    /// * `entry`: The new entry to insert.
    /// * `quota`: A `RequestQuota` to limit the number of slots inspected during eviction.
    pub fn insert(&self, entry: Entry<K, V>, quota: &mut RequestQuota) {
        let called_at = Instant::now();
        let mut iter = SlotIter::new(self);

        while quota.consume() {
            let (index, slot) = match self.index.get(entry.key()).map(|index| index as usize) {
                None => iter.next().expect("cache has at least one slot"),
                Some(index) => {
                    let slot = &self.slots[index];
                    (index, slot)
                }
            };

            let guard = pin();

            let state = slot.state();

            if state == Hot {
                slot.downgrade();
                continue;
            }

            if !(slot.is_writable(state, &guard) && slot.claim(state)) {
                continue;
            }

            let entry = Owned::new(entry);
            let key = entry.key().clone();
            let victim = slot.entry.swap(entry, Relaxed, &guard);

            if let Some(victim_ref) = unsafe { victim.as_ref() } {
                self.index.remove(victim_ref.key());
                unsafe { guard.defer_destroy(victim) };
                self.metrics.record_eviction();
            }

            slot.store_state(Cold);
            self.index.insert(key, index as u64);

            break;
        }

        let elapsed = called_at.elapsed().as_millis() as u64;
        self.metrics.record_latency(elapsed);
    }

    /// Removes an entry from the cache.
    ///
    /// Returns `true` if the entry was found and successfully removed.
    pub fn remove<Q>(&self, key: &Q) -> bool
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.index.remove(key).is_some()
    }
}

impl<K, V> CacheEngine<K, V> for ClockCache<K, V>
where
    K: Eq + Hash,
{
    fn get<Q>(&self, key: &Q, _context: &ThreadContext) -> Option<Ref<K, V>>
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.get(key)
    }

    fn insert(&self, entry: Entry<K, V>, _context: &ThreadContext, quota: &mut RequestQuota) {
        self.insert(entry, quota)
    }

    fn remove<Q>(&self, key: &Q, _context: &ThreadContext) -> bool
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.remove(key)
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
    use crate::core::utils::random_string;
    use crate::core::workload::{WorkloadGenerator, WorkloadStatistics};
    use rand::rng;
    use std::thread::scope;
    use std::time::{Duration, Instant};

    #[inline(always)]
    fn create_cache<K, V>(capacity: usize) -> ClockCache<K, V>
    where
        K: Eq + Hash,
    {
        ClockCache::new(capacity, MetricsConfig::default())
    }

    #[test]
    fn test_clock_cache_insert_should_retrieve_stored_value() {
        let cache = create_cache(10);

        let key = random_string();
        let value = random_string();

        cache.insert(
            Entry::new(key.clone(), value.clone()),
            &mut RequestQuota::default(),
        );

        let entry = cache.get(&key).expect("must present");

        assert_eq!(entry.key(), &key);
        assert_eq!(entry.value(), &value);
    }

    #[test]
    fn test_clock_cache_insert_should_overwrite_existing_key() {
        let cache = create_cache(10);

        let key = random_string();
        let value1 = random_string();
        let value2 = random_string();

        cache.insert(
            Entry::new(key.clone(), value1),
            &mut RequestQuota::default(),
        );
        cache.insert(
            Entry::new(key.clone(), value2.clone()),
            &mut RequestQuota::default(),
        );

        let entry = cache.get(&key).expect("entry must present");

        assert_eq!(entry.key(), &key);
        assert_eq!(entry.value(), &value2);
    }

    #[test]
    fn test_clock_cache_remove_should_invalidate_entry() {
        let cache = create_cache(100);

        let key = random_string();
        let value = random_string();

        cache.insert(Entry::new(key.clone(), value), &mut RequestQuota::default());

        assert!(cache.get(&key).is_some());

        assert!(cache.remove(&key));

        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn test_clock_cache_ghost_reference_safety_should_protect_memory() {
        let cache = create_cache(2);

        let key = random_string();
        let value = random_string();

        cache.insert(
            Entry::new(key.clone(), value.clone()),
            &mut RequestQuota::default(),
        );

        let entry_ref = cache.get(&key).expect("key should present");

        for _ in 0..10000 {
            let (key, value) = (random_string(), random_string());
            cache.insert(Entry::new(key.clone(), value), &mut RequestQuota::default());
        }

        assert!(cache.get(&key).is_none());

        assert_eq!(entry_ref.value(), &value);
    }

    #[test]
    fn test_clock_cache_hot_entry_should_resist_eviction() {
        let cache = create_cache(2);

        cache.insert(Entry::new(1, random_string()), &mut RequestQuota::default());
        cache.insert(Entry::new(2, random_string()), &mut RequestQuota::default());

        let _ = cache.get(&1);

        cache.insert(Entry::new(3, random_string()), &mut RequestQuota::default());

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
        let key = random_string();
        let value = random_string();

        cache.insert(
            Entry::with_expiration(
                key.clone(),
                value.clone(),
                Instant::now() + Duration::from_millis(50),
            ),
            &mut RequestQuota::default(),
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
            for thread_id in 0..num_threads {
                let cache = &cache;
                s.spawn(move || {
                    for i in 0..ops_per_thread {
                        let key = (thread_id * 100) + (i % 50);
                        let value = random_string();
                        cache.insert(Entry::new(key, value), &mut RequestQuota::default());
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
            cache.insert(
                Entry::new(key.clone(), "value"),
                &mut RequestQuota::default(),
            );
            workload_statistics.record(key);
        }

        scope(|scope| {
            for _ in 0..num_threads {
                scope.spawn(|| {
                    let mut rand = rng();
                    for _ in 0..ops_per_thread {
                        let key = workload_generator.key(&mut rand);

                        if cache.get(&key).is_none() {
                            cache.insert(
                                Entry::new(key.clone(), "value"),
                                &mut RequestQuota::default(),
                            );
                            workload_statistics.record(key);
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
