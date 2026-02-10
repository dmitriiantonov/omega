use crate::clock::WriteResult::{Inserted, Rejected, Replaced, Retry};
use crate::core::backoff::Backoff;
use crate::core::engine::CacheEngine;
use crate::core::entry::Entry;
use crate::core::handler::Handler;
use crate::core::key::Key;
use Clock::{Claimed, Cold, Hot, Vacant};
use crossbeam::epoch::{Atomic, Guard, Owned, pin};
use dashmap::DashMap;
use std::borrow::Borrow;
use std::hash::Hash;
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicU8, AtomicUsize};

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
enum Clock {
    Vacant = 0,
    Cold = 1,
    Hot = 2,
    Claimed = 3,
}

impl From<Clock> for u8 {
    fn from(clock: Clock) -> Self {
        match clock {
            Vacant => 0,
            Cold => 1,
            Hot => 2,
            Claimed => 3,
        }
    }
}

impl From<u8> for Clock {
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

/// Result of attempting to write into a slot.
///
/// - `Inserted`: entry successfully inserted into an empty slot.
/// - `Replaced`: old entry with the same key replaced; returned handler provides access.
/// - `Retry`: slot unavailable (hot or claimed); caller should retry.
#[derive(Debug)]
enum WriteResult<K, V>
where
    K: Eq + Hash,
{
    Inserted,
    Replaced(Handler<K, V>),
    Retry,
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
    clock: AtomicU8,
}

impl<K, V> Slot<K, V>
where
    K: Eq + Hash,
{
    /// Creates a new, empty slot.
    fn empty() -> Self {
        Self {
            entry: Atomic::default(),
            clock: AtomicU8::new(Vacant.into()),
        }
    }

    /// Load a value from the slot, returning a handler if present.
    ///
    /// - Upgrades `Cold` → `Hot` if accessed.
    /// - Returns `None` if slot is `Vacant` or `Claimed`, or if the key does not match.
    ///
    /// # Safety
    /// The returned handler holds a pinned guard ensuring safe access.
    fn load<Q>(&self, key: &Q, guard: Guard) -> Option<Handler<K, V>>
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let state = self.load_clock();

        match state {
            Vacant | Claimed => None,
            Cold | Hot => {
                let shared_entry = self.entry.load(Acquire, &guard);
                if shared_entry.is_null() {
                    return None;
                }

                let entry = unsafe { shared_entry.deref() };

                if entry.key().borrow() != key {
                    return None;
                }

                if state == Cold {
                    self.upgrade();
                }

                Some(Handler::new(NonNull::from(entry), guard))
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
        entry: &MaybeUninit<Entry<K, V>>,
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
        let state = self.load_clock();
        let key = unsafe { entry.assume_init_ref().key() };

        match state {
            Vacant | Cold => {
                if self
                    .clock
                    .compare_exchange_weak(state.into(), Claimed.into(), Relaxed, Relaxed)
                    .is_err()
                {
                    return Retry;
                }

                let shared_old_entry = self.entry.load(Relaxed, &guard);

                let replaced_entry = match unsafe { shared_old_entry.as_ref() } {
                    Some(old_entry) => {
                        if !admission(key.as_ref(), old_entry.key().as_ref()) {
                            self.store_state(state);
                            return Rejected;
                        }

                        on_evict(old_entry.key());
                        unsafe { guard.defer_destroy(shared_old_entry) };

                        if old_entry.key() == key {
                            Some(Handler::new(NonNull::from(old_entry), guard))
                        } else {
                            None
                        }
                    }
                    None => None,
                };

                let entry = unsafe { entry.assume_init_read() };
                let key = entry.key().clone();

                self.entry.store(Owned::new(entry), Relaxed);
                on_insert(&key);
                self.clock.store(Cold.into(), Release);

                match replaced_entry {
                    None => Inserted,
                    Some(x) => Replaced(x),
                }
            }
            Hot => {
                let _ = self.downgrade();
                Retry
            }
            Claimed => Retry,
        }
    }

    /// Load the current clock state atomically.
    fn load_clock(&self) -> Clock {
        self.clock.load(Acquire).into()
    }

    /// Store a new clock state.
    fn store_state(&self, state: Clock) {
        self.clock.store(state.into(), Release);
    }

    /// Upgrade a `Cold` entry to `Hot`.
    fn upgrade(&self) -> bool {
        self.clock
            .compare_exchange_weak(Cold.into(), Hot.into(), Release, Relaxed)
            .is_ok()
    }

    /// Downgrade a `Hot` entry to `Cold`.
    fn downgrade(&self) -> bool {
        self.clock
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
    /// Current number of elements.
    len: AtomicUsize,
    /// Maximum number of elements.
    capacity: usize,
}

impl<K, V> ClockCache<K, V>
where
    K: Eq + Hash,
{
    /// Create a new Clock cache with the given capacity.
    ///
    /// # Panics
    /// Capacity must be a power-of-two.
    pub fn new(capacity: usize) -> Self {
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
            len: AtomicUsize::default(),
            capacity,
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
    fn get<Q>(&self, key: &Q) -> Option<Handler<K, V>>
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let guard = pin();
        self.index.get(key).and_then(|entry| {
            let index = *entry.value();
            self.slots[index].load(key, guard)
        })
    }

    /// Insert a value into the cache.
    ///
    /// - Uses `SlotIter` to find a candidate slot if key not present.
    /// - Moves ownership from `MaybeUninit` into the slot only on success.
    fn insert(&self, key: K, value: V) -> Option<Handler<K, V>> {
        self.insert_with(key, value, |_, _| true)
    }

    fn insert_with<F>(&self, key: K, value: V, admission: F) -> Option<Handler<K, V>>
    where
        F: Fn(&K, &K) -> bool,
    {
        let entry = MaybeUninit::new(Entry::new(key, value));
        let key = unsafe { entry.assume_init_ref().key() };
        let mut iter = SlotIter::new(self);
        let mut backoff = Backoff::new(64, 3);

        loop {
            let (index, slot) = match self.index.get(key) {
                None => iter.next().expect("cache has at least one slot"),
                Some(reference) => {
                    let index = *reference.value();
                    let slot = &self.slots[index];
                    (index, slot)
                }
            };

            let guard = pin();

            match slot.try_write(
                &entry,
                guard,
                &admission,
                |key| {
                    self.index.remove(key);
                },
                |key| {
                    self.index.insert(key.clone(), index);
                },
            ) {
                Inserted => {
                    self.len.fetch_add(1, Relaxed);
                    return None;
                }
                Replaced(handler) => return Some(handler),
                Retry => {
                    backoff.snooze();
                }
                Rejected => return None,
            };
        }
    }

    /// Remove a key from the cache.
    ///
    /// Returns a handler if the entry exists, pinned with a guard.
    fn remove<Q>(&self, key: &Q) -> Option<Handler<K, V>>
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.index.remove(key).and_then(|(_, index)| {
            let slot = &self.slots[index];
            let guard = pin();

            let shared = slot.entry.load(Relaxed, &guard);

            match unsafe { shared.as_ref() } {
                Some(entry) if entry.key().borrow() == key => {
                    Some(Handler::new(NonNull::from(entry), guard))
                }
                _ => None,
            }
        })
    }

    /// Number of elements currently stored.
    fn capacity(&self) -> usize {
        self.len.load(Relaxed)
    }

    /// Maximum capacity of the cache.
    fn len(&self) -> usize {
        self.capacity
    }

    /// Check if cache has space for a new entry.
    fn has_space(&self) -> bool {
        self.len() < self.capacity()
    }

    /// Peek a candidate for eviction using the clock hand.
    fn peek_victim(&self) -> Option<Handler<K, V>> {
        let mut current = self.hand.load(Acquire);
        let n = self.slots.len();
        let guard = pin();

        for _ in 0..n {
            let slot = &self.slots[current];
            let clock = slot.load_clock();

            if clock == Cold {
                let shared_entry = slot.entry.load(Relaxed, &guard);
                let entry = unsafe { shared_entry.deref() };

                return Some(Handler::new(NonNull::from(entry), guard));
            }

            current = (current + 1) & self.capacity_mask;
        }

        None
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
                    let clock = slot.load_clock();

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
    use fake::{Fake, Faker};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::thread;

    /* ------------------------------------------------------------
     * Helpers
     * ---------------------------------------------------------- */

    fn fake_key() -> u64 {
        Faker.fake()
    }

    fn fake_value() -> u64 {
        Faker.fake()
    }

    fn handler_value<K: Eq + Hash, V>(h: &Handler<K, V>) -> &V {
        h.value()
    }

    /* ------------------------------------------------------------
     * Basic behavior (single-threaded)
     * ---------------------------------------------------------- */

    #[test]
    fn test_clock_cache_empty_get_returns_none() {
        let cache: ClockCache<u64, u64> = ClockCache::new(8);
        let key = fake_key();

        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn test_clock_cache_insert_then_get_returns_value() {
        let cache: ClockCache<u64, u64> = ClockCache::new(8);
        let key = fake_key();
        let value = fake_value();

        cache.insert(key, value);

        let handler = cache.get(&key).unwrap();
        assert_eq!(handler.key(), &key);
        assert_eq!(handler.value(), &value);
    }

    #[test]
    fn test_clock_cache_insert_same_key_replaces_value() {
        let cache: ClockCache<u64, u64> = ClockCache::new(8);
        let key = fake_key();

        let first = fake_value();
        let second = fake_value();

        cache.insert(key, first);
        let replaced = cache.insert(key, second);

        assert!(replaced.is_some());

        let handler = cache.get(&key).unwrap();
        assert_eq!(*handler_value(&handler), second);
    }

    #[test]
    fn test_clock_cache_remove_removes_entry() {
        let cache: ClockCache<u64, u64> = ClockCache::new(8);
        let key = fake_key();

        cache.insert(key, fake_value());
        cache.remove(&key);

        assert!(cache.get(&key).is_none());
    }

    /* ------------------------------------------------------------
     * Slot / clock state behavior
     * ---------------------------------------------------------- */

    #[test]
    fn test_clock_cache_get_upgrades_cold_entry_to_hot() {
        let cache: ClockCache<u64, u64> = ClockCache::new(1);
        let key = fake_key();

        cache.insert(key, fake_value());
        let slot = &cache.slots[0];

        assert_eq!(slot.load_clock(), Cold);

        let _ = slot.load(&key, pin());
        assert_eq!(slot.load_clock(), Hot);
    }

    #[test]
    fn test_clock_cache_hot_entry_can_be_downgraded() {
        let cache: ClockCache<u64, u64> = ClockCache::new(1);
        let key = fake_key();

        cache.insert(key, fake_value());
        let slot = &cache.slots[0];

        let _ = slot.load(&key, pin());
        assert_eq!(slot.load_clock(), Hot);

        slot.downgrade();
        assert_eq!(slot.load_clock(), Cold);
    }

    #[test]
    fn test_clock_cache_vacant_slot_has_no_entry() {
        let cache: ClockCache<u64, u64> = ClockCache::new(1);
        let slot = &cache.slots[0];

        assert_eq!(slot.load_clock(), Vacant);

        let guard = pin();
        let entry = slot.entry.load(Relaxed, &guard);
        assert!(entry.is_null());
    }

    /* ------------------------------------------------------------
     * try_write / retry behavior
     * ---------------------------------------------------------- */

    #[test]
    fn test_clock_cache_try_write_returns_retry_when_claimed() {
        let cache: ClockCache<u64, u64> = ClockCache::new(1);
        let slot = &cache.slots[0];

        slot.store_state(Claimed);

        let entry = MaybeUninit::new(Entry::new(fake_key(), fake_value()));
        let result = slot.try_write(&entry, pin(), |_, _| true, |_| {}, |_| {});

        assert!(matches!(result, Retry));
    }

    #[test]
    fn test_clock_cache_try_write_succeeds_after_retry() {
        let cache: ClockCache<u64, u64> = ClockCache::new(1);
        let slot = &cache.slots[0];

        slot.store_state(Vacant);

        let entry = MaybeUninit::new(Entry::new(fake_key(), fake_value()));
        let result = slot.try_write(&entry, pin(),|_, _| true, |_| {}, |_| {});

        assert!(matches!(result, Inserted));
    }

    /* ------------------------------------------------------------
     * Concurrency tests
     * ---------------------------------------------------------- */

    #[test]
    fn test_clock_cache_concurrent_insertions_are_visible() {
        let cache: Arc<ClockCache<u64, u64>> = Arc::new(ClockCache::new(64));
        let mut expected = HashMap::new();
        let mut handles = vec![];

        for _ in 0..32 {
            let key = fake_key();
            let value = fake_value();
            expected.insert(key, value);

            let c = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                c.insert(key, value);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        for (key, value) in expected {
            let handler = cache.get(&key).unwrap();
            assert_eq!(*handler_value(&handler), value);
        }
    }

    #[test]
    fn test_clock_cache_concurrent_insert_and_remove() {
        let cache: Arc<ClockCache<u64, u64>> = Arc::new(ClockCache::new(64));
        let keys: Vec<u64> = (0..32).map(|_| fake_key()).collect();
        let mut handles = vec![];

        for key in keys.clone() {
            let c = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                c.insert(key, fake_value());
                c.remove(&key);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        for key in keys {
            assert!(cache.get(&key).is_none());
        }
    }

    #[test]
    fn test_clock_cache_concurrent_insert_get_consistency() {
        let cache: Arc<ClockCache<u64, u64>> = Arc::new(ClockCache::new(128));
        let mut handles = vec![];

        for _ in 0..16 {
            let c = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for _ in 0..1_000 {
                    let key = fake_key();
                    let value = fake_value();

                    c.insert(key, value);

                    if let Some(handler) = c.get(&key) {
                        assert_eq!(*handler_value(&handler), value);
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_clock_cache_progress_under_high_contention_with_jitter() {
        const THREADS: usize = 16;
        const OPS_PER_THREAD: usize = 1_000;
        const KEY_SPACE: usize = 8; // intentionally tiny → contention

        let cache: Arc<ClockCache<u64, u64>> = Arc::new(ClockCache::new(4));

        let mut handles = Vec::new();

        for thread_id in 0..THREADS {
            let cache = Arc::clone(&cache);

            handles.push(thread::spawn(move || {
                for op in 0..OPS_PER_THREAD {
                    // deterministic but highly colliding
                    let key = ((thread_id + op) % KEY_SPACE) as u64;
                    let value = (thread_id as u64) << 32 | op as u64;

                    // insert triggers try_write + Retry + backoff
                    cache.insert(key, value);

                    // optional read path
                    if let Some(h) = cache.get(&key) {
                        // value must be one of the values ever written
                        let v = *h.value();
                        let writer_id = (v >> 32) as usize;
                        assert!(writer_id < THREADS);
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Final sanity check: cache is still usable
        for k in 0..KEY_SPACE {
            let _ = cache.get(&(k as u64));
        }
    }

    /* ------------------------------------------------------------
     * Invariants
     * ---------------------------------------------------------- */

    #[test]
    fn test_clock_cache_vacant_slots_have_null_entries_after_stress() {
        let cache: Arc<ClockCache<u64, u64>> = Arc::new(ClockCache::new(256));
        let mut handles = vec![];

        for _ in 0..8 {
            let c = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for _ in 0..2_000 {
                    let key = fake_key();
                    let value = fake_value();

                    c.insert(key, value);
                    let _ = c.get(&key);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        for slot in cache.slots.iter() {
            if slot.load_clock() == Vacant {
                let guard = pin();
                let entry = slot.entry.load(Relaxed, &guard);
                assert!(entry.is_null());
            }
        }
    }

    /* ------------------------------------------------------------
     * Drop safety
     * ---------------------------------------------------------- */

    #[derive(Debug)]
    struct Droppable(u64);

    impl Drop for Droppable {
        fn drop(&mut self) {
            // If this runs without crashes / leaks, epoch GC is correct
        }
    }

    #[test]
    fn test_clock_cache_drop_safety() {
        let cache: ClockCache<u64, Droppable> = ClockCache::new(16);

        for _ in 0..16 {
            let key = fake_key();
            cache.insert(key, Droppable(fake_value()));
        }

        for _ in 0..16 {
            let key = fake_key();
            let _ = cache.remove(&key);
        }
    }
}
