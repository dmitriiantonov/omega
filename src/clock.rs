use crate::core::engine::CacheEngine;
use crate::core::entry::Entry;
use crate::core::entry_ref::Ref;
use crate::core::index::IndexTable;
use crate::core::key::Key;
use crate::core::request_quota::RequestQuota;
use crate::core::ring::RingQueue;
use crate::core::tag::{Index, Tag};
use crate::core::thread_context::ThreadContext;
use crate::core::utils::hash;
use crate::metrics::{Metrics, MetricsConfig, MetricsSnapshot};
use crossbeam::epoch::{Atomic, pin};
use crossbeam::utils::CachePadded;
use crossbeam_epoch::{Owned, Shared};
use std::borrow::Borrow;
use std::hash::Hash;
use std::ptr::NonNull;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::time::Instant;

#[derive(Debug)]
struct Slot<K, V>
where
    K: Eq + Hash,
{
    tag: AtomicU64,
    entry: Atomic<Entry<K, V>>,
}

impl<K, V> Default for Slot<K, V>
where
    K: Eq + Hash,
{
    fn default() -> Self {
        Self {
            tag: AtomicU64::default(),
            entry: Default::default(),
        }
    }
}

/// An implementation of a concurrent Clock cache.
///
/// The Clock algorithm is an efficient $O(1)$ approximation of Least Recently Used (LRU).
/// It uses a circular "clock hand" logic to iterate over slots and evict entries that
/// haven't been recently accessed.
///
/// ### Concurrency Model
/// This implementation is **lock-free for reads** and uses fine-grained atomic state
/// transitions for concurrent writes and evictions.
///
/// * **Tag-based Synchronization:** Each slot is protected by an `AtomicU64` tag that
///   combines a versioned index, a frequency bit (Hot/Cold), and a "Busy" bit.
/// * **Memory Safety:** Uses Epoch-based Reclamation (via `crossbeam-epoch`) to ensure
///   safe destruction of evicted entries while other threads hold references.
pub struct ClockCache<K, V>
where
    K: Eq + Hash,
{
    /// Key → slot index mapping. Provides $O(1)$ lookup to find the physical slot.
    index_table: IndexTable<K>,
    /// Fixed-size array of slots. CachePadded to prevent false sharing between cores.
    slots: Box<[CachePadded<Slot<K, V>>]>,
    /// MPMC Pool of available indices.
    ///
    /// **Implementation Note:** The pool is sized to `capacity` but utilizes
    /// `tail - head` logical checks to distinguish between a "Busy Slot"
    /// (transient state) and a "Full Queue" (physical saturation).
    index_pool: RingQueue,
    capacity: usize,
    metrics: Metrics,
}

impl<K, V> ClockCache<K, V>
where
    K: Eq + Hash,
{
    /// Creates a new Clock cache with a fixed capacity.
    ///
    /// ### Initialization Logic
    /// 1. **Slots:** Allocates a contiguous block of memory for `capacity` slots.
    ///    Each slot is wrapped in `CachePadded` to eliminate L1 cache-line contention
    ///    between concurrent readers and writers.
    /// 2. **Index Pool:** Initializes the MPMC `RingQueue`.
    ///    *Note:* The pool is populated with all available slot indices (0..capacity)
    ///    immediately. This "cold start" state treats every slot as vacant and
    ///    ready for the first wave of insertions.
    /// 3. **Metrics:** Sets up sharded atomic counters for tracking hits, misses,
    ///    and eviction latency without creating a global bottleneck.
    pub fn new(capacity: usize, metrics_config: MetricsConfig) -> Self {
        let slots = (0..capacity)
            .map(|_| CachePadded::new(Slot::default()))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        let index_pool = RingQueue::new(capacity);

        let context = ThreadContext::default();

        for index in 0..capacity {
            index_pool
                .push(index as u64, &context)
                .expect("index pool can't be overflowed");
        }
        Self {
            index_table: IndexTable::new(),
            slots,
            index_pool,
            capacity,
            metrics: Metrics::new(metrics_config),
        }
    }

    /// Retrieves an entry from the cache.
    ///
    /// Uses an `Acquire` load on the slot tag to synchronize with the `Release`
    /// store of the last writer. If the entry is `Cold`, it is upgraded to `Hot`
    /// via a `compare_exchange` to protect it from the next eviction cycle.
    pub fn get<Q>(&self, key: &Q, context: &ThreadContext) -> Option<Ref<K, V>>
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let started_at = Instant::now();
        let guard = pin();
        let hash = hash(key);

        loop {
            match self.index_table.get(key) {
                Some(index) => {
                    let index = Index::from(index);

                    let slot = &self.slots[index.slot_index()];
                    let mut tag = Tag::from(slot.tag.load(Acquire));

                    if !tag.is_match(index, hash) {
                        let latency = started_at.elapsed().as_millis() as u64;
                        self.metrics.record_miss();
                        self.metrics.record_latency(latency);
                        return None;
                    }

                    let entry = slot.entry.load(Acquire, &guard);

                    match unsafe { entry.as_ref() } {
                        None => {
                            let latency = started_at.elapsed().as_millis() as u64;
                            self.metrics.record_miss();
                            self.metrics.record_latency(latency);
                            break None;
                        }
                        Some(entry_ref) => {
                            if entry_ref.key().borrow() != key || entry_ref.is_expired() {
                                let latency = started_at.elapsed().as_millis() as u64;
                                self.metrics.record_miss();
                                self.metrics.record_latency(latency);
                                break None;
                            }

                            match slot.tag.compare_exchange_weak(
                                tag.into(),
                                tag.increment_frequency().into(),
                                Release,
                                Acquire,
                            ) {
                                Ok(_) => {
                                    context.decay();
                                }
                                Err(latest) => {
                                    tag = Tag::from(latest);
                                    context.wait();
                                    continue;
                                }
                            }

                            self.metrics.record_hit();
                            self.metrics
                                .record_latency(started_at.elapsed().as_millis() as u64);

                            break Some(Ref::new(NonNull::from_ref(entry_ref), guard));
                        }
                    }
                }
                None => {
                    self.metrics.record_miss();
                    self.metrics
                        .record_latency(started_at.elapsed().as_millis() as u64);
                    return None;
                }
            }
        }
    }

    /// Inserts a new entry or updates an existing one.
    ///
    /// If the key is missing, the thread initiates the **Eviction Loop**:
    /// 1. Pops an index from the `index_pool`.
    /// 2. If the slot is `Hot`: Decrements frequency and pushes it back (Second Chance).
    /// 3. If the slot is `Cold`: Claims the slot using a `Busy` bit, swaps the entry,
    ///    and updates the `IndexTable`.
    ///
    /// ### Memory Ordering Logic
    /// 1. **Claiming:** `AcqRel` on the tag ensures exclusive access to the slot.
    /// 2. **Writing:** `Relaxed` store on the `entry` is safe because it is
    ///    guarded by the surrounding Tag barriers.
    /// 3. **Publishing:** `Release` store on the final `Tag` makes the new
    ///    entry visible to all concurrent readers.
    pub fn insert(&self, entry: Entry<K, V>, context: &ThreadContext, quota: &mut RequestQuota) {
        let started_at = Instant::now();
        let guard = pin();
        let hash = hash(entry.key());

        while quota.consume() {
            match self.index_table.get(entry.key()).map(Index::from) {
                Some(index) => {
                    let slot = &self.slots[index.slot_index()];
                    let tag = Tag::from(slot.tag.load(Acquire));

                    if !tag.is_match(index, hash) {
                        context.wait();
                        continue;
                    }

                    match slot.tag.compare_exchange_weak(
                        tag.into(),
                        tag.busy().into(),
                        AcqRel,
                        Relaxed,
                    ) {
                        Ok(_) => {
                            context.decay();
                        }
                        Err(_) => {
                            context.wait();
                            continue;
                        }
                    }

                    let old_entry = slot.entry.load(Relaxed, &guard);

                    if let Some(old_entry_ref) = unsafe { old_entry.as_ref() } {
                        if old_entry_ref.key() != entry.key() {
                            context.wait();
                            continue;
                        }

                        unsafe { guard.defer_destroy(old_entry) };
                    }

                    slot.entry.store(Owned::new(entry), Relaxed);
                    slot.tag.store(tag.increment_frequency().into(), Release);

                    self.metrics
                        .record_latency(started_at.elapsed().as_millis() as u64);

                    return;
                }
                None => match self.index_pool.pop(context) {
                    Some(index) => {
                        let index = Index::from(index);

                        let slot = &self.slots[index.slot_index()];
                        let mut tag = Tag::from(slot.tag.load(Acquire));

                        loop {
                            if tag.is_hot() {
                                if let Err(latest) = slot.tag.compare_exchange_weak(
                                    tag.into(),
                                    tag.decrement_frequency().into(),
                                    Release,
                                    Acquire,
                                ) {
                                    tag = Tag::from(latest);
                                    context.wait();
                                    continue;
                                }

                                self.index_pool
                                    .push(index.into(), context)
                                    .expect("index pool can't be overflowed");

                                context.decay();

                                break;
                            }

                            if let Err(latest) = slot.tag.compare_exchange_weak(
                                tag.into(),
                                tag.busy().into(),
                                AcqRel,
                                Acquire,
                            ) {
                                tag = Tag::from(latest);
                                context.wait();
                                continue;
                            }

                            let entry = Owned::new(entry);
                            let key = entry.key().clone();

                            let victim = slot.entry.swap(entry, Relaxed, &guard);

                            if let Some(victim_ref) = unsafe { victim.as_ref() } {
                                self.index_table.remove(victim_ref.key());
                                self.metrics.record_eviction();
                                unsafe { guard.defer_destroy(victim) };
                            }

                            let (tag, index) = tag.advance(index);
                            let tag = tag.with_signature(hash);

                            slot.tag.store(tag.into(), Release);

                            self.index_pool
                                .push(index.into(), context)
                                .expect("index pool can't be overflowed");

                            context.decay();

                            self.index_table.insert(key, index.into());

                            self.metrics
                                .record_latency(started_at.elapsed().as_millis() as u64);

                            return;
                        }
                    }
                    None => context.wait(),
                },
            }
        }
    }

    /// Removes an entry from the cache and resets its physical slot.
    ///
    /// This operation performs a two-stage teardown:
    /// 1. **Logical Removal:** Atomically removes the key from the `index_table`.
    /// 2. **Physical Invalidation:** Transitions the associated slot's `Tag` to a
    ///    reset/vacant state via a `compare_exchange` loop.
    ///
    /// ### Synchronization
    /// * **Tag Match:** If the `Tag` no longer matches the expected index or hash
    ///   (due to a concurrent eviction), the method returns `true` immediately,
    ///   as the target entry is already gone.
    /// * **Reset:** A successful `tag.reset()` ensures that any "in-flight" `get`
    ///   requests observing this slot will see a signature mismatch and return `None`.
    ///
    /// ### Memory Reclamation
    /// Note that while the `Tag` is reset, the `Entry` itself remains in the slot
    /// and the `index` is not pushed back to the `index_pool`. The physical
    /// memory is reclaimed by the `insert` logic when the Clock hand eventually
    /// encounters this reset slot.
    ///
    /// # Parameters
    /// * `key`: The key to be removed.
    /// * `context`: The thread context for backoff coordination during contention.
    ///
    /// # Returns
    /// Returns `true` if the entry was found and successfully invalidated.
    pub fn remove<Q>(&self, key: &Q, context: &ThreadContext) -> bool
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        match self.index_table.remove(key) {
            Some(index) => {
                let index = Index::from(index);

                let slot = &self.slots[index.slot_index()];
                let mut tag = Tag::from(slot.tag.load(Acquire));

                let hash = hash(key);

                loop {
                    if !tag.is_match(index, hash) {
                        return true;
                    }

                    if let Err(latest) = slot.tag.compare_exchange_weak(
                        tag.into(),
                        tag.reset().into(),
                        Release,
                        Acquire,
                    ) {
                        tag = Tag::from(latest);
                        context.wait();
                        continue;
                    }

                    return true;
                }
            }
            None => false,
        }
    }
}

impl<K, V> CacheEngine<K, V> for ClockCache<K, V>
where
    K: Eq + Hash,
{
    fn get<Q>(&self, key: &Q, context: &ThreadContext) -> Option<Ref<K, V>>
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.get(key, context)
    }

    fn insert(&self, entry: Entry<K, V>, context: &ThreadContext, quota: &mut RequestQuota) {
        self.insert(entry, context, quota)
    }

    fn remove<Q>(&self, key: &Q, context: &ThreadContext) -> bool
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.remove(key, context)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::utils::random_string;
    use crate::core::workload::{WorkloadGenerator, WorkloadStatistics};
    use rand::rng;
    use std::hash::Hash;
    use std::sync::{Arc, Mutex};
    use std::thread::scope;

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
        let context = ThreadContext::default();

        let key = random_string();
        let value = random_string();

        cache.insert(
            Entry::new(key.clone(), value.clone()),
            &context,
            &mut RequestQuota::default(),
        );

        let entry = cache.get(&key, &context).expect("must present");

        assert_eq!(entry.key(), &key);
        assert_eq!(entry.value(), &value);
    }

    #[test]
    fn test_clock_cache_insert_should_overwrite_existing_key() {
        let cache = create_cache(10);
        let context = ThreadContext::default();

        let key = random_string();
        let value1 = random_string();
        let value2 = random_string();

        cache.insert(
            Entry::new(key.clone(), value1),
            &context,
            &mut RequestQuota::default(),
        );
        cache.insert(
            Entry::new(key.clone(), value2.clone()),
            &context,
            &mut RequestQuota::default(),
        );

        let entry = cache.get(&key, &context).expect("entry must present");

        assert_eq!(entry.key(), &key);
        assert_eq!(entry.value(), &value2);
    }

    #[test]
    fn test_clock_cache_remove_should_invalidate_entry() {
        let cache = create_cache(100);
        let context = ThreadContext::default();

        let key = random_string();
        let value = random_string();

        cache.insert(
            Entry::new(key.clone(), value),
            &context,
            &mut RequestQuota::default(),
        );

        assert!(cache.get(&key, &context).is_some());

        assert!(cache.remove(&key, &context));

        assert!(cache.get(&key, &context).is_none());
    }

    #[test]
    fn test_clock_cache_ghost_reference_safety_should_protect_memory() {
        let cache = create_cache(2);
        let context = ThreadContext::default();

        let key = random_string();
        let value = random_string();

        cache.insert(
            Entry::new(key.clone(), value.clone()),
            &context,
            &mut RequestQuota::default(),
        );

        let entry_ref = cache.get(&key, &context).expect("key should present");

        for _ in 0..10000 {
            let (key, value) = (random_string(), random_string());
            cache.insert(
                Entry::new(key.clone(), value),
                &context,
                &mut RequestQuota::default(),
            );
        }

        assert!(cache.get(&key, &context).is_none());

        assert_eq!(entry_ref.value(), &value);
    }

    #[test]
    fn test_clock_cache_hot_entry_should_resist_eviction() {
        let cache = create_cache(2);
        let context = ThreadContext::default();

        cache.insert(
            Entry::new(1, random_string()),
            &context,
            &mut RequestQuota::default(),
        );
        cache.insert(
            Entry::new(2, random_string()),
            &context,
            &mut RequestQuota::default(),
        );

        let _ = cache.get(&1, &context);

        cache.insert(
            Entry::new(3, random_string()),
            &context,
            &mut RequestQuota::default(),
        );

        assert!(
            cache.get(&1, &context).is_some(),
            "K1 should have been protected by Hot state"
        );
        assert!(
            cache.get(&2, &context).is_none(),
            "K2 should have been the first choice for eviction"
        );
    }

    #[test]
    fn test_clock_cache_ttl_expiration_should_hide_expired_items() {
        let cache = create_cache(10);
        let context = ThreadContext::default();
        let key = random_string();
        let value = random_string();

        let expired = Arc::new(Mutex::new(false));

        let is_expired = {
            let expired = expired.clone();
            move || *expired.lock().unwrap()
        };

        cache.insert(
            Entry::with_custom_expiration(key.clone(), value.clone(), is_expired),
            &context,
            &mut RequestQuota::default(),
        );

        assert!(cache.get(&key, &context).is_some());

        *expired.lock().unwrap() = true;

        assert!(cache.get(&key, &context).is_none());
    }

    #[test]
    fn test_clock_cache_concurrent_hammer_should_not_crash_or_hang() {
        let cache = create_cache(1024);
        let num_threads = 16;
        let ops_per_thread = 10000;

        scope(|s| {
            for thread_id in 0..num_threads {
                let cache = &cache;
                s.spawn(move || {
                    let context = ThreadContext::default();
                    for i in 0..ops_per_thread {
                        let key = (thread_id * 100) + (i % 50);
                        let value = random_string();
                        cache.insert(
                            Entry::new(key, value),
                            &context,
                            &mut RequestQuota::default(),
                        );
                        let _ = cache.get(&key, &context);
                        if i % 10 == 0 {
                            cache.remove(&key, &context);
                        }
                    }
                });
            }
        });
    }

    #[test]
    fn test_clock_cache_should_preserve_hot_set() {
        let capacity = 1024;
        let cache = create_cache(capacity);
        let context = ThreadContext::default();

        let num_threads = 16;
        let ops_per_thread = 15_000;
        let workload_generator = WorkloadGenerator::new(10000, 1.2);
        let workload_statistics = WorkloadStatistics::new();

        let mut rand = rng();

        for _ in 0..capacity {
            let key = workload_generator.key(&mut rand);
            cache.insert(
                Entry::new(key.clone(), "value"),
                &context,
                &mut RequestQuota::default(),
            );
            workload_statistics.record(key);
        }

        scope(|scope| {
            for _ in 0..num_threads {
                scope.spawn(|| {
                    let mut rand = rng();
                    let context = ThreadContext::default();
                    for _ in 0..ops_per_thread {
                        let key = workload_generator.key(&mut rand);

                        if cache.get(&key, &context).is_none() {
                            cache.insert(
                                Entry::new(key.clone(), "value"),
                                &context,
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
                if cache.get(key, &context).is_some() {
                    acc + 1
                } else {
                    acc
                }
            });

        assert!(count >= 200)
    }
}
