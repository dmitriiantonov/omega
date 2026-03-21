use crate::core::cms::CountMinSketch;
use crate::core::engine::CacheEngine;
use crate::core::entry::Entry;
use crate::core::entry_ref::Ref;
use crate::core::index::IndexTable;
use crate::core::key::Key;
use crate::core::request_quota::RequestQuota;
use crate::core::ring::RingQueue;
use crate::core::tag::{Index, Tag};
use crate::core::thread_context::ThreadContext;
use crate::core::utils;
use crate::metrics::{Metrics, MetricsConfig, MetricsSnapshot};
use crossbeam::utils::CachePadded;
use crossbeam_epoch::{Atomic, Owned, pin};
use crossbeam_epoch::{Guard, Shared};
use std::borrow::Borrow;
use std::hash::Hash;
use std::ptr::NonNull;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::time::Instant;
use utils::hash;

pub struct Slot<K, V>
where
    K: Eq + Hash,
{
    entry: Atomic<Entry<K, V>>,
    tag: AtomicU64,
}

impl<K, V> Slot<K, V>
where
    K: Eq + Hash,
{
    #[inline(always)]
    fn new() -> Self {
        Self {
            entry: Atomic::null(),
            tag: AtomicU64::default(),
        }
    }
}

impl<K, V> Default for Slot<K, V>
where
    K: Eq + Hash,
{
    #[inline(always)]
    fn default() -> Self {
        Slot::new()
    }
}

/// A high-concurrency, segmented cache implementing the S3-FIFO eviction algorithm.
///
/// This structure organizes memory into a multi-tiered hierarchy to achieve
/// scan-resistance and high hit rates, specifically optimized for modern
/// multicore processors.
///
/// # Architecture
/// S3-FIFO (Simple Scalable Static FIFO) extends traditional FIFO by using
/// three distinct queues:
/// 1. **Probationary**: A small FIFO queue (typically 10% of capacity) for new entries.
/// 2. **Protected**: A large FIFO queue for frequently accessed entries.
/// 3. **Ghost**: A "shadow" queue that tracks the hashes of evicted entries to
///    inform future admission decisions.
///
/// # Concurrency & Performance
/// - **Lock-Free Design**: Uses atomic operations and `crossbeam-epoch` for
///   thread-safe access without global mutexes.
/// - **False Sharing Protection**: Slots are wrapped in `CachePadded` to ensure
///   different threads don't invalidate each other's CPU cache lines.
/// - **Index Pool**: A dedicated queue manages slot reuse, eliminating the
///   need for expensive memory allocations during the steady state.
pub struct S3FIFOCache<K, V>
where
    K: Eq + Hash,
{
    /// Mapping of keys to versioned indices for fast lookups.
    index_table: IndexTable<K>,
    /// Contiguous storage for cache entries, padded to prevent false sharing.
    slots: Box<[CachePadded<Slot<K, V>>]>,
    /// The protected segment of the cache.
    protected_segment: RingQueue,
    /// The probationary segment for new data.
    probation_segment: RingQueue,
    /// Metadata queue for tracking evicted entry hashes.
    ghost_queue: RingQueue,
    /// Collection of available slot indices ready for new allocations.
    index_pool: RingQueue,
    /// Frequency estimator used to decide if an entry should bypass probation.
    ghost_filter: CountMinSketch,
    /// Hit/Miss counters and latency tracking.
    metrics: Metrics,
    /// Total number of entries the cache can hold.
    capacity: usize,
}

impl<K, V> S3FIFOCache<K, V>
where
    K: Eq + Hash,
{
    /// Initializes a new cache instance with a segmented S3-FIFO architecture.
    ///
    /// This constructor allocates the underlying slot storage and partitions the
    /// cache capacity into functional segments designed to balance scan-resistance
    /// with high hit rates.
    ///
    /// # Segmentation Logic
    /// - **Probation Segment (10%)**: Acts as the initial landing zone for new entries.
    ///   It prevents "one-hit wonders" from polluting the main cache body.
    /// - **Protected Segment (90%)**: Houses frequency-proven entries. Data here has
    ///   survived a probationary period or was identified as frequent via the ghost filter.
    /// - **Ghost Queue & Filter**: A historical tracking mechanism sized to match total
    ///   capacity. It records the hashes of recently evicted items, allowing the
    ///   admission policy to "remember" and promote returning keys.
    ///
    /// # Resource Initialization
    /// - **Index Pool**: Pre-populated with all available slot indices (0 to capacity).
    ///   This acts as a lock-free allocator for cache slots.
    /// - **Cache Padding**: Each `Slot` is wrapped in `CachePadded` to prevent
    ///   "false sharing," a critical optimization for performance on high-core-count
    ///   processors like the M1 Pro.
    /// - **Ghost Filter**: Uses a `CountMinSketch` with a depth of 4 to provide space-efficient
    ///   frequency estimation for the admission policy.
    ///
    /// # Parameters
    /// - `capacity`: The total number of entries the cache can hold. This value is
    ///   distributed between the probation and protected segments.
    /// - `metrics_config`: Configuration for hit/miss/latency tracking.
    #[inline]
    pub fn new(capacity: usize, metrics_config: MetricsConfig) -> Self {
        const GHOST_FILTER_DEPTH: usize = 4;

        let probation_segment_capacity = (capacity as f64 * 0.1) as usize;
        let protected_segment_capacity = capacity - probation_segment_capacity;

        let probation_segment = RingQueue::new(probation_segment_capacity);
        let protected_segment = RingQueue::new(protected_segment_capacity);
        let ghost_queue = RingQueue::new(capacity);

        let index_pool = RingQueue::new(capacity);

        let context = ThreadContext::default();

        for index in 0..capacity {
            let _ = index_pool.push(index as u64, &context);
        }

        let metrics = Metrics::new(metrics_config);

        let slots = (0..capacity)
            .map(|_| CachePadded::new(Slot::new()))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            index_table: IndexTable::new(),
            slots,
            protected_segment,
            probation_segment,
            ghost_queue,
            index_pool,
            ghost_filter: CountMinSketch::new(capacity, GHOST_FILTER_DEPTH),
            metrics,
            capacity,
        }
    }

    /// Retrieves a value from the cache, upgrading its frequency on a successful match.
    ///
    /// This method implements a lock-free read path that utilizes atomic tags for
    /// fast validation before accessing the actual entry memory.
    ///
    /// # Control Flow
    /// 1. **Index Resolution**: Performs a lookup in the `index_table`. If the key is
    ///    not found, records a miss and returns `None`.
    /// 2. **Tag Validation**: Loads the slot's `Tag` using `Acquire` semantics and
    ///    validates it against the provided `hash` and `index`. This prevents
    ///    accessing a slot that has been repurposed (ABA protection).
    /// 3. **Liveness & Expiration**:
    ///    - Checks if the `Entry` is null or if the stored key has changed.
    ///    - Validates the entry's TTL. If expired, it records a miss.
    /// 4. **Frequency Upgrade**:
    ///    - Attempts to increment the access frequency in the `Tag` via `compare_exchange_weak`.
    ///    - On success, the entry is considered "Hot," potentially protecting it from
    ///      future eviction.
    ///    - On failure (contention), the thread performs a backoff and retries the loop.
    /// 5. **Reference Return**: On a successful hit, returns a `Ref` which wraps
    ///    the entry and the `Guard`, ensuring memory remains valid for the caller.
    ///
    /// # Memory Model & Synchronization
    /// - **Acquire/Release**: The `Tag` load (`Acquire`) synchronizes with the `insert`
    ///   or `evict` stores (`Release`), ensuring the `entry` pointer is valid.
    /// - **Lock-Free Reads**: Readers never block writers. The frequency update is
    ///   optimistic and handles contention via the `ThreadContext` wait/decay mechanism.
    ///
    /// # Parameters
    /// - `key`: The key to look up.
    /// - `context`: Thread-local state for frequency decay and contention management.
    ///
    /// # Returns
    /// - `Some(Ref<K, V>)`: A handle to the entry if found and valid.
    /// - `None`: If the key is missing, expired, or the signature mismatch occurs.
    pub fn get<Q>(&self, key: &Q, context: &ThreadContext) -> Option<Ref<K, V>>
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let called_at = Instant::now();
        let hash = hash(key);
        let guard = pin();

        loop {
            match self.index_table.get(key) {
                Some(index) => {
                    let index = Index::from(index);

                    let slot = &self.slots[index.slot_index()];
                    let mut tag = Tag::from(slot.tag.load(Acquire));

                    if !tag.is_match(index, hash) {
                        let latency = called_at.elapsed().as_millis() as u64;
                        self.metrics.record_miss();
                        self.metrics.record_latency(latency);
                        return None;
                    }

                    let entry = slot.entry.load(Relaxed, &guard);

                    match unsafe { entry.as_ref() } {
                        None => {
                            let latency = called_at.elapsed().as_millis() as u64;
                            self.metrics.record_miss();
                            self.metrics.record_latency(latency);
                            break None;
                        }
                        Some(entry_ref) => {
                            if entry_ref.key().borrow() != key || entry_ref.is_expired() {
                                let latency = called_at.elapsed().as_millis() as u64;
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

                            let latency = called_at.elapsed().as_millis() as u64;
                            self.metrics.record_hit();
                            self.metrics.record_latency(latency);

                            break Some(Ref::new(NonNull::from_ref(entry_ref), guard));
                        }
                    }
                }
                None => {
                    self.metrics.record_miss();
                    self.metrics
                        .record_latency(called_at.elapsed().as_millis() as u64);
                    return None;
                }
            }
        }
    }

    /// The main entry point for inserting or updating data within the cache.
    ///
    /// This method implements an adaptive admission policy by distinguishing between
    /// existing entries, known "hot" candidates (via the ghost filter), and new arrivals.
    ///
    /// # Control Flow
    /// 1. **Index Lookup**: Checks the `index_table` to see if the key is already resident.
    /// 2. **Update Path (Resident Key)**:
    ///    - If found, it attempts to lock the specific slot by transitioning its `Tag` to `busy`.
    ///    - It validates the `signature` and `index` to ensure the slot hasn't been repurposed (ABA protection).
    ///    - Upon a successful lock, it swaps the `Entry`, increments the access frequency in the `Tag`,
    ///      and releases the lock.
    ///    - If the lock fails or a mismatch is detected, the thread performs a backoff and retries.
    /// 3. **Admission Path (New Key)**:
    ///    - If the key is not in the index, the `ghost_filter` is consulted.
    ///    - **Promotion**: If the key's hash is present in the ghost filter (indicating it was
    ///      recently evicted or seen), it is inserted directly into the `protected_segment`.
    ///    - **Probation**: If the key is entirely new, it is placed in the `probation_queue`.
    ///
    /// # Memory Model & Synchronization
    /// - **AcqRel/Release**: The `compare_exchange_weak` and subsequent `store` on the `Tag`
    ///   ensure that the `Entry` swap is safely published to readers.
    /// - **Epoch-Based Reclamation**: `guard.defer_destroy` ensures the `old_entry` is only
    ///   deallocated when no concurrent readers hold a reference to it.
    /// - **Adaptive Backoff**: Uses `context.wait()` and `context.decay()` to handle contention
    ///   gracefully on highly active slots.
    ///
    /// # Parameters
    /// - `entry`: The key-value pair to insert.
    /// - `context`: Thread-local state for synchronization and performance metrics.
    /// - `quota`: Budget for the operation, primarily used if insertion triggers cascading evictions.
    pub fn insert(&self, entry: Entry<K, V>, context: &ThreadContext, quota: &mut RequestQuota) {
        let key = entry.key().clone();
        let hash = hash(entry.key());
        let guard = pin();

        loop {
            match self.index_table.get(&key).map(Index::from) {
                Some(index) => {
                    let slot = &self.slots[index.slot_index()];
                    let tag = Tag::from(slot.tag.load(Acquire));

                    if !(tag.is_match(index, hash)
                        && slot
                            .tag
                            .compare_exchange_weak(tag.into(), tag.busy().into(), AcqRel, Relaxed)
                            .is_ok())
                    {
                        context.wait();
                        continue;
                    }

                    context.decay();

                    let old_entry = slot.entry.swap(Owned::new(entry), Relaxed, &guard);
                    slot.tag.store(tag.increment_frequency().into(), Release);

                    unsafe { guard.defer_destroy(old_entry) };

                    break;
                }
                None => {
                    if self.ghost_filter.contains(&hash) {
                        self.push_into_protected_segment(entry, &guard, context, quota)
                    } else {
                        self.push_into_probation_queue(entry, &guard, context, quota)
                    }

                    break;
                }
            }
        }
    }

    /// Inserts a new entry into the probation segment, serving as the entry point for most new data.
    ///
    /// # Control Flow
    /// 1. **Index Acquisition**: Attempts to retrieve an index from the `index_pool`. If exhausted,
    ///    it triggers `evict_from_probation_segment` to reclaim space.
    /// 2. **Segment Insertion**: Attempts to push the acquired index into the `probation_segment`.
    /// 3. **Data Publication**:
    ///    - Stores the `Entry` into the designated slot.
    ///    - Maps the key to the index within the `IndexTable`.
    ///    - Computes and stores a new `Tag` signature to validate the slot and release the write
    ///      to concurrent readers.
    /// 4. **Contention & Overflow**: If the segment push fails (full queue), the thread performs
    ///    emergency eviction. The reclaimed index is returned to the pool, and the thread retries
    ///    until successful or the `quota` is depleted.
    ///
    /// # Memory Model & Synchronization
    /// - **Visibility Barrier**: The `Tag` store uses `Release` semantics, ensuring all previous
    ///   writes (Entry and IndexTable) are visible to any thread that performs an `Acquire`
    ///   load on that same tag.
    /// - **Resource Safety**: In cases of quota exhaustion or unexpected failure, indices are
    ///   restored to the `index_pool` to prevent permanent loss of cache capacity.
    ///
    /// # Returns
    /// This function returns early without insertion if the `quota` is exhausted during
    /// eviction or index recovery attempts.
    fn push_into_probation_queue(
        &self,
        entry: Entry<K, V>,
        guard: &Guard,
        context: &ThreadContext,
        quota: &mut RequestQuota,
    ) {
        let index = match self.index_pool.pop(context) {
            Some(index) => Index::from(index),
            None => match self.evict_from_probation_segment(guard, context, quota) {
                Some(index) => index,
                None => return,
            },
        };

        loop {
            if self.probation_segment.push(index.into(), context).is_ok() {
                let slot = &self.slots[index.slot_index()];

                let tag = Tag::from(slot.tag.load(Acquire));

                let entry = Owned::new(entry);
                let key = entry.key().clone();

                slot.entry.store(entry, Relaxed);
                self.index_table.insert(key.clone(), index.into());

                let tag = tag.with_signature(hash(key.as_ref()));
                slot.tag.store(tag.into(), Release);

                break;
            }

            match self.evict_from_probation_segment(guard, context, quota) {
                Some(evicted_index) => {
                    self.index_pool
                        .push(evicted_index.into(), context)
                        .expect("the index pool can't overflow");
                }
                None => {
                    self.index_pool
                        .push(index.into(), context)
                        .expect("the index pool can't overflow");

                    break;
                }
            }
        }
    }

    /// Evicts an entry from the probation segment, implementing a promotion path for frequently accessed keys.
    ///
    /// # Control Flow
    /// 1. **Selection**: Pops an index from the `probation_segment`. Returns `None` if the quota is
    ///    exhausted or the segment is empty.
    /// 2. **Liveness Check**: Skips slots that are `busy` or uninitialized, re-queuing them to maintain
    ///    segment integrity.
    /// 3. **Promotion Path**:
    ///    - If an entry is `Hot` (has been accessed) and is not expired, it qualifies for promotion.
    ///    - The `Tag` is reset (clearing the hot bit), and the thread attempts to move the index
    ///      into the protected segment via `promote_index`.
    ///    - If promotion succeeds, the loop breaks to the next candidate.
    /// 4. **Eviction Path**:
    ///    - If not promoted, the thread attempts to CAS the tag to `busy`.
    ///    - On success:
    ///        - The key is removed from the `IndexTable` and the slot's entry is nullified.
    ///        - The `Tag` signature is advanced to prevent ABA issues.
    ///        - The evicted key is pushed into the `ghost_queue` to track its frequency for
    ///          future admission decisions.
    ///        - The entry's memory is scheduled for deallocation via `guard.defer_destroy`.
    ///
    /// # Memory Model & Synchronization
    /// - **Acquire/Release Semantics**: Synchronizes slot data and index visibility across threads,
    ///   ensuring the `busy` state transition is globally observed before data cleanup begins.
    /// - **Ghost Synchronization**: The handover to the `ghost_queue` occurs after the entry is
    ///   made undiscoverable, ensuring a clean transition from "resident" to "remembered."
    /// - **Retry Logic**: Uses a nested loop and `compare_exchange_weak` to handle high contention
    ///   on the slot's metadata without blocking.
    ///
    /// # Returns
    /// - `Some(Index)`: The index of the successfully evicted slot, ready for reuse.
    /// - `None`: Failure to evict due to quota exhaustion or an empty probation segment.
    fn evict_from_probation_segment(
        &self,
        guard: &Guard,
        context: &ThreadContext,
        quota: &mut RequestQuota,
    ) -> Option<Index> {
        while quota.consume()
            && let Some(index) = self.probation_segment.pop(context).map(Index::from)
        {
            let slot = &self.slots[index.slot_index()];
            let mut tag = Tag::from(slot.tag.load(Acquire));
            let mut reseted = false;

            loop {
                if tag.is_busy() || tag.signature() == 0 {
                    if self.probation_segment.push(index.into(), context).is_ok() {
                        break;
                    }

                    tag = Tag::from(slot.tag.load(Acquire));
                    context.wait();
                    continue;
                }

                let entry = slot.entry.load(Relaxed, guard);

                let entry_ref =
                    unsafe { entry.as_ref().expect("the occupied entry cannot be null") };

                if !reseted && tag.is_hot() && !entry_ref.is_expired() {
                    let updated_tag = tag.reset();

                    match slot.tag.compare_exchange_weak(
                        tag.into(),
                        updated_tag.into(),
                        Release,
                        Acquire,
                    ) {
                        Ok(_) => {
                            context.decay();
                            reseted = true;
                        }
                        Err(latest) => {
                            tag = Tag::from(latest);
                            context.wait();
                            continue;
                        }
                    }

                    if self.promote_index(index, guard, context, quota) {
                        break;
                    }

                    tag = updated_tag
                }

                match slot
                    .tag
                    .compare_exchange_weak(tag.into(), tag.busy().into(), AcqRel, Acquire)
                {
                    Ok(_) => {
                        let key = entry_ref.key().clone();
                        self.index_table.remove(key.as_ref());
                        slot.entry.store(Shared::null(), Relaxed);

                        let (tag, index) = tag.advance(index);
                        slot.tag.store(tag.into(), Release);

                        let _ = self.push_into_ghost_queue(key.as_ref(), context, quota);

                        unsafe { guard.defer_destroy(entry) };

                        return Some(index);
                    }
                    Err(latest) => {
                        tag = Tag::from(latest);

                        if self.probation_segment.push(index.into(), context).is_ok() {
                            break;
                        }
                    }
                }
            }
        }

        None
    }

    /// Pushes a key's hash into the ghost queue and updates the frequency filter.
    ///
    /// # Control Flow
    /// 1. **Hashing**: Computes the hash of the key to be used as a fingerprint in the ghost structures.
    /// 2. **Insertion**: Attempts to push the hash into the `ghost_queue`.
    ///    - If successful, it increments the frequency count in the `ghost_filter` and returns.
    /// 3. **Queue Maintenance**: If the queue is full:
    ///    - Checks the `quota`. If exhausted, the operation fails.
    ///    - Pops the oldest hash from the queue to make room.
    ///    - Decrements the frequency count for the evicted hash in the `ghost_filter` to keep the filter synchronized with the queue's contents.
    /// 4. **Retry**: The loop continues until the new hash is successfully pushed or the quota limit is reached.
    ///
    /// # Logic & Invariants
    /// - **Ghost Filter Synchronization**: The `ghost_filter` (likely a Counting Bloom Filter or similar) is strictly tied to the lifetime of hashes within the `ghost_queue`. This prevents "stale" frequency counts for keys that have long since left the ghost segment.
    /// - **Admission Signaling**: The presence of a high count in the `ghost_filter` typically serves as the signal to promote a probationary entry to the protected segment upon its next access.
    ///
    /// # Returns
    /// - `true`: The hash was successfully added to the ghost queue.
    /// - `false`: The operation failed due to quota exhaustion.
    #[inline(always)]
    fn push_into_ghost_queue(
        &self,
        key: &K,
        context: &ThreadContext,
        quota: &mut RequestQuota,
    ) -> bool {
        let hash = hash(key);

        loop {
            if self.ghost_queue.push(hash, context).is_ok() {
                self.ghost_filter.increment(&hash, context);
                return true;
            }

            if !quota.consume() {
                return false;
            }

            if let Some(oldest_hash) = self.ghost_queue.pop(context) {
                self.ghost_filter.decrement(&oldest_hash, context);
            }
        }
    }

    /// Promotes an index into the protected segment, reclaiming space if necessary.
    ///
    /// # Control Flow
    /// 1. **Initial Push**: Attempts to move the provided `index` into the `protected_segment`.
    ///    If the segment has immediate capacity, the promotion is successful.
    /// 2. **Eviction Loop**: If the segment is full, the thread attempts to free a slot by
    ///    invoking `evict_from_protected_segment`.
    /// 3. **Index Recovery**: Indices reclaimed via eviction are returned to the `index_pool`
    ///    to maintain the total available slot count.
    /// 4. **Termination**: The process repeats until the original index is successfully
    ///    pushed or the `evict_from_protected_segment` call returns `None` (due to quota
    ///    exhaustion or an empty segment), signaling a failed promotion.
    ///
    /// # Invariants
    /// - **Index Conservation**: Every index evicted to make room for the promotion is
    ///   pushed to the `index_pool` to ensure no slots are "lost" during high-contention
    ///   re-balancing.
    /// - **Panic Safety**: The `index_pool` push uses an expectation that the pool
    ///   cannot overflow, assuming the pool capacity matches the total cache capacity.
    ///
    /// # Returns
    /// - `true`: The index was successfully promoted into the protected segment.
    /// - `false`: Promotion failed because the quota was exhausted before space could be cleared.
    fn promote_index(
        &self,
        index: Index,
        guard: &Guard,
        context: &ThreadContext,
        quota: &mut RequestQuota,
    ) -> bool {
        loop {
            if self.protected_segment.push(index.into(), context).is_ok() {
                return true;
            }

            match self.evict_from_protected_segment(guard, context, quota) {
                Some(evicted_index) => {
                    self.index_pool
                        .push(evicted_index.into(), context)
                        .expect("the index pool can't overflow");
                }
                None => return false,
            }
        }
    }

    /// Inserts a new entry into the protected segment, potentially triggering eviction if the segment is full.
    ///
    /// # Control Flow
    /// 1. **Index Acquisition**: Attempts to pop an available index from the `index_pool`.
    ///    If empty, it invokes `evict_from_protected_segment` to reclaim a slot.
    /// 2. **Segment Placement**: Attempts to push the index into the `protected_segment` queue.
    /// 3. **Data Publication**:
    ///    - Stores the new `Entry` into the resolved slot.
    ///    - Updates the `IndexTable` to map the key to the slot index.
    ///    - Calculates a new `Tag` signature based on the key's hash and stores it to
    ///      mark the slot as initialized and valid for readers.
    /// 4. **Contention Handling**: If the segment push fails (queue full), it performs
    ///    an emergency eviction. The evicted index is returned to the pool, and the
    ///    original operation retries until successful or the quota is exhausted.
    ///
    /// # Memory Model & Synchronization
    /// - **Publication Order**: The `Entry` is stored `Relaxed`, followed by the `IndexTable`
    ///   insertion. The `Tag` is stored with `Release` semantics, acting as the memory
    ///   barrier that makes the entry visible to concurrent readers.
    /// - **Resource Recovery**: On failed pushes or quota exhaustion, indices are
    ///   explicitly pushed back to the `index_pool` to prevent slot leakage.
    ///
    /// # Parameters
    /// - `entry`: The data to be cached.
    /// - `guard`: Epoch guard for memory reclamation safety.
    /// - `context`: Thread-local state for queue operations and backoff.
    /// - `quota`: Execution budget to prevent unbound searching during high pressure.
    fn push_into_protected_segment(
        &self,
        entry: Entry<K, V>,
        guard: &Guard,
        context: &ThreadContext,
        quota: &mut RequestQuota,
    ) {
        let index = match self.index_pool.pop(context) {
            Some(index) => Index::from(index),
            None => match self.evict_from_protected_segment(guard, context, quota) {
                Some(index) => index,
                None => return,
            },
        };

        loop {
            if self.protected_segment.push(index.into(), context).is_ok() {
                let slot = &self.slots[index.slot_index()];
                let tag = Tag::from(slot.tag.load(Acquire));

                let entry = Owned::new(entry);
                let key = entry.key().clone();

                slot.entry.store(entry, Relaxed);
                self.index_table.insert(key.clone(), index.into());

                let tag = tag.with_signature(hash(key.as_ref()));
                slot.tag.store(tag.into(), Release);

                break;
            }

            match self.evict_from_protected_segment(guard, context, quota) {
                Some(evicted_index) => {
                    self.index_pool
                        .push(evicted_index.into(), context)
                        .expect("the index pool can't overflow");
                }
                None => {
                    self.index_pool
                        .push(index.into(), context)
                        .expect("the index pool can't overflow");

                    break;
                }
            }
        }
    }

    /// Evicts an entry from the protected segment using a lock-free, second-chance algorithm.
    ///
    /// # Control Flow
    /// 1. **Selection**: Pops an index from the `protected_segment`. If the quota is
    ///    exhausted or the segment is empty, returns `None`.
    /// 2. **Liveness Check**: Validates if the slot is currently `busy` or uninitialized.
    ///    Contended slots are pushed back to the segment to maintain system liveness.
    /// 3. **Phase 1 (Second-Chance Rotation)**:
    ///    - If an entry is `Hot` and not expired, it receives a "second chance."
    ///    - Its frequency is aged (decremented), and it is re-inserted into the protected segment.
    /// 4. **Phase 2 (Atomic Eviction)**:
    ///    - If the entry is eligible for eviction, the thread attempts to CAS the slot tag to `busy`.
    ///    - On success, it synchronizes the `IndexTable`, nullifies the slot entry,
    ///      and advances the tag signature to prevent ABA issues during subsequent lookups.
    ///
    /// # Memory Model & Synchronization
    /// - **Acquire/Release Semantics**: Ensures that memory writes to the `Entry` and `IndexTable`
    ///   are visible to other threads before the `Tag` state transition is observed.
    /// - **RCU-style Reclamation**: Utilizes `guard.defer_destroy` to ensure that memory is
    ///   only reclaimed after all concurrent readers have finished their operations.
    /// - **Atomic Bit-Packing**: The `Tag` integrates the busy-lock, frequency, and signature
    ///   into a single word to allow atomic state transitions without mutexes.
    ///
    /// # Returns
    /// - `Some(Index)`: The index of the successfully cleared slot, ready for reuse.
    /// - `None`: Eviction failed due to quota exhaustion or an empty segment.
    fn evict_from_protected_segment(
        &self,
        guard: &Guard,
        context: &ThreadContext,
        quota: &mut RequestQuota,
    ) -> Option<Index> {
        while quota.consume()
            && let Some(index) = self.protected_segment.pop(context).map(Index::from)
        {
            let slot = &self.slots[index.slot_index()];
            let mut tag = Tag::from(slot.tag.load(Acquire));

            loop {
                if tag.is_busy() || tag.signature() == 0 {
                    if self.protected_segment.push(index.into(), context).is_ok() {
                        break;
                    }

                    tag = Tag::from(slot.tag.load(Acquire));
                    context.wait();
                    continue;
                }

                context.decay();

                let entry = slot.entry.load(Relaxed, guard);
                let entry_ref = unsafe { entry.as_ref().expect("occupied entry can't be null") };

                // Phase 1 Second-Chance Rotation:
                // Attempt to decide whether to evict an entry from the queue based on its frequency
                // and TTL.
                if tag.is_hot() && !entry_ref.is_expired() {
                    let updated_tag = tag.decrement_frequency();

                    match slot.tag.compare_exchange_weak(
                        tag.into(),
                        updated_tag.into(),
                        Release,
                        Acquire,
                    ) {
                        Ok(_) => {
                            if self.protected_segment.push(index.into(), context).is_ok() {
                                context.decay();
                                break;
                            }

                            tag = updated_tag;
                        }
                        Err(latest) => {
                            tag = Tag::from(latest);
                            context.wait();
                            continue;
                        }
                    }
                }

                // Phase 2 Eviction:
                // Attempt to lock the entry for eviction; if locking fails, try to insert the index back into the queue.
                match slot
                    .tag
                    .compare_exchange_weak(tag.into(), tag.busy().into(), AcqRel, Acquire)
                {
                    Ok(_) => {
                        self.index_table.remove(entry_ref.key());
                        slot.entry.store(Shared::null(), Relaxed);

                        let (next_tag, next_index) = tag.advance(index);
                        slot.tag.store(next_tag.into(), Release);

                        unsafe { guard.defer_destroy(entry) };
                        return Some(next_index);
                    }
                    Err(latest) => {
                        tag = Tag::from(latest);
                        context.wait();

                        if self.protected_segment.push(index.into(), context).is_ok() {
                            break;
                        }
                    }
                }
            }
        }

        None
    }

    /// Removes an entry from the cache by key, ensuring safe synchronization with concurrent readers and writers.
    ///
    /// This method uses a two-phase approach to safely invalidate a slot: first by locking the
    /// metadata via a `busy` bit, and then by verifying the key identity before final removal.
    ///
    /// # Control Flow
    /// 1. **Index Resolution**: Performs a lookup in the `index_table`. Returns `false` immediately
    ///    if the key is not present.
    /// 2. **Epoch & Liveness Check**: Validates the `Tag` against the provided `index` to ensure
    ///    the slot hasn't been repurposed (ABA protection). If the slot is `busy`, it performs
    ///    an adaptive backoff.
    /// 3. **Atomic Lock**: Attempts to CAS the slot tag to a `busy` state. This grants exclusive
    ///    access to the slot's entry pointer for the duration of the removal.
    /// 4. **Key Verification**: Once locked, it loads the `Entry` and performs a final check
    ///    to ensure the resident key matches the target `key`.
    ///    - **Mismatch**: If the key changed during the lock acquisition, the tag is restored
    ///      to its original state and returns `false`.
    ///    - **Match**: The key is removed from the `index_table`, and the slot tag is `reset`
    ///      (clearing frequency and busy bits) before being released.
    ///
    /// # Memory Model & Synchronization
    /// - **AcqRel/Release**: Ensures that the `IndexTable` removal and any local modifications
    ///   are globally visible before the slot's `busy` bit is cleared.
    /// - **Spin-Reduction**: Utilizes `context.wait()` and `context.decay()` to prevent
    ///   CPU-churn when multiple threads attempt to remove or update the same hot key.
    /// - **Epoch Safety**: Uses a `pin()` guard to safely inspect the entry pointer without
    ///   risking a use-after-free, even if another thread is concurrently evicting the slot.
    ///
    /// # Parameters
    /// - `key`: The key of the entry to be removed.
    /// - `context`: Thread-local state for managing backoff and contention.
    ///
    /// # Returns
    /// - `true`: The entry was found and successfully removed.
    /// - `false`: The entry was not found or the key did not match the current slot occupant.
    pub fn remove<Q>(&self, key: &Q, context: &ThreadContext) -> bool
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let index = match self.index_table.get(key) {
            None => return false,
            Some(index) => Index::from(index),
        };

        let slot = &self.slots[index.slot_index()];
        let mut tag = Tag::from(slot.tag.load(Acquire));

        loop {
            if !tag.is_epoch_match(index) {
                return false;
            }

            if tag.is_busy() {
                context.wait();
                continue;
            }

            match slot
                .tag
                .compare_exchange_weak(tag.into(), tag.busy().into(), AcqRel, Acquire)
            {
                Ok(_) => {
                    context.decay();
                }
                Err(latest) => {
                    tag = Tag::from(latest);
                    context.wait();
                    continue;
                }
            }

            let guard = pin();

            let entry = slot.entry.load(Relaxed, &guard);

            let is_key_match = unsafe { entry.as_ref() }
                .map(|entry_ref| entry_ref.key().borrow() == key)
                .unwrap_or(false);

            if !is_key_match {
                slot.tag.store(tag.into(), Release);
                return false;
            }

            self.index_table.remove(key);

            slot.tag.store(tag.reset().into(), Release);

            return true;
        }
    }
}

impl<K, V> CacheEngine<K, V> for S3FIFOCache<K, V>
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
        self.insert(entry, context, quota);
    }

    fn remove<Q>(&self, key: &Q, context: &ThreadContext) -> bool
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.remove(key, context)
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn metrics(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }
}

impl<K, V> Drop for S3FIFOCache<K, V>
where
    K: Eq + Hash,
{
    fn drop(&mut self) {
        let guard = pin();

        for slot in &self.slots {
            let entry = slot.entry.swap(Shared::null(), Relaxed, &guard);

            if !entry.is_null() {
                unsafe { guard.defer_destroy(entry) }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::utils::random_string;
    use crate::core::workload::{WorkloadGenerator, WorkloadStatistics};
    use rand::{RngExt, rng};
    use std::sync::{Arc, Mutex};
    use std::thread::scope;

    #[inline(always)]
    fn create_cache<K, V>(capacity: usize) -> S3FIFOCache<K, V>
    where
        K: Eq + Hash,
    {
        S3FIFOCache::new(capacity, MetricsConfig::default())
    }

    #[test]
    fn test_s3cache_insert_should_retrieve_stored_value() {
        let cache = create_cache(10);
        let context = ThreadContext::default();

        let key = random_string();
        let value = random_string();
        let entry = Entry::new(key.clone(), value.clone());

        cache.insert(entry, &context, &mut RequestQuota::default());

        let entry_ref = cache.get(&key, &context).expect("must present");

        assert_eq!(entry_ref.key(), &key);
        assert_eq!(entry_ref.value(), &value);
    }

    #[test]
    fn test_s3cache_insert_should_overwrite_existing_key() {
        let cache = create_cache(10);
        let context = ThreadContext::default();

        let key = random_string();
        let key_ref: &str = key.as_ref();
        let value1 = random_string();
        let value2 = random_string();

        cache.insert(
            Entry::new(key.clone(), value1.clone()),
            &context,
            &mut RequestQuota::default(),
        );

        let entry_ref = cache.get(key_ref, &context);
        assert!(entry_ref.is_some(), "the entry must present");
        assert_eq!(entry_ref.unwrap().value(), &value1);

        cache.insert(
            Entry::new(key.clone(), value2.clone()),
            &context,
            &mut RequestQuota::default(),
        );

        let entry_ref = cache.get(key_ref, &context);
        assert!(entry_ref.is_some(), "the entry must present");
        assert_eq!(entry_ref.unwrap().value(), &value2);
    }

    #[test]
    fn test_s3cache_remove_should_invalidate_entry() {
        let cache = create_cache(100);
        let context = ThreadContext::default();

        let key = random_string();
        let value = random_string();

        cache.insert(
            Entry::new(key.clone(), value.clone()),
            &context,
            &mut RequestQuota::default(),
        );

        let entry_ref = cache.get(&key, &context).expect("entry must present");

        assert_eq!(entry_ref.key(), &key);
        assert_eq!(entry_ref.value(), &value);

        assert!(cache.remove(&key, &context));

        assert!(cache.get(&key, &context).is_none());
    }

    #[test]
    fn test_s3cache_fill_beyond_capacity_should_evict_fifo() {
        let cache = create_cache(100);
        let context = ThreadContext::default();

        for _ in 0..1000 {
            let key = random_string();
            let value = random_string();
            let entry = Entry::new(key, value);
            cache.insert(entry, &context, &mut RequestQuota::default());
        }
    }

    #[test]
    fn test_s3cache_hot_entry_should_resist_eviction() {
        let cache = create_cache(1000);
        let context = &ThreadContext::default();

        let key = random_string();
        let value = random_string();
        let entry = Entry::new(key.clone(), value.clone());

        cache.insert(entry, context, &mut RequestQuota::default());

        let entry_ref = cache.get(&key, &context).expect("entry must present");

        assert_eq!(entry_ref.value(), &value);

        for _ in 0..250 {
            cache.insert(
                Entry::new(random_string(), random_string()),
                context,
                &mut RequestQuota::default(),
            );
        }

        let entry = cache.get(&key, &context).expect("must present");

        assert_eq!(entry.key(), &key);
        assert_eq!(entry.value(), &value);
    }

    #[test]
    fn test_s3cache_reinserted_ghost_entry_should_be_promoted_to_main() {
        let cache = create_cache(1000);
        let context = ThreadContext::default();

        let (key, value) = (random_string(), random_string());

        cache.insert(
            Entry::new(key.clone(), value.clone()),
            &context,
            &mut RequestQuota::default(),
        );

        for _ in 0..1000 {
            let key = random_string();
            let value = random_string();
            cache.insert(
                Entry::new(key.clone(), value.clone()),
                &context,
                &mut RequestQuota::default(),
            );
        }

        assert!(cache.get(&key, &context).is_none());

        cache.insert(
            Entry::new(key.clone(), value.clone()),
            &context,
            &mut RequestQuota::default(),
        );

        for _ in 0..1000 {
            let key = random_string();
            let value = random_string();

            cache.insert(
                Entry::new(key.clone(), value.clone()),
                &context,
                &mut RequestQuota::default(),
            );
        }

        let entry = cache.get(&key, &context).expect("entry must present");

        assert_eq!(entry.key(), &key);
        assert_eq!(entry.value(), &value);
    }

    #[test]
    fn test_s3cache_ghost_filter_should_protect_working_set() {
        let cache = create_cache(1000);
        let context = ThreadContext::default();

        let hot_entries = vec![
            (random_string(), random_string()),
            (random_string(), random_string()),
            (random_string(), random_string()),
            (random_string(), random_string()),
            (random_string(), random_string()),
        ];

        for (key, value) in &hot_entries {
            let key = key.clone();
            let value = value.clone();

            cache.insert(
                Entry::new(key, value),
                &context,
                &mut RequestQuota::default(),
            );
        }

        for i in 0..100000 {
            if i % 2 == 0 {
                let key = format!("key-{}", i);
                let value = format!("value-{}", i);
                let entry = Entry::new(key, value);
                cache.insert(entry, &context, &mut RequestQuota::default());
            } else {
                let index = rng().random_range(..hot_entries.len());
                let key = hot_entries[index].0.as_str();
                let _ = cache.get(key, &context);
            }
        }

        let count = hot_entries
            .iter()
            .map(|(key, _)| cache.get(key, &context))
            .filter(Option::is_some)
            .count();

        assert!(count >= 4);
    }

    #[test]
    fn test_s3cache_concurrent_hammer_should_not_crash_or_hang() {
        let cache = create_cache(1000);
        let num_threads = 32;
        let ops_per_thread = 5000;

        scope(|scope| {
            for _ in 0..num_threads {
                scope.spawn(|| {
                    let context = ThreadContext::default();
                    for op in 0..ops_per_thread {
                        let key = (op % 500).to_string();
                        if op % 2 == 0 {
                            cache.insert(
                                Entry::new(key, random_string()),
                                &context,
                                &mut RequestQuota::default(),
                            );
                        } else {
                            let _ = cache.get(&key, &context);
                        }
                    }
                });
            }
        });
    }

    #[test]
    fn test_s3_fifo_should_protect_hot_set_under_high_churn() {
        let capacity = 1000;
        let cache = create_cache(capacity);
        let context = ThreadContext::default();

        let num_threads = 16;
        let ops_per_thread = 10000;

        let workload_generator = WorkloadGenerator::new(20000, 1.3);
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
                    let mut thread_rng = rng();
                    let context = ThreadContext::default();

                    for _ in 0..ops_per_thread {
                        let key = workload_generator.key(&mut thread_rng);
                        workload_statistics.record(key.clone());

                        if cache.get(&key, &context).is_none() {
                            cache.insert(
                                Entry::new(key, "value"),
                                &context,
                                &mut RequestQuota::default(),
                            );
                        }
                    }
                });
            }
        });

        let top_keys_size = 500;
        let frequent_keys = workload_statistics.frequent_keys(top_keys_size);

        let count = frequent_keys.iter().fold(0, |acc, key| {
            if cache.get(key, &context).is_some() {
                acc + 1
            } else {
                acc
            }
        });

        assert!(
            count >= 400,
            "S3-FIFO efficiency dropped! Captured only {}/{} hot keys",
            count,
            top_keys_size
        );
    }

    #[test]
    fn test_s3cache_ttl_entry_should_expire() {
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

        assert!(
            cache.get(&key, &context).is_none(),
            "Entry should have expired"
        );
    }
}
