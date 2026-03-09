use crate::core::backoff::{Backoff, BackoffConfig};
use crate::core::cms::CountMinSketch;
use crate::core::engine::CacheEngine;
use crate::core::entry::Entry;
use crate::core::entry_ref::Ref;
use crate::core::index::IndexTable;
use crate::core::key::Key;
use crate::core::ring::RingQueue;
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

/// A 64-bit metadata entry for a cache or hash table slot.
///
/// Layout:
/// - Bits 63-48: 16-bit Version ID (ABA protection)
/// - Bits 47-16: 32-bit Signature (Hashed key identifier)
/// - Bit 15:    Busy Flag (Concurrency lock)
/// - Bits 7-0:  8-bit Frequency (Access counter)
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Tag(u64);

impl Tag {
    const ID_SHIFT: u64 = 48;
    const SIGNATURE_SHIFT: u64 = 16;
    const SIGNATURE_MASK: u64 = 0xFFFF_FFFF << Self::SIGNATURE_SHIFT;
    const BUSY_MASK: u64 = 1 << 15;
    const FREQUENCY_MASK: u64 = 0xFF;

    // Mixing constants from MurmurHash3 (64-bit finalizer)
    const SIGNATURE_C1: u64 = 0xff51afd7ed558ccd;
    const SIGNATURE_C2: u64 = 0xc4ceb9fe1a85ec53;

    /// Generates a non-zero 32-bit signature from a 64-bit hash.
    /// Uses a branchless bit-avalanche to ensure high entropy.
    #[inline]
    fn make_signature(hash: u64) -> u32 {
        let mut x = hash;
        x ^= x >> 33;
        x = x.wrapping_mul(Self::SIGNATURE_C1);
        x ^= x >> 33;
        x = x.wrapping_mul(Self::SIGNATURE_C2);
        x ^= x >> 33;

        // Force the Least Significant Bit to 1.
        // This guarantees the signature is never 0, making 0 a safe 'Empty' marker.
        (x as u32) | 1
    }

    #[inline]
    fn id(self) -> u16 {
        (self.0 >> Self::ID_SHIFT) as u16
    }
    #[inline]
    fn signature(self) -> u32 {
        (self.0 >> Self::SIGNATURE_SHIFT) as u32
    }
    #[inline]
    fn is_busy(self) -> bool {
        (self.0 & Self::BUSY_MASK) != 0
    }
    #[inline]
    fn frequency(self) -> u8 {
        (self.0 & Self::FREQUENCY_MASK) as u8
    }

    #[inline]
    fn is_epoch_match(self, index: Index) -> bool {
        self.id() == index.id()
    }

    #[inline]
    fn is_hot(self) -> bool {
        self.frequency() > 0
    }

    /// Returns true if the slot is live, matches the version ID, and the signature.
    #[inline]
    fn is_match(self, index: Index, hash: u64) -> bool {
        self.id() == index.id() && !self.is_busy() && self.signature() == Self::make_signature(hash)
    }

    /// Creates a new Tag with a specific signature based on the provided hash.
    #[inline]
    fn with_signature(self, hash: u64) -> Self {
        let sig = Self::make_signature(hash);
        Self((self.0 & !Self::SIGNATURE_MASK) | (sig as u64) << Self::SIGNATURE_SHIFT)
    }

    /// Sets the busy bit for atomic/CAS operations.
    #[inline]
    fn busy(self) -> Self {
        Self(self.0 | Self::BUSY_MASK)
    }

    /// Increments the frequency counter, saturating at 255.
    #[inline]
    fn increment_frequency(self) -> Self {
        let frequency = self.frequency();
        if frequency < u8::MAX {
            Self((self.0 & !Self::FREQUENCY_MASK) | (frequency + 1) as u64)
        } else {
            self
        }
    }

    /// Decrements the frequency counter, saturating at 0.
    #[inline]
    fn decrement_frequency(self) -> Self {
        let frequency = self.frequency();
        if frequency > 0 {
            Self((self.0 & !Self::FREQUENCY_MASK) | (frequency - 1) as u64)
        } else {
            self
        }
    }

    /// Advances the state for the next inhabitant (ID++, Metadata Cleared).
    /// Returns a synchronized pair of Tag and Index.
    #[inline]
    fn advance(self, index: Index) -> (Self, Index) {
        let next_id = self.id().wrapping_add(1);
        let new_tag = Tag((next_id as u64) << Self::ID_SHIFT);
        let new_index = index.with_id(next_id);
        (new_tag, new_index)
    }

    /// Resets the entry's frequency bits to zero.
    ///
    /// This is used during promotion or specific eviction cycles to clear the
    /// "hotness" of an entry. By masking out the frequency bits, the entry
    /// loses its "second chance" status and must be accessed again to
    /// build up its frequency.
    ///
    /// Returns a new `Tag` with the same metadata and busy status, but
    /// with a frequency of 0.
    #[inline]
    fn reset(&self) -> Tag {
        Tag(self.0 & !Self::FREQUENCY_MASK)
    }
}

impl From<u64> for Tag {
    #[inline]
    fn from(raw: u64) -> Self {
        Self(raw)
    }
}

impl From<Tag> for u64 {
    #[inline]
    fn from(tag: Tag) -> u64 {
        tag.0
    }
}

/// A 64-bit versioned pointer.
///
/// Combines a 16-bit version ID with a 48-bit slot index. This ensures that
/// an index pointing to an old version of a slot cannot be used after the
/// slot has been advanced.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Index(u64);

impl Index {
    const ID_SHIFT: u64 = 48;
    const INDEX_MASK: u64 = 0x0000_FFFF_FFFF_FFFF;

    /// Creates a new versioned Index.
    ///
    /// # Arguments
    /// * `id` - The 16-bit version identifier.
    /// * `slot_index` - The physical offset in the data array (max 48 bits).
    #[inline]
    pub fn new(id: u16, slot_index: usize) -> Self {
        let clean_slot = (slot_index as u64) & Self::INDEX_MASK;
        Self(((id as u64) << Self::ID_SHIFT) | clean_slot)
    }

    /// Returns the 16-bit version identifier.
    #[inline]
    pub fn id(self) -> u16 {
        (self.0 >> Self::ID_SHIFT) as u16
    }

    /// Returns the 48-bit physical slot index.
    #[inline]
    pub fn slot_index(self) -> usize {
        (self.0 & Self::INDEX_MASK) as usize
    }

    /// Returns a copy of the index with an updated version ID.
    ///
    /// This is typically used during an `advance` operation to synchronize
    /// the index with a new `Tag`.
    #[inline]
    pub fn with_id(self, id: u16) -> Self {
        let slot_part = self.0 & Self::INDEX_MASK;
        Self(((id as u64) << Self::ID_SHIFT) | slot_part)
    }
}

impl From<u64> for Index {
    #[inline]
    fn from(raw_index: u64) -> Self {
        Index(raw_index)
    }
}

impl From<Index> for u64 {
    #[inline]
    fn from(index: Index) -> Self {
        index.0
    }
}

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

/// A cache implementation based on the S3-FIFO (Simple Scalable Static FIFO) algorithm.
///
/// S3-FIFO uses a three-queue architecture to achieve high hit rates by separating
/// transient "one-hit wonders" from frequently accessed data.
///
/// ### Architecture
/// 1. **Small Queue**: An intake FIFO for new entries (probationary area).
/// 2. **Main Queue**: A FIFO for entries that have proven their value (protected area).
/// 3. **Ghost Queue**: Tracks hashes of recently evicted items to facilitate
///    re-insertion directly into the Main Queue.
///
///
pub struct S3FIFOCache<K, V>
where
    K: Eq + Hash,
{
    /// Mapping of keys to versioned indices for fast lookups.
    index_table: IndexTable<K>,
    /// Contiguous storage for cache entries, padded to prevent false sharing.
    slots: Box<[CachePadded<Slot<K, V>>]>,
    /// The protected segment of the cache.
    main_queue: RingQueue,
    /// The probationary segment for new data.
    small_queue: RingQueue,
    /// Metadata queue for tracking evicted entry hashes.
    ghost_queue: RingQueue,
    /// Collection of available slot indices ready for new allocations.
    index_pool: RingQueue,
    /// Frequency estimator used to decide if an entry should bypass probation.
    ghost_filter: CountMinSketch,
    /// Configuration for spinning/yielding behavior under high contention.
    backoff_config: BackoffConfig,
    /// Hit/Miss counters and latency tracking.
    metrics: Metrics,
    /// Total number of entries the cache can hold.
    capacity: usize,
}

impl<K, V> S3FIFOCache<K, V>
where
    K: Eq + Hash,
{
    /// Creates a new `S3Cache` with the specified capacity.
    ///
    /// The total capacity is partitioned between a 10% small probationary
    /// segment and a 90% main protected segment. All slots are initialized
    /// as empty and their indices are added to the available pool.
    #[inline]
    pub fn new(
        capacity: usize,
        backoff_config: BackoffConfig,
        metrics_config: MetricsConfig,
    ) -> Self {
        const GHOST_FILTER_DEPTH: usize = 4;

        let small_queue_capacity = (capacity as f64 * 0.1) as usize;
        let main_queue_capacity = capacity - small_queue_capacity;

        let small_queue = RingQueue::new(small_queue_capacity, backoff_config);
        let main_queue = RingQueue::new(main_queue_capacity, backoff_config);
        let ghost_queue = RingQueue::new(capacity, backoff_config);

        let index_pool = RingQueue::new(capacity, backoff_config);

        for index in 0..capacity {
            let _ = index_pool.push(index as u64);
        }

        let metrics = Metrics::new(metrics_config);

        let slots = (0..capacity)
            .map(|_| CachePadded::new(Slot::new()))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            index_table: IndexTable::new(),
            slots,
            main_queue,
            small_queue,
            ghost_queue,
            index_pool,
            ghost_filter: CountMinSketch::new(capacity, GHOST_FILTER_DEPTH),
            backoff_config,
            metrics,
            capacity,
        }
    }

    /// Retrieves a reference to an entry associated with the provided key.
    ///
    /// If the entry exists and is valid, its access frequency is atomically
    /// incremented. This protects the entry from being removed during the
    /// next space-reclamation cycle.
    fn get<Q>(&self, key: &Q) -> Option<Ref<K, V>>
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let called_at = Instant::now();
        let hash = hash(key);
        let guard = pin();
        let mut backoff = self.backoff_config.build();

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
                            if entry_ref.key().borrow() != key {
                                let latency = called_at.elapsed().as_millis() as u64;
                                self.metrics.record_miss();
                                self.metrics.record_latency(latency);
                                break None;
                            }

                            if let Err(latest) = slot.tag.compare_exchange_weak(
                                tag.into(),
                                tag.increment_frequency().into(),
                                Release,
                                Acquire,
                            ) {
                                tag = Tag::from(latest);
                                backoff.backoff();
                                continue;
                            }

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

    /// Inserts a key-value pair into the cache.
    ///
    /// If the key's hash is recognized by the `ghost_filter`, the entry is
    /// admitted directly into the main segment. Otherwise, it is placed
    /// in the small segment for probation.
    fn insert(&self, key: K, value: V, expired_at: Option<Instant>) {
        self.insert_with(key, value, expired_at, |_, _| true);
    }

    /// Handles the admission of new entries into the probationary segment.
    ///
    /// If the segment is full, this function triggers space reclamation.
    /// The entry is stored in a slot obtained from the available pool and
    /// then appended to the small queue.
    fn insert_with(
        &self,
        key: K,
        value: V,
        expired_at: Option<Instant>,
        admission: impl Fn(&K, &K) -> bool,
    ) {
        let entry = Entry::new(key, value, expired_at);
        let key = entry.key().clone();
        let hash = hash(entry.key());
        let guard = pin();
        let mut backoff = self.backoff_config.build();

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
                        backoff.backoff();
                        continue;
                    }

                    let old_entry = slot.entry.swap(Owned::new(entry), Relaxed, &guard);
                    slot.tag.store(tag.increment_frequency().into(), Release);

                    unsafe { guard.defer_destroy(old_entry) };

                    break;
                }
                None => {
                    if self.ghost_filter.contains(&hash) {
                        self.push_into_main_queue(entry, admission, &guard, &mut backoff)
                    } else {
                        self.push_into_small_queue(entry, admission, &guard, &mut backoff)
                    }

                    break;
                }
            }
        }
    }

    /// Manages space reclamation within the probationary segment.
    ///
    /// This function iterates through entries in the small queue:
    /// 1. **Promotion**: Entries with an active access frequency are moved
    ///    to the main segment.
    /// 2. **Removal**: Entries that are expired or have no access history
    ///    are removed. Their hashes are added to the ghost filter, and
    ///    their indices are returned to the available pool.
    fn push_into_small_queue(
        &self,
        entry: Entry<K, V>,
        admission: impl Fn(&K, &K) -> bool,
        guard: &Guard,
        backoff: &mut Backoff,
    ) {
        let index = match self.index_pool.pop() {
            Some(index) => Index::from(index),
            None => {
                match self.evict_from_small_queue(|key| admission(entry.key(), key), guard, backoff)
                {
                    Some(index) => index,
                    None => return,
                }
            }
        };

        let slot = &self.slots[index.slot_index()];

        let tag = Tag::from(slot.tag.load(Acquire));

        let entry = Owned::new(entry);
        let key = entry.key().clone();

        slot.entry.store(entry, Relaxed);

        let tag = tag.with_signature(hash(key.as_ref()));

        slot.tag.store(tag.into(), Release);

        loop {
            match self.small_queue.push(index.into()) {
                Ok(_) => break,
                Err(_) => {
                    if let Some(index) = self.evict_from_small_queue(|_| true, guard, backoff) {
                        let Ok(_) = self.index_pool.push(index.into()) else {
                            unreachable!("")
                        };
                    }
                }
            }
        }

        self.index_table.insert(key, index.into());
    }

    fn evict_from_small_queue(
        &self,
        allow_eviction: impl Fn(&K) -> bool,
        guard: &Guard,
        backoff: &mut Backoff,
    ) -> Option<Index> {
        while let Some(index) = self.small_queue.pop().map(Index::from) {
            let slot = &self.slots[index.slot_index()];

            let mut tag = Tag::from(slot.tag.load(Acquire));

            loop {
                let entry = slot.entry.load(Relaxed, guard);
                let entry_ref =
                    unsafe { entry.as_ref().expect("the occupied entry cannot be null") };

                if tag.is_hot() && !entry_ref.is_expired() {
                    if let Err(latest) = slot.tag.compare_exchange_weak(
                        tag.into(),
                        tag.reset().into(),
                        Release,
                        Acquire,
                    ) {
                        tag = Tag::from(latest);
                        backoff.backoff();
                        continue;
                    }

                    self.promote_index(index, guard, backoff);
                    break;
                }

                if !(entry_ref.is_expired() || allow_eviction(entry_ref.key()))
                    && self.small_queue.push(index.into()).is_ok()
                {
                    return None;
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

                        self.push_into_ghost_queue(key.as_ref(), backoff);

                        unsafe { guard.defer_destroy(entry) };

                        return Some(index);
                    }
                    Err(latest) => {
                        tag = Tag::from(latest);
                        backoff.backoff();
                    }
                }
            }
        }

        None
    }

    #[inline(always)]
    fn push_into_ghost_queue(&self, key: &K, backoff: &mut Backoff) {
        let hash = hash(key);
        loop {
            match self.ghost_queue.push(hash) {
                Ok(_) => {
                    self.ghost_filter.increment(&hash);
                    break;
                }
                Err(_) => {
                    backoff.backoff();

                    if let Some(hash) = self.ghost_queue.pop() {
                        self.ghost_filter.decrement(&hash);
                    }
                }
            }
        }
    }

    /// Promotes an index into the `main_queue`, potentially triggering eviction.
    ///
    /// This method transitions an entry from the admission filter or small queue into
    /// long-term storage. Because the `main_queue` has a fixed capacity, this operation
    /// is blocking: if the queue is full, the calling thread is conscripted to perform
    /// an eviction to make space.
    ///
    /// # Invariants
    /// * The provided `index` must represent a valid, allocated slot.
    /// * Successful execution ensures the index is pushed to `main_queue`.
    fn promote_index(&self, index: Index, guard: &Guard, backoff: &mut Backoff) {
        loop {
            if self.main_queue.push(index.into()).is_ok() {
                break;
            }

            if let Some(index) = self.evict_from_main_queue(|_| true, guard, backoff) {
                self.index_pool
                    .push(index.into())
                    .expect("the index pool can't overflow");
            }
        }
    }

    /// Tries to add a new entry to the main queue, making space if necessary.
    ///
    /// This function handles the lifecycle of putting data into the cache. It first looks
    /// for an empty slot, and if none are available, it uses the admission policy to
    /// decide which old entry to kick out.
    ///
    /// **Algorithm**
    ///
    /// * **Finding a Slot**: It first tries to receive an index from the pool.
    ///   If the pool is empty, it triggers the eviction process. It passes the
    ///   `admission` policy to the evictor to decide if the new entry is "worthy"
    ///   enough to replace an old one. If the evictor can't free a slot (because
    ///   the policy protected everything), the function gives up and returns.
    ///
    /// * **Preparing the Entry**: Once it has a slot, it stores the new data,
    ///   calculates a hash signature for quick lookups, and updates the slot's metadata.
    ///
    /// * **Queueing**: The new entry is pushed onto the main queue. If the queue
    ///   is full (which can happen in high-traffic concurrent scenarios), it
    ///   manually triggers a "forced" eviction—ignoring the admission policy this
    ///   time—to ensure the new entry can actually fit.
    ///
    /// * **Table Registration**: Finally, it records the entry in the index table
    ///   so other threads can find this data using its key.
    fn push_into_main_queue(
        &self,
        entry: Entry<K, V>,
        admission: impl Fn(&K, &K) -> bool,
        guard: &Guard,
        backoff: &mut Backoff,
    ) {
        let index = match self.index_pool.pop() {
            Some(index) => Index::from(index),
            None => {
                match self.evict_from_main_queue(|key| admission(entry.key(), key), guard, backoff)
                {
                    Some(index) => index,
                    None => return,
                }
            }
        };

        let slot = &self.slots[index.slot_index()];

        let tag = Tag::from(slot.tag.load(Acquire));

        let entry = Owned::new(entry);
        let key = entry.key().clone();

        slot.entry.store(entry, Relaxed);

        let tag = tag.with_signature(hash(key.as_ref()));

        slot.tag.store(tag.into(), Release);

        loop {
            match self.main_queue.push(index.into()) {
                Ok(_) => break,
                Err(_) => {
                    if let Some(index) = self.evict_from_main_queue(|_| true, guard, backoff) {
                        let Ok(_) = self.index_pool.push(index.into()) else {
                            unreachable!("")
                        };
                    }
                }
            }
        }

        self.index_table.insert(key, index.into());
    }

    /// Tries to evict an entry from the main queue following the passed admission policy.
    ///
    /// This function walks the queue and decides whether to recycle a slot or keep its data:
    ///
    /// * **Second-Chance Rotation**: If an entry is "hot" and hasn't expired, we lower its frequency
    ///   and move it to the back of the queue so it stays in the cache longer.
    ///
    /// * **Admission Filter**: If an entry isn't hot, we check the admission policy.
    ///   If the policy says to keep it, we put it back in the queue and return `None`.
    ///
    /// * **Physical Eviction**: If the entry is expired or the policy allows it, we lock the slot,
    ///   remove the key from the table, and return the cleared index for immediate reuse.
    fn evict_from_main_queue(
        &self,
        allow_eviction: impl Fn(&K) -> bool,
        guard: &Guard,
        backoff: &mut Backoff,
    ) -> Option<Index> {
        while let Some(index) = self.main_queue.pop().map(Index::from) {
            let slot = &self.slots[index.slot_index()];
            let mut tag = Tag::from(slot.tag.load(Acquire));

            loop {
                if tag.is_busy() {
                    tag = Tag::from(slot.tag.load(Acquire));
                    backoff.backoff();
                    continue;
                }

                let entry = slot.entry.load(Relaxed, guard);

                let entry_ref =
                    unsafe { entry.as_ref().expect("the occupied entry cannot be null") };

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
                            if self.main_queue.push(index.into()).is_ok() {
                                break;
                            }
                            tag = updated_tag;
                        }
                        Err(latest) => {
                            tag = Tag::from(latest);
                            backoff.backoff();
                            continue;
                        }
                    }
                }

                // 2. Admission Phase
                // If the entry is not expired, we ask the admission policy if we can evict it.
                // If the policy says "don't evict" (returns false), we try to insert it back to the main queue.
                if !(entry_ref.is_expired() || allow_eviction(entry_ref.key()))
                    && self.main_queue.push(index.into()).is_ok()
                {
                    return None;
                }

                // Phase 3 Eviction:
                // Attempt to lock the entry for eviction; if locking fails, try to insert the index back into the queue.

                loop {
                    match slot.tag.compare_exchange_weak(
                        tag.into(),
                        tag.busy().into(),
                        AcqRel,
                        Relaxed,
                    ) {
                        Ok(_) => {
                            let entry = slot.entry.load(Relaxed, guard);
                            let entry_ref = unsafe { entry.as_ref() }.expect("");

                            self.index_table.remove(entry_ref.key());
                            slot.entry.store(Shared::null(), Relaxed);
                            unsafe { guard.defer_destroy(entry) };

                            let (next_tag, next_index) = tag.advance(index);

                            slot.tag.store(next_tag.into(), Release);

                            return Some(next_index);
                        }
                        Err(latest) => {
                            if self.main_queue.push(index.into()).is_ok() {
                                break;
                            }

                            tag = Tag::from(latest);
                            backoff.backoff();
                            continue;
                        }
                    }
                }
            }
        }

        None
    }

    fn remove<Q>(&self, key: &Q) -> bool
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let mut backoff = self.backoff_config.build();

        match self.index_table.remove(key).map(Index::from) {
            None => false,
            Some(index) => {
                let slot = &self.slots[index.slot_index()];

                let mut tag = Tag::from(slot.tag.load(Relaxed));

                loop {
                    if tag.is_epoch_match(index) {
                        break;
                    }

                    if let Err(latest) = slot.tag.compare_exchange_weak(
                        tag.into(),
                        tag.reset().into(),
                        Relaxed,
                        Relaxed,
                    ) {
                        tag = Tag::from(latest);
                        backoff.backoff();
                        continue;
                    }
                }

                true
            }
        }
    }
}

impl<K, V> CacheEngine<K, V> for S3FIFOCache<K, V>
where
    K: Eq + Hash,
{
    fn get<Q>(&self, key: &Q) -> Option<Ref<K, V>>
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.get(key)
    }

    fn insert_with<A>(&self, key: K, value: V, expired_at: Option<Instant>, admission: A)
    where
        A: Fn(&K, &K) -> bool,
    {
        self.insert_with(key, value, expired_at, admission)
    }

    fn remove<Q>(&self, key: &Q) -> bool
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.remove(key)
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn metrics(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam::scope;
    use rand::distr::{Alphanumeric, SampleString};
    use rand::{RngExt, rng};

    #[inline(always)]
    fn create_cache<K, V>(capacity: usize) -> S3FIFOCache<K, V>
    where
        K: Eq + Hash,
    {
        S3FIFOCache::new(
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
    fn test_s3cache_insert_should_retrieve_stored_value() {
        let cache = create_cache(10);

        let key = random_alphanumeric(32);
        let value = random_alphanumeric(255);

        cache.insert(key.clone(), value.clone(), None);

        let entry = cache.get(&key).expect("must present");

        assert_eq!(entry.key(), &key);
        assert_eq!(entry.value(), &value);
    }

    #[test]
    fn test_s3cache_insert_should_overwrite_existing_key() {
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
    fn test_s3cache_remove_should_invalidate_entry() {
        let cache = create_cache(100);

        let key = random_alphanumeric(32);

        cache.insert(key.clone(), random_alphanumeric(255), None);

        assert!(cache.get(&key).is_some());

        assert!(cache.remove(&key));

        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn test_s3cache_fill_beyond_capacity_should_evict_fifo() {
        let cache = create_cache(100);

        for _ in 0..1000 {
            let key = random_alphanumeric(32);
            let value = random_alphanumeric(255);

            cache.insert(key, value, None);
        }
    }

    #[test]
    fn test_s3cache_hot_entry_should_resist_eviction() {
        let cache = create_cache(1000);

        let key = random_alphanumeric(32);
        let value = random_alphanumeric(255);

        cache.insert(key.clone(), value.clone(), None);

        let entry = cache.get(&key).expect("must present");

        assert_eq!(entry.value(), &value);

        for _ in 0..250 {
            let key = random_alphanumeric(32);
            let value = random_alphanumeric(255);

            cache.insert(key, value, None);
        }

        let entry = cache.get(&key).expect("must present");

        assert_eq!(entry.key(), &key);
        assert_eq!(entry.value(), &value);
    }

    #[test]
    fn test_s3cache_reinserted_ghost_entry_should_be_promoted_to_main() {
        let cache = create_cache(1000);

        let (key, value) = (random_alphanumeric(32), random_alphanumeric(255));

        cache.insert(key.clone(), value.clone().to_string(), None);

        for _ in 0..200 {
            let key = random_alphanumeric(32);
            let value = random_alphanumeric(255);

            cache.insert(key, value, None);
        }

        assert!(cache.get(&key).is_none());

        cache.insert(key.clone(), value.clone().to_string(), None);

        for _ in 0..1000 {
            let key = random_alphanumeric(32);
            let value = random_alphanumeric(255);

            cache.insert(key, value, None);
        }

        let entry = cache.get(&key).expect("must present");

        assert_eq!(entry.key(), &key);
        assert_eq!(entry.value(), &value);
    }

    #[test]
    fn test_s3cache_ghost_filter_should_protect_working_set() {
        let cache = create_cache(1000);
        let hot_entries = vec![("a", "a"), ("b", "b"), ("c", "c"), ("d", "d"), ("e", "e")];

        for &(key, value) in &hot_entries {
            cache.insert(key.to_string(), value.to_string(), None);
        }

        for i in 0..100000 {
            if i % 2 == 0 {
                cache.insert(format!("key-{}", i), format!("value-{}", i), None);
            } else {
                let index = rng().random_range(..hot_entries.len());
                let key = hot_entries[index].0;
                let _ = cache.get(key);
            }
        }

        let count = hot_entries
            .iter()
            .map(|&(key, _)| cache.get(key))
            .filter(|it| it.is_some())
            .count();

        assert!(count >= 4);
    }

    #[test]
    fn test_s3cache_concurrent_hammer_should_not_crash_or_hang() {
        let cache = create_cache(1000);
        let num_threads = 8;
        let ops_per_thread = 10000;

        std::thread::scope(|s| {
            for _ in 0..num_threads {
                s.spawn(|| {
                    for i in 0..ops_per_thread {
                        let key = (i % 500).to_string();
                        if i % 2 == 0 {
                            cache.insert(key, random_alphanumeric(255), None);
                        } else {
                            let _ = cache.get(&key);
                        }
                    }
                });
            }
        });
    }

    #[test]
    fn test_s3cache_concurrent_should_protect_frequent_entries() {
        let frequent_entries_len = 50;
        let mut frequent_entries = Vec::with_capacity(frequent_entries_len);

        for _ in 0..frequent_entries_len {
            let key = random_alphanumeric(32);
            frequent_entries.push((key, random_alphanumeric(255)));
        }

        let num_threads = 8;
        let ops_per_thread = 10000;

        let cache = create_cache(1000);

        for (key, value) in &frequent_entries {
            cache.insert(key.clone(), value.clone(), None);
            let _ = cache.get(key);
        }

        for _ in 0..2000 {
            cache.insert(random_alphanumeric(32), random_alphanumeric(255), None);
        }

        for (key, _) in &frequent_entries {
            for _ in 0..5 {
                let _ = cache.get(key);
            }
        }

        let _ = scope(|scope| {
            for _ in 0..num_threads {
                scope.spawn(|_| {
                    for op in 0..ops_per_thread {
                        if op % 5 == 0 {
                            let index = rng().random_range(0..frequent_entries_len);
                            let (key, _) = &frequent_entries[index];
                            let _ = cache.get(key);
                        } else {
                            cache.insert(random_alphanumeric(32), random_alphanumeric(255), None);
                        }
                    }
                });
            }
        });

        let mut count = 0;

        for (key, value) in &frequent_entries {
            if let Some(entry_ref) = cache.get(key) {
                assert_eq!(entry_ref.value(), value);
                count += 1;
            }
        }

        assert!(count >= 10);
    }
}
