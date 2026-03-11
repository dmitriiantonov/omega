use crate::core::backoff::BackoffConfig;
use crossbeam::utils::CachePadded;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicU64, AtomicUsize};

const MIN_CAPACITY: usize = 8;

/// A bounded Multi-Producer Multi-Consumer (MPMC) queue utilizing a fixed-size ring buffer.
///
/// The implementation employs a sequence-based synchronization gate to coordinate access between threads, ensuring that producers and consumers only operate on slots that have been released by the previous "lap" of the buffer.
///
/// Memory and Hardware Considerations
/// Cache Isolation: head, tail, and len cursors are isolated using CachePadded to mitigate false sharing and L1 cache-line contention between concurrent threads.
///
/// Memory Layout: Uses #[repr(C)] for a predictable field order, aiding hardware pre-fetchers.
///
/// Portability: Uses 64-bit atomics for cross-platform compatibility, including ARMv8 architectures where 128-bit atomic operations may not be natively supported.
#[repr(C)]
pub struct RingQueue {
    /// Producer cursor. Only incremented on successful claim of a slot for writing.
    head: CachePadded<AtomicUsize>,
    /// Consumer cursor. Only incremented on successful claim of a slot for reading.
    tail: CachePadded<AtomicUsize>,
    /// Current count of items in the queue.
    len: CachePadded<AtomicUsize>,
    /// The heap-allocated array of atomic nodes.
    buffer: Box<[RingSlot]>,
    /// Total number of slots. Must be a power of two for mask-based wrapping.
    capacity: usize,
    /// Bitmask (capacity - 1) used for fast modulo mapping of cursors to buffer indices.
    mask: usize,
    /// Configuration for the thread backoff strategy.
    backoff_config: BackoffConfig,
}

/// A single atomic slot within the `RingBuffer`.
#[derive(Debug, Default)]
struct RingSlot {
    value: AtomicU64,
    sequence: AtomicUsize,
}

impl RingQueue {
    /// Creates a new queue with a capacity rounded up to the nearest power of two.
    ///
    /// Each slot is initialized with a sequence number that permits
    /// the first lap of production.
    #[inline]
    pub fn new(capacity: usize, backoff_config: BackoffConfig) -> Self {
        let capacity = capacity.max(MIN_CAPACITY).next_power_of_two();

        let buffer = (0..capacity)
            .map(|index| RingSlot {
                value: AtomicU64::default(),
                sequence: AtomicUsize::new(index),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(0)),
            len: CachePadded::new(AtomicUsize::new(0)),
            buffer,
            capacity,
            mask: capacity - 1,
            backoff_config,
        }
    }

    /// Attempts to push a value into the buffer.
    ///
    /// Returns `Ok(())` if successful, or `Err(u64)` containing the value if the buffer is full.
    ///
    /// # Synchronization
    /// 1. Loads the slot sequence with `Acquire` ordering to observe the last consumer's release.
    /// 2. Claims a slot via `compare_exchange` on the `tail` cursor.
    /// 3. Updates the slot value and publishes it by incrementing the sequence with `Release` ordering.
    pub fn push(&self, value: u64) -> Result<(), u64> {
        let mut tail = self.tail.load(Relaxed);
        let mut backoff = self.backoff_config.build();

        loop {
            let item = &self.buffer[tail & self.mask];
            let sequence = item.sequence.load(Acquire);
            let diff = sequence as isize - tail as isize;

            match diff {
                0 => {
                    match self
                        .tail
                        .compare_exchange_weak(tail, tail + 1, Relaxed, Acquire)
                    {
                        Ok(_) => {
                            item.value.store(value, Relaxed);
                            self.len.fetch_add(1, Relaxed);
                            item.sequence.store(tail + 1, Release);
                            return Ok(());
                        }
                        Err(current_tail) => {
                            tail = current_tail;
                            backoff.backoff();
                        }
                    }
                }
                diff if diff < 0 => return Err(value),
                _ => {
                    tail = self.tail.load(Relaxed);
                    backoff.backoff();
                }
            }
        }
    }

    /// Attempts to pop a value from the buffer.
    ///
    /// Returns `Some(u64)` if data is available, or `None` if the buffer is empty.
    ///
    /// # Synchronization
    /// 1. Validates that the sequence number matches the expected `head` lap.
    /// 2. Claims the index via `compare_exchange` on the `head` cursor.
    /// 3. Loads the value and resets the slot sequence to signal readiness
    ///    to future producers (lap + capacity).
    pub fn pop(&self) -> Option<u64> {
        let mut head = self.head.load(Relaxed);
        let mut backoff = self.backoff_config.build();

        loop {
            let item = &self.buffer[head & self.mask];
            let sequence = item.sequence.load(Acquire);
            let diff = sequence as isize - head as isize - 1;

            match diff {
                0 => {
                    match self
                        .head
                        .compare_exchange_weak(head, head + 1, Relaxed, Acquire)
                    {
                        Ok(_) => {
                            let value = item.value.load(Relaxed);
                            let next_sequence = head.wrapping_add(self.capacity);
                            self.len.fetch_sub(1, Relaxed);
                            item.sequence.store(next_sequence, Release);
                            return Some(value);
                        }
                        Err(current_head) => {
                            head = current_head;
                            backoff.backoff();
                        }
                    }
                }
                diff if diff < 0 => return None,
                _ => {
                    head = self.head.load(Relaxed);
                    backoff.backoff();
                }
            }
        }
    }

    /// Returns the number of items currently in the queue.
    #[inline]
    pub fn len(&self) -> usize {
        self.len.load(Acquire)
    }

    /// Returns true if the queue contains no items.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns true if the queue has reached its maximum capacity.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.len() == self.capacity
    }
}

#[cfg(test)]
mod tests {
    use crate::core::backoff::BackoffConfig;
    use crate::core::ring::RingQueue;
    use crossbeam::scope;
    use rand::{RngExt, random, rng};
    use std::collections::HashSet;
    use std::hint::spin_loop;
    use std::sync::{Arc, Barrier};

    #[test]
    fn test_ring_queue_basic() {
        let queue = RingQueue::new(16, BackoffConfig::linear(10));

        for num in 1..=16 {
            assert!(queue.push(num).is_ok());
        }

        assert!(queue.is_full());

        for num in 1..=16 {
            assert_eq!(queue.pop(), Some(num as u64));
        }

        assert!(queue.is_empty());
    }

    #[test]
    fn test_ring_queue_concurrent() {
        let num_threads = 32;
        let queue = Arc::new(RingQueue::new(16, BackoffConfig::linear(4096)));

        let (producer, consumer) = std::sync::mpsc::channel();
        let producer = Arc::new(producer);

        let mut written_values = HashSet::with_capacity(16384);

        let _ = scope(|scope| {
            for _ in 0..num_threads {
                scope.spawn({
                    let producer = producer.clone();
                    let queue = queue.clone();

                    move |_| {
                        loop {
                            let value = rng().random();

                            match queue.push(value) {
                                Ok(_) => {
                                    let _ = producer.send(value);
                                }
                                Err(_) => break,
                            }
                        }
                    }
                });
            }
        });

        assert!(queue.is_full());

        drop(producer);

        while let Ok(value) = consumer.recv() {
            written_values.insert(value);
        }

        while let Some(value) = queue.pop() {
            assert!(written_values.contains(&value));
        }

        assert!(queue.is_empty());
    }

    #[test]
    fn test_ring_consumer_producer() {
        let queue = RingQueue::new(16, BackoffConfig::linear(4096));
        let op_num = 10000;
        let num_threads = 4;

        let _ = scope(|scope| {
            for _ in 0..num_threads {
                scope.spawn(|_| {
                    for op in 0..(op_num / num_threads) {
                        let value = op;

                        while queue.push(value).is_err() {
                            spin_loop()
                        }
                    }
                });
            }

            for _ in 0..num_threads {
                scope.spawn(|_| {
                    for _ in 0..(op_num / num_threads) {
                        while queue.pop().is_none() {
                            spin_loop()
                        }
                    }
                });
            }
        });

        assert!(queue.is_empty());
    }

    #[test]
    fn test_ring_queue_high_contention() {
        let op_num = 16;
        let queue = RingQueue::new(16, BackoffConfig::linear(4096));
        let barrier = Barrier::new(16);

        let _ = scope(|scope| {
            for _ in 0..op_num {
                scope.spawn(|_| {
                    let num = random();
                    barrier.wait();

                    let _ = queue.push(num);
                });
            }
        });

        assert_eq!(op_num, queue.len());
    }
}
