use crate::core::thread_context::ThreadContext;
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
    /// The heap-allocated array of atomic nodes.
    buffer: Box<[RingSlot]>,
    /// Total number of slots. Must be a power of two for mask-based wrapping.
    capacity: usize,
    /// Bitmask (capacity - 1) used for fast modulo mapping of cursors to buffer indices.
    mask: usize,
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
    pub fn new(capacity: usize) -> Self {
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
            buffer,
            capacity,
            mask: capacity - 1,
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
    pub fn push(&self, value: u64, context: &ThreadContext) -> Result<(), u64> {
        let mut tail = self.tail.load(Relaxed);

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
                            item.sequence.store(tail + 1, Release);
                            context.decay();
                            return Ok(());
                        }
                        Err(current_tail) => {
                            tail = current_tail;
                            context.wait();
                        }
                    }
                }
                diff if diff < 0 => return Err(value),
                _ => {
                    tail = self.tail.load(Relaxed);
                    context.wait();
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
    pub fn pop(&self, context: &ThreadContext) -> Option<u64> {
        let mut head = self.head.load(Relaxed);

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
                            item.sequence.store(next_sequence, Release);
                            context.decay();
                            return Some(value);
                        }
                        Err(current_head) => {
                            head = current_head;
                            context.wait();
                        }
                    }
                }
                diff if diff < 0 => return None,
                _ => {
                    head = self.head.load(Relaxed);
                    context.wait();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::thread_context::ThreadContext;
    use rand::random;
    use std::sync::atomic::AtomicUsize;
    use std::thread;
    use thread::scope;

    #[test]
    fn test_ring_queue_should_fill_and_empty_linearly() {
        let context = ThreadContext::default();
        let queue = RingQueue::new(8);

        for i in 0..8 {
            assert!(queue.push(i as u64, &context).is_ok());
        }

        assert!(queue.push(99, &context).is_err());

        for i in 0..8 {
            assert_eq!(queue.pop(&context), Some(i as u64));
        }

        assert_eq!(queue.pop(&context), None);
    }

    #[test]
    fn test_ring_queue_should_maintain_consistency() {
        let queue = RingQueue::new(2);
        let context = ThreadContext::default();

        for _ in 0..queue.capacity {
            let value: u64 = random();
            assert!(queue.push(value, &context).is_ok());
        }

        assert!(queue.push(random(), &context).is_err());

        assert!(queue.pop(&context).is_some());

        assert!(queue.push(random(), &context).is_ok());
    }

    #[test]
    fn test_ring_queue_should_handle_concurrent_producers_and_consumers() {
        let num_threads: usize = 16;
        let op_per_threads: usize = 10000;
        let queue = RingQueue::new(512);
        let total_sum = AtomicUsize::new(0);

        scope(|s| {
            for _ in 0..num_threads {
                s.spawn(|| {
                    let context = ThreadContext::default();
                    for op in 0..op_per_threads {
                        while queue.push(op as u64 + 1, &context).is_err() {
                            context.wait();
                        }

                        context.decay();
                    }
                });
            }

            for _ in 0..num_threads {
                s.spawn(|| {
                    let context = ThreadContext::default();
                    for _ in 0..op_per_threads {
                        loop {
                            if let Some(val) = queue.pop(&context) {
                                total_sum.fetch_add(val as usize, Relaxed);
                                break;
                            }
                            context.wait();
                        }
                    }
                });
            }
        });

        let expected_sum = op_per_threads * (op_per_threads + 1) / 2 * num_threads;
        assert_eq!(total_sum.load(Relaxed), expected_sum);
    }

    #[test]
    fn test_ring_queue_should_survive_multiple_buffer_laps() {
        let capacity = 16;
        let queue = RingQueue::new(capacity);
        let ctx = ThreadContext::default();

        for _ in 0..100 {
            for i in 0..capacity {
                assert!(queue.push(i as u64, &ctx).is_ok())
            }
            for i in 0..capacity {
                assert_eq!(queue.pop(&ctx), Some(i as u64));
            }
        }
    }

    #[test]
    fn test_ring_queue_should_not_deadlock_under_high_contention() {
        let num_threads: usize = 16;
        let queue = RingQueue::new(10);
        let items_to_send = 1024;
        let received_count = AtomicUsize::new(0);

        scope(|scope| {
            for _ in 0..num_threads {
                scope.spawn(|| {
                    let context = ThreadContext::default();
                    for _ in 0..(items_to_send / 16) {
                        while queue.push(1, &context).is_err() {
                            context.wait();
                        }
                    }
                });

                scope.spawn(|| {
                    let ctx = ThreadContext::default();
                    for _ in 0..(items_to_send / 16) {
                        loop {
                            if queue.pop(&ctx).is_some() {
                                received_count.fetch_add(1, Relaxed);
                                break;
                            }
                            ctx.wait();
                        }
                    }
                });
            }
        });

        assert_eq!(received_count.load(Acquire), items_to_send);
    }
}
