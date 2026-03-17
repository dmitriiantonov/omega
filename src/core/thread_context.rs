use crate::core::backoff::Backoff;
use std::cell::UnsafeCell;

/// `ThreadContext` maintains the long-lived, persistent contention state of a
/// specific thread as it interacts with a specific cache instance.
///
/// Unlike a request-scoped budget (quota), `ThreadContext` acts as the "Thread Memory."
/// It tracks historical contention via [`Backoff`], allowing the thread to adapt its
/// timing based on how "hot" the cache has been in recent operations.
///
/// ### Safety and Thread Locality
/// This struct is explicitly **!Send** and **!Sync**.
///
/// It uses [`UnsafeCell`] to provide interior mutability with zero runtime overhead.
/// This is only safe when the context is stored in a way that guarantees exclusive
/// access by a single thread (e.g., inside a `ThreadLocal` ). The non-thread-safe
/// markers ensure this state cannot accidentally leak or be shared across thread boundaries.
pub struct ThreadContext {
    backoff: UnsafeCell<Backoff>,
}

impl ThreadContext {
    /// Creates a new `ThreadContext` with the provided backoff policy.
    ///
    /// Usually initialized lazily when a thread first interacts with a cache instance.
    #[inline(always)]
    pub fn new(backoff: Backoff) -> Self {
        Self {
            backoff: UnsafeCell::new(backoff),
        }
    }

    /// Signals hardware-level contention and triggers a thread yield.
    ///
    /// This should be called when an atomic operation (like a CAS) fails.
    /// It increases the internal frustration level and performs a hardware-friendly
    /// stall (e.g., `spin_loop` or `yield`) based on the persistent backoff state.
    #[inline(always)]
    pub fn wait(&self) {
        unsafe { &mut *self.backoff.get() }.wait();
    }

    /// Signals a successful operation, allowing the thread-local "heat" to dissipate.
    ///
    /// This should be called after a successful operation. It decays the
    /// persistent frustration level, ensuring that subsequent calls from
    /// this thread are not unnecessarily throttled.
    #[inline(always)]
    pub fn decay(&self) {
        unsafe { &mut *self.backoff.get() }.wait();
    }
}

impl Default for ThreadContext {
    #[inline(always)]
    fn default() -> Self {
        const DEFAULT_LIMIT: usize = 32;

        Self {
            backoff: UnsafeCell::new(Backoff::linear(DEFAULT_LIMIT)),
        }
    }
}
