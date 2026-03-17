/// `RequestQuota` provides a short-lived, ephemeral budget for a single cache operation.
///
/// While [`ThreadContext`] manages the hardware-level timing (how long to wait),
/// `RequestQuota` manages the logical retries (how many times to try). It acts as
/// a safety valve to prevent "Livelock"—a scenario where a thread enters an
/// infinite retry loop because a high-contention slot never becomes available.
///
/// ### Characteristics
/// * **Stack-Allocated**: Designed to be created on the stack at the start of an operation.
/// * **Single-Use**: Typically discarded once the `push` or `pop` operation completes.
/// * **Deterministic**: Ensures that an operation will eventually return (either with
///   success or a "Busy" error) within a bounded number of attempts.
pub struct RequestQuota {
    remaining: usize,
}

impl RequestQuota {
    /// Creates a new quota with the specified number of allowed retries.
    ///
    /// # Parameters
    /// * `remaining`: The maximum number of failed atomic attempts to tolerate
    ///   before aborting the request.
    #[inline(always)]
    pub fn new(remaining: usize) -> Self {
        Self { remaining }
    }

    /// Attempts to consume one unit of the retry budget.
    ///
    /// This should be called every time a contention-sensitive operation (like
    /// a Compare-And-Swap) fails, but *before* performing a backoff yield.
    ///
    /// # Returns
    /// * `true`: Budget is available; the operation should retry.
    /// * `false`: Budget is exhausted; the operation should abort to prevent
    ///   stalling the system.
    #[inline(always)]
    pub fn consume(&mut self) -> bool {
        if self.remaining == 0 {
            return false;
        }

        self.remaining -= 1;
        true
    }

    #[inline(always)]
    pub fn has_attempts(&self) -> bool {
        self.remaining > 0
    }
}

impl Default for RequestQuota {
    fn default() -> Self {
        const DEFAULT_ATTEMPTS: usize = 64;
        Self {
            remaining: DEFAULT_ATTEMPTS,
        }
    }
}
