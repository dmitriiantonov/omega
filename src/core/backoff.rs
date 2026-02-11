use std::hint::spin_loop;

/// A stateful backoff strategy used to reduce CPU contention in busy-wait loops.
///
/// This enum encapsulates both the configuration and the current state of the backoff.
/// It uses "Full Jitter" to prevent "thundering herd" problems by picking a random
/// number of spins between `0` and the current limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backoff {
    /// Linear growth: increases the maximum spin count by 1 each step until it hits the limit.
    Linear {
        /// The current maximum number of spins for the next jitter calculation.
        current: usize,
        /// The maximum allowed value for `current`.
        limit: usize
    },
    /// Exponential growth: doubles the maximum spin count each step until it hits the limit.
    Exponential {
        /// The current maximum number of spins for the next jitter calculation.
        current: usize,
        /// The maximum allowed value for `current`.
        limit: usize
    },
}

impl Backoff {
    /// Creates a new linear backoff starting with a maximum spin of 1.
    ///
    /// At each call to [`backoff`], the internal limit will increase by 1 until `limit` is reached.
    #[inline]
    pub fn linear(limit: usize) -> Self {
        Self::Linear { current: 1, limit }
    }

    /// Creates a new exponential backoff starting with a maximum spin of 1.
    ///
    /// At each call to [`backoff`], the internal limit will double (multiplicative growth)
    /// until `limit` is reached.
    #[inline]
    pub fn exponential(limit: usize) -> Self {
        Self::Exponential { current: 1, limit }
    }

    /// Returns the current upper bound for the spin jitter.
    #[inline]
    fn current_limit(&self) -> usize {
        match *self {
            Self::Linear { current, .. } => current,
            Self::Exponential { current, .. } => current,
        }
    }

    /// Updates the internal state for the next backoff attempt.
    #[inline]
    fn step(&mut self) {
        match self {
            Self::Linear { current, limit } => {
                *current = current.saturating_add(1).min(*limit);
            }
            Self::Exponential { current, limit } => {
                *current = current.saturating_mul(2).min(*limit);
            }
        }
    }

    /// Performs the backoff by spinning the CPU.
    ///
    /// This method:
    /// 1. Generates a random number jitter from 0 to ${limit}.
    /// 2. Executes `std::hint::spin_loop()` jitter times.
    /// 3. Increments the internal limit according to the chosen strategy.
    pub fn backoff(&mut self) {
        let limit = self.current_limit();

        // Full Jitter: randomize within the [0, limit] range to desynchronize threads.
        let jitter = fastrand::usize(..=limit);

        for _ in 0..jitter {
            spin_loop();
        }

        self.step();
    }
}

/// Configuration for creating [`Backoff`] instances.
///
/// This struct acts as a factory, allowing you to store a preferred backoff
/// strategy (e.g., in a cache or connection pool) and generate fresh,
/// thread-local [`Backoff`] states as needed.
#[derive(Debug, Clone, Copy)]
pub struct BackoffConfig {
    /// The mathematical strategy used to increase delay.
    pub policy: BackoffPolicy,
    /// The maximum number of spins allowed in a single backoff iteration.
    pub limit: usize,
}

/// Defines how the backoff duration scales after each failed attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackoffPolicy {
    /// Increases the maximum spin count linearly (e.g., 1, 2, 3, 4...).
    Linear,
    /// Increases the maximum spin count exponentially (e.g., 1, 2, 4, 8...).
    /// This is generally preferred for high-contention scenarios.
    Exponential,
}

impl BackoffConfig {
    /// Creates a fresh, stateful [`Backoff`] instance based on this configuration.
    ///
    /// This is typically called at the start of a retry loop. Because the
    /// returned `Backoff` is owned and stateful, it should be kept local
    /// to the thread performing the retries.
    #[must_use]
    pub fn build(&self) -> Backoff {
        match self.policy {
            BackoffPolicy::Linear => Backoff::linear(self.limit),
            BackoffPolicy::Exponential => Backoff::exponential(self.limit),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linear_growth() {
        let mut backoff = Backoff::linear(10);

        // Initial limit is 1
        assert_eq!(backoff.current_limit(), 1);

        // After one backoff, limit should be 2
        backoff.backoff();
        assert_eq!(backoff.current_limit(), 2);

        // After another, limit should be 3
        backoff.backoff();
        assert_eq!(backoff.current_limit(), 3);
    }

    #[test]
    fn test_exponential_growth() {
        let mut backoff = Backoff::exponential(100);

        // Initial limit is 1
        assert_eq!(backoff.current_limit(), 1);

        backoff.backoff(); // 1 -> 2
        assert_eq!(backoff.current_limit(), 2);

        backoff.backoff(); // 2 -> 4
        assert_eq!(backoff.current_limit(), 4);

        backoff.backoff(); // 4 -> 8
        assert_eq!(backoff.current_limit(), 8);
    }

    #[test]
    fn test_cap_limit() {
        // Test that linear caps
        let mut lin = Backoff::linear(2);
        lin.backoff(); // 1 -> 2
        lin.backoff(); // 2 -> 2 (capped)
        assert_eq!(lin.current_limit(), 2);

        // Test that exponential caps
        let mut exp = Backoff::exponential(10);
        exp.backoff(); // 1 -> 2
        exp.backoff(); // 2 -> 4
        exp.backoff(); // 4 -> 8
        exp.backoff(); // 8 -> 10 (capped)
        assert_eq!(exp.current_limit(), 10);
    }

    #[test]
    fn test_saturating_multiplication() {
        // Ensure that even with a massive cap, we don't panic on overflow
        let mut exp = Backoff::exponential(usize::MAX);
        // Set current to something that would overflow if doubled
        if let Backoff::Exponential { current, .. } = &mut exp {
            *current = usize::MAX / 2 + 1;
        }

        exp.backoff();
        assert_eq!(exp.current_limit(), usize::MAX);
    }

    #[test]
    fn test_backoff_does_not_panic() {
        let mut backoff = Backoff::exponential(10);
        // Simple smoke test to ensure no internal panics during execution
        for _ in 0..10 {
            backoff.backoff();
        }
    }
}