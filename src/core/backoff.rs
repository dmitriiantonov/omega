use std::hint::spin_loop;

pub struct Backoff {
    spins: usize,
    attempts: usize,
    max_spins: usize,
    fast_retries: usize
}

impl Backoff {

    #[inline]
    pub fn new(max_spins: usize, fast_retries: usize) -> Self {
        Self {
            spins: 1,
            attempts: 0,
            max_spins,
            fast_retries,
        }
    }

    #[inline]
    pub fn snooze(&mut self) {
        self.attempts += 1;

        let jitter = fastrand::usize(..=self.spins);

        for _ in 0..jitter {
            spin_loop()
        }

        if self.attempts > self.fast_retries {
            self.spins = (self.spins * 2).min(self.max_spins);
        }
    }
}