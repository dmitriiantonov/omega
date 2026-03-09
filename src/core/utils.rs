use rand::distr::{Alphanumeric, SampleString};
use std::hash::{Hash, Hasher};
use twox_hash::xxhash64::Hasher as XxHash64;

#[inline(always)]
pub fn hash<T: Eq + Hash>(value: T) -> u64 {
    let mut hasher = XxHash64::default();
    value.hash(&mut hasher);
    hasher.finish()
}

#[inline(always)]
pub fn random_string(len: usize) -> String {
    Alphanumeric.sample_string(&mut rand::rng(), len)
}
