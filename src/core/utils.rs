use rand::distr::{Alphanumeric, SampleString};
use rand::{RngExt, rng};
use std::hash::{Hash, Hasher};
use twox_hash::xxhash64::Hasher as XxHash64;

#[inline(always)]
pub fn hash<T: Eq + Hash>(value: T) -> u64 {
    let mut hasher = XxHash64::default();
    value.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
#[inline(always)]
pub fn random_string_with_len(len: usize) -> String {
    Alphanumeric.sample_string(&mut rand::rng(), len)
}

#[cfg(test)]
#[inline(always)]
pub fn random_string() -> String {
    let len = rng().random_range(10..255);
    random_string_with_len(len)
}
