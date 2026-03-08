use std::hash::{Hash, Hasher};
use twox_hash::xxhash64::Hasher as XxHash64;

pub fn hash<T: Eq + Hash>(value: T) -> u64 {
    let mut hasher = XxHash64::default();
    value.hash(&mut hasher);
    hasher.finish()
}
