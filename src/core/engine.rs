use crate::core::handler::Handler;
use crate::core::key::Key;
use std::borrow::Borrow;
use std::hash::Hash;

/// Trait representing a generic cache engine.
///
/// This trait defines the core operations for a key-value cache, including
/// insertion, retrieval, removal, and eviction of entries. Implementors of this
/// trait can define different caching policies such as LRU, LFU, or Clock.
///
/// # Type Parameters
///
/// - `K`: The type of the key. Must implement `Eq` and `Hash`.
/// - `V`: The type of the value stored in the cache.
pub trait CacheEngine<K, V>
where
    K: Eq + Hash,
{
    /// Retrieves a cached value corresponding to the given key.
    ///
    /// Returns an `Option<Handler<K, V>>` containing the cached value if present,
    /// or `None` if the key is not found in the cache.
    ///
    /// # Type Parameters
    ///
    /// - `Q`: A type that can be borrowed from `K` for lookup purposes.
    ///
    /// # Examples
    ///
    /// ```
    /// let cache: MyCache<String, i32> = MyCache::new(10);
    /// cache.insert("key1".to_string(), 42);
    /// assert!(cache.get("key1").is_some());
    /// ```
    fn get<Q>(&self, key: &Q) -> Option<Handler<K, V>>
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized;

    /// Inserts a key-value pair into the cache.
    ///
    /// If the key already exists, the value may be updated according to the cache
    /// implementation. This may trigger eviction if the cache is at capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// cache.insert("key2".to_string(), 100);
    /// ```
    fn insert(&self, key: K, value: V) -> Option<Handler<K, V>>;

    fn insert_with<F>(&self, key: K, value: V, admission: F) -> Option<Handler<K, V>>
    where
        F: Fn(&K, &K) -> bool;

    /// Removes the cached entry for the given key.
    ///
    /// Returns an `Option<Handler<K, V>>` containing the removed value if it was
    /// present, or `None` otherwise.
    ///
    /// # Type Parameters
    ///
    /// - `Q`: A type that can be borrowed from `K` for lookup purposes.
    ///
    /// # Examples
    ///
    /// ```
    /// cache.remove("key1");
    /// ```
    fn remove<Q>(&self, key: &Q) -> Option<Handler<K, V>>
    where
        Key<K>: Borrow<Q>,
        Q: Eq + Hash + ?Sized;

    /// Returns the total capacity of the cache.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(cache.capacity(), 100);
    /// ```
    fn capacity(&self) -> usize;

    /// Returns the current number of entries in the cache.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(cache.len(), 5);
    /// ```
    fn len(&self) -> usize;

    /// Returns `true` if the cache has space to insert new entries without eviction.
    ///
    /// # Examples
    ///
    /// ```
    /// if cache.has_space() {
    ///     cache.insert("key3".to_string(), 7);
    /// }
    /// ```
    fn has_space(&self) -> bool;

    /// Peeks at the next entry that would be evicted according to the cache policy,
    /// without actually removing it.
    ///
    /// Returns `None` if the cache is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// if let Some(evicted) = cache.peek_evicted() {
    ///     println!("Next to evict: {:?}", evicted);
    /// }
    /// ```
    fn peek_victim(&self) -> Option<Handler<K, V>>;
}
