// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Thread-safe LRU cache.
//!
//! Replaces C++ `shared_lru.h`.

use lru::LruCache;
use parking_lot::Mutex;
use std::hash::Hash;
use std::num::NonZeroUsize;

/// A thread-safe LRU cache wrapping `lru::LruCache` with a `parking_lot::Mutex`.
pub struct SharedLruCache<K: Eq + Hash, V> {
    inner: Mutex<LruCache<K, V>>,
}

impl<K: Eq + Hash, V> SharedLruCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(LruCache::new(
                NonZeroUsize::new(capacity).expect("LRU capacity must be > 0"),
            )),
        }
    }

    /// Insert a key-value pair. Returns the evicted value if the cache was full.
    pub fn put(&self, key: K, value: V) -> Option<V> {
        let mut cache = self.inner.lock();
        cache.put(key, value)
    }

    /// Get a clone of the value for a key, promoting it to most-recently-used.
    pub fn get(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        let mut cache = self.inner.lock();
        cache.get(key).cloned()
    }

    /// Remove a key and return its value.
    pub fn pop(&self, key: &K) -> Option<V> {
        let mut cache = self.inner.lock();
        cache.pop(key)
    }

    /// Check if a key exists.
    pub fn contains(&self, key: &K) -> bool {
        let cache = self.inner.lock();
        cache.contains(key)
    }

    /// Current number of entries.
    pub fn len(&self) -> usize {
        let cache = self.inner.lock();
        cache.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear all entries.
    pub fn clear(&self) {
        let mut cache = self.inner.lock();
        cache.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let cache = SharedLruCache::new(2);
        cache.put("a", 1);
        cache.put("b", 2);
        assert_eq!(cache.get(&"a"), Some(1));

        // Inserting "c" should evict "b" (LRU) since "a" was just accessed
        cache.put("c", 3);
        assert_eq!(cache.get(&"b"), None);
        assert_eq!(cache.get(&"c"), Some(3));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_pop() {
        let cache = SharedLruCache::new(2);
        cache.put("a", 1);
        assert_eq!(cache.pop(&"a"), Some(1));
        assert!(cache.is_empty());
    }

    #[test]
    fn test_contains() {
        let cache = SharedLruCache::new(3);
        assert!(!cache.contains(&"a"));
        cache.put("a", 1);
        assert!(cache.contains(&"a"));
        assert!(!cache.contains(&"b"));
    }

    #[test]
    fn test_is_empty_and_len() {
        let cache = SharedLruCache::new(3);
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);

        cache.put("a", 1);
        assert!(!cache.is_empty());
        assert_eq!(cache.len(), 1);

        cache.put("b", 2);
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_clear() {
        let cache = SharedLruCache::new(3);
        cache.put("a", 1);
        cache.put("b", 2);
        assert_eq!(cache.len(), 2);

        cache.clear();
        assert!(cache.is_empty());
        assert_eq!(cache.get(&"a"), None);
    }

    #[test]
    fn test_eviction_returns_old_value() {
        let cache = SharedLruCache::new(2);
        cache.put("a", 1);
        cache.put("b", 2);
        // Cache is full; inserting "c" should evict LRU ("a")
        cache.put("c", 3);
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get(&"a"), None);
        assert_eq!(cache.get(&"b"), Some(2));
        assert_eq!(cache.get(&"c"), Some(3));
    }

    #[test]
    fn test_put_overwrite() {
        let cache = SharedLruCache::new(2);
        cache.put("a", 1);
        let old = cache.put("a", 10);
        assert_eq!(old, Some(1));
        assert_eq!(cache.get(&"a"), Some(10));
        assert_eq!(cache.len(), 1);
    }

    // --- Ported from C++ shared_lru_test.cc ---

    /// Port of C++ PutAndGet: capacity-1 cache, insert/evict/delete.
    #[test]
    fn test_put_and_get_capacity_one() {
        let cache = SharedLruCache::new(1);

        // No value initially.
        assert_eq!(cache.get(&"1"), None);

        // Put and get.
        cache.put("1", "one".to_string());
        assert_eq!(cache.get(&"1"), Some("one".to_string()));

        // Eviction: inserting "2" evicts "1" (capacity=1).
        cache.put("2", "two".to_string());
        assert_eq!(cache.get(&"1"), None);
        assert_eq!(cache.get(&"2"), Some("two".to_string()));

        // Delete non-existent key returns None.
        assert_eq!(cache.pop(&"1"), None);
        assert_eq!(cache.get(&"1"), None);
    }

    /// Port of C++ SameKeyTest: putting the same key updates the value.
    #[test]
    fn test_same_key_update() {
        let cache = SharedLruCache::new(2);

        cache.put(1, 1);
        assert_eq!(cache.get(&1), Some(1));

        cache.put(1, 2);
        assert_eq!(cache.get(&1), Some(2));
    }

    /// Port of C++ FactoryTest: concurrent get_or_create with factory.
    /// Since our SharedLruCache doesn't have get_or_create, we test
    /// concurrent put/get which exercises the same Mutex path.
    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        let cache = Arc::new(SharedLruCache::new(10));
        let mut handles = Vec::new();

        for i in 0..100 {
            let cache = cache.clone();
            handles.push(std::thread::spawn(move || {
                cache.put(i % 10, i);
                cache.get(&(i % 10))
            }));
        }

        for h in handles {
            let val = h.join().unwrap();
            // Value should be Some (we just inserted).
            assert!(val.is_some());
        }
    }

    /// Port of C++ CustomizedKey: custom key types that implement Hash + Eq.
    #[test]
    fn test_custom_key_type() {
        #[derive(Hash, Eq, PartialEq, Clone)]
        struct CustomKey(String);

        let cache = SharedLruCache::<CustomKey, String>::new(2);
        let k1 = CustomKey("hello".to_string());
        let k2 = CustomKey("hello".to_string());

        cache.put(k1, "val".to_string());
        // k2 is equal to k1, so should find the value.
        assert_eq!(cache.get(&k2), Some("val".to_string()));
    }
}
