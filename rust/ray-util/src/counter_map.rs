// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Thread-safe counter map.
//!
//! Replaces C++ `counter_map.h`.

use std::collections::HashMap;
use std::hash::Hash;

/// A map from keys to integer counts, with automatic cleanup of zero-count entries.
#[derive(Debug, Clone)]
pub struct CounterMap<K: Eq + Hash> {
    counters: HashMap<K, i64>,
    total: i64,
}

impl<K: Eq + Hash> Default for CounterMap<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Eq + Hash> CounterMap<K> {
    pub fn new() -> Self {
        Self {
            counters: HashMap::new(),
            total: 0,
        }
    }

    /// Increment the count for a key. Returns the new count.
    pub fn increment(&mut self, key: K) -> i64 {
        self.total += 1;
        let count = self.counters.entry(key).or_insert(0);
        *count += 1;
        *count
    }

    /// Decrement the count for a key. Returns the new count.
    /// Removes the entry if count reaches zero.
    pub fn decrement(&mut self, key: K) -> i64
    where
        K: Clone,
    {
        self.total -= 1;
        if let Some(count) = self.counters.get_mut(&key) {
            *count -= 1;
            let result = *count;
            if result == 0 {
                self.counters.remove(&key);
            }
            result
        } else {
            // Key didn't exist; insert -1
            self.counters.insert(key, -1);
            -1
        }
    }

    /// Get the count for a key (0 if not present).
    pub fn get(&self, key: &K) -> i64 {
        self.counters.get(key).copied().unwrap_or(0)
    }

    /// Get the total of all counts.
    pub fn total(&self) -> i64 {
        self.total
    }

    /// Number of distinct keys with non-zero counts.
    pub fn len(&self) -> usize {
        self.counters.len()
    }

    pub fn is_empty(&self) -> bool {
        self.counters.is_empty()
    }

    /// Increment the count for a key by a given amount. Returns the new count.
    pub fn increment_by(&mut self, key: K, amount: i64) -> i64
    where
        K: Clone,
    {
        self.total += amount;
        let count = self.counters.entry(key.clone()).or_insert(0);
        *count += amount;
        let result = *count;
        if result == 0 {
            self.counters.remove(&key);
        }
        result
    }

    /// Decrement the count for a key by a given amount. Returns the new count.
    /// Removes the entry if count reaches zero.
    pub fn decrement_by(&mut self, key: K, amount: i64) -> i64
    where
        K: Clone,
    {
        self.total -= amount;
        if let Some(count) = self.counters.get_mut(&key) {
            *count -= amount;
            let result = *count;
            if result == 0 {
                self.counters.remove(&key);
            }
            result
        } else {
            self.counters.insert(key, -amount);
            -amount
        }
    }

    /// Swap `amount` counts from `src` key to `dst` key.
    /// Decrements `src` by `amount` and increments `dst` by `amount`.
    /// Total remains unchanged.
    pub fn swap(&mut self, src: K, dst: K, amount: i64)
    where
        K: Clone,
    {
        // Decrement src
        if let Some(count) = self.counters.get_mut(&src) {
            *count -= amount;
            if *count == 0 {
                self.counters.remove(&src);
            }
        } else {
            self.counters.insert(src, -amount);
        }

        // Increment dst
        let count = self.counters.entry(dst).or_insert(0);
        *count += amount;
    }

    /// Iterate over all (key, count) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &i64)> {
        self.counters.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_increment_decrement() {
        let mut map = CounterMap::new();
        assert_eq!(map.increment("a"), 1);
        assert_eq!(map.increment("a"), 2);
        assert_eq!(map.increment("b"), 1);
        assert_eq!(map.total(), 3);

        assert_eq!(map.decrement("a"), 1);
        assert_eq!(map.get(&"a"), 1);
        assert_eq!(map.decrement("a"), 0);
        assert_eq!(map.len(), 1); // "a" removed, "b" remains
    }

    #[test]
    fn test_empty() {
        let map: CounterMap<&str> = CounterMap::new();
        assert!(map.is_empty());
        assert_eq!(map.total(), 0);
        assert_eq!(map.get(&"x"), 0);
    }

    #[test]
    fn test_iter() {
        let mut map = CounterMap::new();
        map.increment("a");
        map.increment("a");
        map.increment("b");

        let pairs: std::collections::HashMap<&&str, &i64> = map.iter().collect();
        assert_eq!(pairs.len(), 2);
        assert_eq!(*pairs[&"a"], 2);
        assert_eq!(*pairs[&"b"], 1);
    }

    #[test]
    fn test_decrement_nonexistent() {
        let mut map = CounterMap::new();
        let count = map.decrement("x");
        assert_eq!(count, -1);
        assert_eq!(map.get(&"x"), -1);
        assert_eq!(map.total(), -1);
    }

    #[test]
    fn test_default() {
        let map: CounterMap<String> = CounterMap::default();
        assert!(map.is_empty());
    }

    #[test]
    fn test_clone() {
        let mut map = CounterMap::new();
        map.increment("a");
        let cloned = map.clone();
        assert_eq!(cloned.get(&"a"), 1);
        assert_eq!(cloned.total(), 1);
    }

    // --- Ported from C++ counter_test.cc ---

    /// Port of C++ TestBasic: full increment/decrement/multi-value ops.
    #[test]
    fn test_basic_from_cpp() {
        let mut c = CounterMap::<String>::new();
        c.increment("k1".into());
        c.increment("k1".into());
        c.increment("k2".into());
        assert_eq!(c.get(&"k1".into()), 2);
        assert_eq!(c.get(&"k2".into()), 1);
        assert_eq!(c.get(&"k3".into()), 0);
        assert_eq!(c.total(), 3);
        assert_eq!(c.len(), 2);

        c.decrement("k1".into());
        c.decrement("k2".into());
        assert_eq!(c.get(&"k1".into()), 1);
        assert_eq!(c.get(&"k2".into()), 0);
        assert_eq!(c.get(&"k3".into()), 0);
        assert_eq!(c.total(), 1);
        assert_eq!(c.len(), 1);
    }

    /// Port of C++ TestIterate: iterate and check values.
    #[test]
    fn test_iterate_from_cpp() {
        let mut c = CounterMap::<String>::new();
        let mut num_keys = 0;
        c.increment("k1".into());
        c.increment("k1".into());
        c.increment("k2".into());

        for (key, &value) in c.iter() {
            num_keys += 1;
            if key == "k1" {
                assert_eq!(value, 2);
            } else {
                assert_eq!(value, 1);
            }
        }
        assert_eq!(num_keys, 2);
    }

    // --- Ported from C++ counter_test.cc: Swap and multi-value ops ---

    /// Port of C++ TestBasic: swap operation.
    #[test]
    fn test_swap_from_cpp() {
        let mut c = CounterMap::<String>::new();
        c.increment("k1".into());
        c.increment("k1".into());
        c.increment("k2".into());
        c.decrement("k1".into());
        c.decrement("k2".into());
        // Now: k1=1, k2=0(removed), total=1

        c.swap("k1".into(), "k2".into(), 1);
        assert_eq!(c.get(&"k1".into()), 0);
        assert_eq!(c.get(&"k2".into()), 1);
        assert_eq!(c.total(), 1);
        assert_eq!(c.len(), 1);
    }

    /// Port of C++ TestBasic: multi-value increment/decrement.
    #[test]
    fn test_multi_value_ops_from_cpp() {
        let mut c = CounterMap::<String>::new();
        c.increment("k1".into());
        c.increment("k1".into());
        c.increment("k2".into());
        c.decrement("k1".into());
        c.decrement("k2".into());
        c.swap("k1".into(), "k2".into(), 1);
        // Now: k1=0, k2=1, total=1

        // Test multi-value increment
        c.increment_by("k1".into(), 10);
        c.increment_by("k2".into(), 5);
        assert_eq!(c.get(&"k1".into()), 10);
        assert_eq!(c.get(&"k2".into()), 6);
        assert_eq!(c.total(), 16);
        assert_eq!(c.len(), 2);

        // Test multi-value decrement
        c.decrement_by("k1".into(), 5);
        c.decrement_by("k2".into(), 1);
        assert_eq!(c.get(&"k1".into()), 5);
        assert_eq!(c.get(&"k2".into()), 5);
        assert_eq!(c.total(), 10);
        assert_eq!(c.len(), 2);

        // Swap with amount
        c.swap("k1".into(), "k2".into(), 5);
        assert_eq!(c.get(&"k1".into()), 0);
        assert_eq!(c.get(&"k2".into()), 10);
        assert_eq!(c.total(), 10);
        assert_eq!(c.len(), 1);
    }

    /// Test increment_by with zero amount.
    #[test]
    fn test_increment_by_zero() {
        let mut c = CounterMap::<String>::new();
        c.increment("k1".into());
        c.increment_by("k1".into(), 0);
        assert_eq!(c.get(&"k1".into()), 1);
        assert_eq!(c.total(), 1);
    }

    /// Test decrement_by removing entry at zero.
    #[test]
    fn test_decrement_by_to_zero_removes() {
        let mut c = CounterMap::<String>::new();
        c.increment_by("k1".into(), 5);
        assert_eq!(c.len(), 1);
        c.decrement_by("k1".into(), 5);
        assert_eq!(c.get(&"k1".into()), 0);
        assert_eq!(c.len(), 0);
        assert_eq!(c.total(), 0);
    }

    /// Test swap between two non-existent keys.
    #[test]
    fn test_swap_nonexistent_keys() {
        let mut c = CounterMap::<String>::new();
        c.swap("k1".into(), "k2".into(), 3);
        assert_eq!(c.get(&"k1".into()), -3);
        assert_eq!(c.get(&"k2".into()), 3);
        // total is unchanged (still 0)
        assert_eq!(c.total(), 0);
    }
}
