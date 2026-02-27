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
}
