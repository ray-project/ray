// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! LRU eviction policy for the plasma store.
//!
//! Replaces `src/ray/object_manager/plasma/eviction_policy.h/cc`.

use std::collections::{HashMap, VecDeque};

use ray_common::id::ObjectID;

/// A cache entry in the LRU list.
#[derive(Debug)]
struct CacheEntry {
    object_id: ObjectID,
    size: i64,
}

/// An LRU (Least Recently Used) cache that tracks objects by access order.
///
/// Matches C++ `LRUCache` — uses a doubly-linked list for O(1) eviction
/// and a hash map for O(1) lookup.
pub struct LruCache {
    /// Items in LRU order (front = least recently used).
    item_list: VecDeque<CacheEntry>,
    /// Map from ObjectID to index in item_list.
    item_map: HashMap<ObjectID, usize>,
    /// Cache name for debugging.
    name: String,
    /// Original capacity in bytes.
    original_capacity: i64,
    /// Current capacity (may be adjusted).
    capacity: i64,
    /// Currently used bytes.
    used_capacity: i64,
    /// Total evictions performed.
    num_evictions_total: i64,
    /// Total bytes evicted.
    bytes_evicted_total: i64,
}

impl LruCache {
    pub fn new(name: &str, capacity: i64) -> Self {
        Self {
            item_list: VecDeque::new(),
            item_map: HashMap::new(),
            name: name.to_string(),
            original_capacity: capacity,
            capacity,
            used_capacity: 0,
            num_evictions_total: 0,
            bytes_evicted_total: 0,
        }
    }

    /// Add an object to the cache (most recently used position).
    pub fn add(&mut self, key: ObjectID, size: i64) {
        if self.item_map.contains_key(&key) {
            return;
        }
        let index = self.item_list.len();
        self.item_list.push_back(CacheEntry {
            object_id: key,
            size,
        });
        self.item_map.insert(key, index);
        self.used_capacity += size;
    }

    /// Remove an object from the cache. Returns its size.
    pub fn remove(&mut self, key: &ObjectID) -> i64 {
        if let Some(&index) = self.item_map.get(key) {
            let size = self.item_list[index].size;
            // Mark as removed (tombstone approach for O(1) removal)
            self.item_map.remove(key);
            self.used_capacity -= size;
            // Compact later during eviction scan
            size
        } else {
            0
        }
    }

    /// Choose objects to evict to free `num_bytes_required`.
    /// Returns the total bytes that can be freed by evicting the chosen objects.
    pub fn choose_objects_to_evict(
        &mut self,
        num_bytes_required: i64,
        objects_to_evict: &mut Vec<ObjectID>,
    ) -> i64 {
        let mut bytes_evicted = 0i64;
        let mut to_remove = Vec::new();

        // Scan from front (LRU) to back (MRU)
        for entry in &self.item_list {
            if bytes_evicted >= num_bytes_required {
                break;
            }
            // Skip tombstoned entries
            if !self.item_map.contains_key(&entry.object_id) {
                continue;
            }
            objects_to_evict.push(entry.object_id);
            to_remove.push(entry.object_id);
            bytes_evicted += entry.size;
        }

        // Remove evicted objects
        for oid in &to_remove {
            self.remove(oid);
            self.num_evictions_total += 1;
            self.bytes_evicted_total += self
                .item_list
                .iter()
                .find(|e| e.object_id == *oid)
                .map(|e| e.size)
                .unwrap_or(0);
        }

        // Compact the list periodically
        if self.item_list.len() > self.item_map.len() * 2 {
            self.compact();
        }

        bytes_evicted
    }

    /// Compact the internal list by removing tombstoned entries.
    fn compact(&mut self) {
        let live_entries: VecDeque<CacheEntry> = self
            .item_list
            .drain(..)
            .filter(|e| self.item_map.contains_key(&e.object_id))
            .collect();
        self.item_list = live_entries;
        // Rebuild index map
        self.item_map.clear();
        for (i, entry) in self.item_list.iter().enumerate() {
            self.item_map.insert(entry.object_id, i);
        }
    }

    pub fn original_capacity(&self) -> i64 {
        self.original_capacity
    }

    pub fn capacity(&self) -> i64 {
        self.capacity
    }

    pub fn remaining_capacity(&self) -> i64 {
        self.capacity - self.used_capacity
    }

    pub fn used_capacity(&self) -> i64 {
        self.used_capacity
    }

    /// Adjust capacity by a delta (can be positive or negative).
    pub fn adjust_capacity(&mut self, delta: i64) {
        self.capacity += delta;
    }

    pub fn exists(&self, key: &ObjectID) -> bool {
        self.item_map.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.item_map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.item_map.is_empty()
    }

    pub fn debug_string(&self) -> String {
        format!(
            "LRUCache(name={}, capacity={}/{}, used={}, items={}, evictions={}, bytes_evicted={})",
            self.name,
            self.capacity,
            self.original_capacity,
            self.used_capacity,
            self.item_map.len(),
            self.num_evictions_total,
            self.bytes_evicted_total,
        )
    }
}

/// Eviction policy that uses LRU to choose victim objects.
///
/// Matches C++ `EvictionPolicy`.
pub struct EvictionPolicy {
    /// Bytes currently pinned by applications (not evictable).
    pinned_memory_bytes: i64,
    /// The LRU cache tracking evictable objects.
    cache: LruCache,
}

impl EvictionPolicy {
    pub fn new(capacity: i64) -> Self {
        Self {
            pinned_memory_bytes: 0,
            cache: LruCache::new("plasma_eviction", capacity),
        }
    }

    /// Notify that a new object was created and is evictable.
    pub fn object_created(&mut self, object_id: ObjectID, size: i64) {
        self.cache.add(object_id, size);
    }

    /// Request `size` bytes of space. Populates `objects_to_evict` with
    /// objects that should be evicted to make room. Returns total bytes freed.
    pub fn require_space(&mut self, size: i64, objects_to_evict: &mut Vec<ObjectID>) -> i64 {
        if self.cache.remaining_capacity() >= size {
            return 0;
        }
        let bytes_needed = size - self.cache.remaining_capacity();
        self.cache
            .choose_objects_to_evict(bytes_needed, objects_to_evict)
    }

    /// Mark object as being accessed (pinned — not evictable).
    pub fn begin_object_access(&mut self, object_id: &ObjectID) {
        let size = self.cache.remove(object_id);
        if size > 0 {
            self.pinned_memory_bytes += size;
        }
    }

    /// Mark object as no longer being accessed (unpinned — evictable again).
    pub fn end_object_access(&mut self, object_id: ObjectID, size: i64) {
        self.cache.add(object_id, size);
        self.pinned_memory_bytes -= size;
        if self.pinned_memory_bytes < 0 {
            self.pinned_memory_bytes = 0;
        }
    }

    /// Remove an object entirely (e.g., after deletion).
    pub fn remove_object(&mut self, object_id: &ObjectID) {
        self.cache.remove(object_id);
    }

    pub fn pinned_memory_bytes(&self) -> i64 {
        self.pinned_memory_bytes
    }

    pub fn debug_string(&self) -> String {
        format!(
            "EvictionPolicy(pinned_bytes={}, {})",
            self.pinned_memory_bytes,
            self.cache.debug_string()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_oid(val: u8) -> ObjectID {
        let mut data = [0u8; 28];
        data[0] = val;
        ObjectID::from_binary(&data)
    }

    #[test]
    fn test_lru_basic() {
        let mut cache = LruCache::new("test", 1000);
        let o1 = make_oid(1);
        let o2 = make_oid(2);
        let o3 = make_oid(3);

        cache.add(o1, 100);
        cache.add(o2, 200);
        cache.add(o3, 300);

        assert_eq!(cache.len(), 3);
        assert_eq!(cache.used_capacity(), 600);
        assert_eq!(cache.remaining_capacity(), 400);
    }

    #[test]
    fn test_lru_eviction_order() {
        let mut cache = LruCache::new("test", 1000);
        let o1 = make_oid(1);
        let o2 = make_oid(2);
        let o3 = make_oid(3);

        cache.add(o1, 100);
        cache.add(o2, 200);
        cache.add(o3, 300);

        // Evict 250 bytes — should evict o1 (100) + o2 (200)
        let mut evicted = Vec::new();
        let freed = cache.choose_objects_to_evict(250, &mut evicted);
        assert!(freed >= 250);
        assert!(evicted.contains(&o1));
        assert!(evicted.contains(&o2));
        assert!(!evicted.contains(&o3));
    }

    #[test]
    fn test_eviction_policy_pinning() {
        let mut policy = EvictionPolicy::new(1000);
        let o1 = make_oid(1);
        let o2 = make_oid(2);

        policy.object_created(o1, 400);
        policy.object_created(o2, 400);

        // Pin o1 — it should not be evictable
        policy.begin_object_access(&o1);

        let mut evicted = Vec::new();
        policy.require_space(500, &mut evicted);
        // Only o2 should be evictable
        assert!(!evicted.contains(&o1));
    }

    #[test]
    fn test_eviction_policy_unpin() {
        let mut policy = EvictionPolicy::new(1000);
        let o1 = make_oid(1);
        policy.object_created(o1, 400);
        policy.begin_object_access(&o1);
        assert_eq!(policy.pinned_memory_bytes(), 400);

        policy.end_object_access(o1, 400);
        assert_eq!(policy.pinned_memory_bytes(), 0);
    }
}
