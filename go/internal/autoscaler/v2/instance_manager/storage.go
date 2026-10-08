// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package instance_manager

import (
	"sync"
)

// StoreStatus is the storage operation result.
// Corresponds to Python storage.StoreStatus.
type StoreStatus struct {
	// Success is whether the operation succeeded.
	Success bool
	// Version is the new version on success, the current version on failure.
	Version int64
}

// VersionedValue is a versioned storage entry.
// Corresponds to Python storage.VersionedValue.
type VersionedValue struct {
	// Value is the stored value (the serialized Instance).
	Value []byte
	// Version is the storage version when this entry was last updated.
	Version int64
}

// IStorage is the storage backend interface.
// Corresponds to Python storage.Storage:
// the storage is thread safe; it is versioned, and every successful change
// increments the storage version by 1, which can be used for optimistic
// concurrency control; every entry is versioned too, and the entry version is
// the storage version when the entry was last updated.
type IStorage interface {
	// BatchUpdate atomically batch updates a storage table.
	// When expectedStorageVersion is non-nil and does not match the current
	// storage version, the update fails.
	BatchUpdate(table string, mutation map[string][]byte, deletion []string, expectedStorageVersion *int64) StoreStatus
	// Update updates a single entry.
	// When expectedEntryVersion/expectedStorageVersion are non-nil, the
	// corresponding version checks run; when insertOnly is true, the update
	// fails if the entry already exists.
	Update(table string, key string, value []byte, expectedEntryVersion *int64, expectedStorageVersion *int64, insertOnly bool) StoreStatus
	// GetAll returns all entries of the table and the current storage version.
	GetAll(table string) (map[string]VersionedValue, int64)
	// Get returns the entries of the given keys (missing keys are skipped) and
	// the current storage version; empty keys is equivalent to GetAll.
	Get(table string, keys []string) (map[string]VersionedValue, int64)
	// GetVersion returns the current storage version.
	GetVersion() int64
}

// InMemoryStorage is the in-memory implementation of IStorage without
// persistence.
// Corresponds to Python storage.InMemoryStorage.
type InMemoryStorage struct {
	mutex   sync.Mutex
	version int64
	tables  map[string]map[string]VersionedValue
}

// NewInMemoryStorage creates the in-memory storage instance.
func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{
		tables: make(map[string]map[string]VersionedValue),
	}
}

// table returns the given table, lazily creating it (the caller must hold the
// mutex).
func (s *InMemoryStorage) table(name string) map[string]VersionedValue {
	t, ok := s.tables[name]
	if !ok {
		t = make(map[string]VersionedValue)
		s.tables[name] = t
	}
	return t
}

// BatchUpdate atomically batch updates a storage table.
func (s *InMemoryStorage) BatchUpdate(table string, mutation map[string][]byte, deletion []string, expectedStorageVersion *int64) StoreStatus {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if expectedStorageVersion != nil && *expectedStorageVersion != s.version {
		return StoreStatus{Success: false, Version: s.version}
	}
	s.version++
	t := s.table(table)
	for key, value := range mutation {
		t[key] = VersionedValue{Value: value, Version: s.version}
	}
	for _, key := range deletion {
		delete(t, key)
	}
	return StoreStatus{Success: true, Version: s.version}
}

// Update updates a single entry.
func (s *InMemoryStorage) Update(table string, key string, value []byte, expectedEntryVersion *int64, expectedStorageVersion *int64, insertOnly bool) StoreStatus {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if expectedStorageVersion != nil && *expectedStorageVersion != s.version {
		return StoreStatus{Success: false, Version: s.version}
	}
	t := s.table(table)
	if insertOnly {
		if _, exists := t[key]; exists {
			return StoreStatus{Success: false, Version: s.version}
		}
	}
	// A new entry starts at version -1 (not existing yet), matching Python's
	// get(key, (None, -1)).
	entryVersion := int64(-1)
	if existing, exists := t[key]; exists {
		entryVersion = existing.Version
	}
	if expectedEntryVersion != nil && *expectedEntryVersion != entryVersion {
		return StoreStatus{Success: false, Version: s.version}
	}
	s.version++
	t[key] = VersionedValue{Value: value, Version: s.version}
	return StoreStatus{Success: true, Version: s.version}
}

// GetAll returns all entries of the table and the current storage version.
func (s *InMemoryStorage) GetAll(table string) (map[string]VersionedValue, int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	result := make(map[string]VersionedValue, len(s.tables[table]))
	for key, value := range s.tables[table] {
		result[key] = value
	}
	return result, s.version
}

// Get returns the entries of the given keys and the current storage version.
func (s *InMemoryStorage) Get(table string, keys []string) (map[string]VersionedValue, int64) {
	if len(keys) == 0 {
		return s.GetAll(table)
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	result := make(map[string]VersionedValue, len(keys))
	t := s.table(table)
	for _, key := range keys {
		if value, exists := t[key]; exists {
			result[key] = value
		}
	}
	return result, s.version
}

// GetVersion returns the current storage version.
func (s *InMemoryStorage) GetVersion() int64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.version
}
