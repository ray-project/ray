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
	"testing"

	"github.com/stretchr/testify/assert"
)

// ---------- InMemoryStorage ----------

// TestInMemoryStorage_BatchUpdate covers the basic CRUD and the version
// increments.
func TestInMemoryStorage_BatchUpdate(t *testing.T) {
	storage := NewInMemoryStorage()

	// The initial version is 0.
	assert.EqualValues(t, 0, storage.GetVersion())

	// Writing two entries increments the version by 1; the entry versions
	// equal the new storage version.
	status := storage.BatchUpdate("t", map[string][]byte{"a": []byte("va"), "b": []byte("vb")}, nil, nil)
	assert.True(t, status.Success)
	assert.EqualValues(t, 1, status.Version)

	entries, version := storage.GetAll("t")
	assert.EqualValues(t, 1, version)
	assert.Equal(t, []byte("va"), entries["a"].Value)
	assert.EqualValues(t, 1, entries["a"].Version)
	assert.EqualValues(t, 1, entries["b"].Version)

	// Deleting an entry increments the version by 1.
	status = storage.BatchUpdate("t", nil, []string{"a"}, nil)
	assert.True(t, status.Success)
	assert.EqualValues(t, 2, status.Version)
	entries, _ = storage.GetAll("t")
	assert.NotContains(t, entries, "a")
	assert.Contains(t, entries, "b")

	// Deleting a missing key does not fail; the version still increments.
	status = storage.BatchUpdate("t", nil, []string{"missing"}, nil)
	assert.True(t, status.Success)
}

// TestInMemoryStorage_BatchUpdateExpectedVersion covers optimistic concurrency
// control: a batch update fails on an expected version mismatch and leaves the
// version unchanged.
func TestInMemoryStorage_BatchUpdateExpectedVersion(t *testing.T) {
	storage := NewInMemoryStorage()
	storage.BatchUpdate("t", map[string][]byte{"a": []byte("va")}, nil, nil)
	assert.EqualValues(t, 1, storage.GetVersion())

	stale := int64(0)
	status := storage.BatchUpdate("t", map[string][]byte{"a": []byte("va2")}, nil, &stale)
	assert.False(t, status.Success)
	assert.EqualValues(t, 1, status.Version)
	// A failed update does not change the data.
	entries, _ := storage.GetAll("t")
	assert.Equal(t, []byte("va"), entries["a"].Value)

	current := int64(1)
	status = storage.BatchUpdate("t", map[string][]byte{"a": []byte("va2")}, nil, &current)
	assert.True(t, status.Success)
	assert.EqualValues(t, 2, status.Version)
}

// TestInMemoryStorage_Update covers the various validations of a single
// update.
func TestInMemoryStorage_Update(t *testing.T) {
	storage := NewInMemoryStorage()

	// A normal write.
	status := storage.Update("t", "a", []byte("va"), nil, nil, false)
	assert.True(t, status.Success)
	assert.EqualValues(t, 1, status.Version)

	// insertOnly: fails when the key already exists.
	status = storage.Update("t", "a", []byte("va2"), nil, nil, true)
	assert.False(t, status.Success)
	// insertOnly: succeeds on a new key.
	status = storage.Update("t", "b", []byte("vb"), nil, nil, true)
	assert.True(t, status.Success)

	// Entry version check: fails on an expected version mismatch.
	stale := int64(0)
	status = storage.Update("t", "a", []byte("va3"), &stale, nil, false)
	assert.False(t, status.Success)
	// Entry version check: succeeds when the expected version matches.
	current := int64(1)
	status = storage.Update("t", "a", []byte("va3"), &current, nil, false)
	assert.True(t, status.Success)
	entries, _ := storage.GetAll("t")
	assert.EqualValues(t, 3, entries["a"].Version)

	// Storage version check: fails on an expected version mismatch.
	storageVersion := int64(1)
	status = storage.Update("t", "a", []byte("va4"), nil, &storageVersion, false)
	assert.False(t, status.Success)

	// Entry version check on a missing entry: the initial version is -1.
	missing := int64(-1)
	status = storage.Update("t", "c", []byte("vc"), &missing, nil, false)
	assert.True(t, status.Success)
}

// TestInMemoryStorage_Get reads by key, skipping missing keys; an empty key
// list returns everything.
func TestInMemoryStorage_Get(t *testing.T) {
	storage := NewInMemoryStorage()
	storage.BatchUpdate("t", map[string][]byte{
		"a": []byte("va"), "b": []byte("vb"), "c": []byte("vc"),
	}, nil, nil)

	entries, version := storage.Get("t", []string{"a", "missing", "c"})
	assert.EqualValues(t, 1, version)
	assert.Len(t, entries, 2)
	assert.Equal(t, []byte("va"), entries["a"].Value)
	assert.Equal(t, []byte("vc"), entries["c"].Value)

	// An empty key list is equivalent to GetAll.
	entries, _ = storage.Get("t", nil)
	assert.Len(t, entries, 3)

	// Reading a missing table returns empty.
	entries, _ = storage.Get("missing-table", nil)
	assert.Empty(t, entries)
}

// TestInMemoryStorage_Concurrency verifies no version is lost under concurrent
// writes.
func TestInMemoryStorage_Concurrency(t *testing.T) {
	storage := NewInMemoryStorage()
	const goroutines = 10
	const writes = 50

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < writes; j++ {
				status := storage.Update("t", string(rune('a'+id)), []byte("v"), nil, nil, false)
				assert.True(t, status.Success)
			}
		}(i)
	}
	wg.Wait()

	// Every successful write increments the version by 1; the total should be
	// goroutines * writes.
	assert.EqualValues(t, goroutines*writes, storage.GetVersion())
}
