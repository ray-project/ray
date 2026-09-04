// Copyright 2025 The Ray Authors.
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

package ids

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash"
	"sync"
	"unsafe"
)

const (
	UniqueIDSize                    = 28
	JobIDSize                       = 4
	ActorIDSize                     = 16
	ActorIDUniqueBytesSize          = 12
	TaskIDSize                      = 24
	TaskIDUniqueBytesSize           = 8
	ObjectIDSize                    = 28
	ObjectIDIndexSize               = 4
	PlacementGroupIDSize            = 18
	PlacementGroupIDUniqueBytesSize = 14
	LeaseIDSize                     = 32
	LeaseIDUniqueBytesSize          = 4
)

const MaxObjectIndex int64 = (1 << (ObjectIDIndexSize * 8)) - 1

func idToHex(data []byte) string {
	return hex.EncodeToString(data)
}

// decodeHexToBytes decodes into the destination array in place to avoid a
// double allocation, and uses unsafe to avoid a heap allocation for the
// read-only source string.
func decodeHexToBytes(dst []byte, hexStr string) error {
	expectedLen := len(dst)
	if len(hexStr) != 2*expectedLen {
		return errors.New("invalid hex string length")
	}
	// Use unsafe to avoid a heap allocation (read-only path).
	hexBytes := unsafe.Slice(unsafe.StringData(hexStr), len(hexStr))
	n, err := hex.Decode(dst, hexBytes)
	if err != nil {
		return err
	}
	if n != expectedLen {
		return errors.New("hex decode length mismatch")
	}
	return nil
}

func fillRandom(data []byte) {
	_, err := rand.Read(data)
	if err != nil {
		panic("failed to generate random bytes: " + err.Error())
	}
}

// hasherPool reuses sha256 hashers to reduce GC pressure on the high-throughput path.
var hasherPool = sync.Pool{
	New: func() interface{} { return sha256.New() },
}

// generateUniqueBytesInto writes the unique bytes in place, avoiding the double
// allocation that would otherwise result from a slice escaping to the heap.
func generateUniqueBytesInto(dst []byte, jobID JobID, parentTaskID TaskID,
	counter uint64, extra int64,
) {
	h := hasherPool.Get().(hash.Hash)
	defer hasherPool.Put(h)
	h.Reset()

	h.Write(jobID.data[:])

	h.Write(parentTaskID.data[:])

	// Write the counter, using a stack allocation to avoid a heap allocation.
	var counterBytes [8]byte
	binary.LittleEndian.PutUint64(counterBytes[:], counter)
	h.Write(counterBytes[:])

	// Write the optional extra value, using a stack allocation to avoid a heap allocation.
	if extra != 0 {
		var extraBytes [8]byte
		binary.LittleEndian.PutUint64(extraBytes[:], uint64(extra))
		h.Write(extraBytes[:])
	}

	// Receive the hash into a stack-allocated array.
	var hashBuf [sha256.Size]byte
	h.Sum(hashBuf[:0])
	copy(dst, hashBuf[:len(dst)])
}

// generateUniqueBytes returns the slice returned by this function, which escapes
// to the heap; prefer generateUniqueBytesInto for direct in-place writes.
func generateUniqueBytes(jobID JobID, parentTaskID TaskID,
	counter uint64, extra int64, length int,
) []byte {
	var buf [sha256.Size]byte
	generateUniqueBytesInto(buf[:length], jobID, parentTaskID, counter, extra)
	return buf[:length]
}
