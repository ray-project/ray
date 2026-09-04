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

// murmurHash64A ports the public-domain algorithm from C++ id.cc lines 74-126.
func murmurHash64A(key []byte, seed uint64) uint64 {
	const m uint64 = 0xc6a4a7935bd1e995
	const r = 47

	h := seed ^ (uint64(len(key)) * m)

	nblocks := len(key) / 8
	for i := 0; i < nblocks; i++ {
		k := uint64(key[i*8]) |
			uint64(key[i*8+1])<<8 |
			uint64(key[i*8+2])<<16 |
			uint64(key[i*8+3])<<24 |
			uint64(key[i*8+4])<<32 |
			uint64(key[i*8+5])<<40 |
			uint64(key[i*8+6])<<48 |
			uint64(key[i*8+7])<<56

		k *= m
		k ^= k >> r
		k *= m

		h ^= k
		h *= m
	}

	tail := key[nblocks*8:]
	switch len(tail) {
	case 7:
		h ^= uint64(tail[6]) << 48
		fallthrough
	case 6:
		h ^= uint64(tail[5]) << 40
		fallthrough
	case 5:
		h ^= uint64(tail[4]) << 32
		fallthrough
	case 4:
		h ^= uint64(tail[3]) << 24
		fallthrough
	case 3:
		h ^= uint64(tail[2]) << 16
		fallthrough
	case 2:
		h ^= uint64(tail[1]) << 8
		fallthrough
	case 1:
		h ^= uint64(tail[0])
		h *= m
	}

	h ^= h >> r
	h *= m
	h ^= h >> r

	return h
}
