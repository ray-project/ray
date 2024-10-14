/**
 * Copyright 2023 ByteDance Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package loader

const (
    _N_PCDATA   = 4

    _PCDATA_UnsafePoint   = 0
    _PCDATA_StackMapIndex = 1
    _PCDATA_InlTreeIndex  = 2
    _PCDATA_ArgLiveIndex  = 3

    _PCDATA_INVALID_OFFSET = 0
)

const (
    // PCDATA_UnsafePoint values.
    PCDATA_UnsafePointSafe   = -1 // Safe for async preemption
    PCDATA_UnsafePointUnsafe = -2 // Unsafe for async preemption

    // PCDATA_Restart1(2) apply on a sequence of instructions, within
    // which if an async preemption happens, we should back off the PC
    // to the start of the sequence when resume.
    // We need two so we can distinguish the start/end of the sequence
    // in case that two sequences are next to each other.
    PCDATA_Restart1 = -3
    PCDATA_Restart2 = -4

    // Like PCDATA_RestartAtEntry, but back to function entry if async
    // preempted.
    PCDATA_RestartAtEntry = -5

    _PCDATA_START_VAL = -1
)

var emptyByte byte

func encodeValue(v int) []byte {
    return encodeVariant(toZigzag(v))
}

func toZigzag(v int) int {
    return (v << 1) ^ (v >> 31)
}

func encodeVariant(v int) []byte {
    var u int
    var r []byte

    /* split every 7 bits */
    for v > 127 {
        u = v & 0x7f
        v = v >> 7
        r = append(r, byte(u) | 0x80)
    }

    /* check for last one */
    if v == 0 {
        return r
    }

    /* add the last one */
    r = append(r, byte(v))
    return r
}

type Pcvalue struct {
    PC  uint32 // PC offset from func entry
    Val int32
}

type Pcdata []Pcvalue

// see https://docs.google.com/document/d/1lyPIbmsYbXnpNj57a261hgOYVpNRcgydurVQIyZOz_o/pub
func (self Pcdata) MarshalBinary() (data []byte, err error) {
    // delta value always starts from -1
    sv := int32(_PCDATA_START_VAL)
    sp := uint32(0)
    for _, v := range self {
        data = append(data, encodeVariant(toZigzag(int(v.Val - sv)))...)
        data = append(data, encodeVariant(int(v.PC - sp))...)
        sp = v.PC
        sv = v.Val
    }
    return
}