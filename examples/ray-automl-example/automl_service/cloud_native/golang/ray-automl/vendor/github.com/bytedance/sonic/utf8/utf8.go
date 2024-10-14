/*
 * Copyright 2022 ByteDance Inc.
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

package utf8

import (
    `github.com/bytedance/sonic/internal/rt`
    `github.com/bytedance/sonic/internal/native/types`
    `github.com/bytedance/sonic/internal/native`
)

// CorrectWith corrects the invalid utf8 byte with repl string.
func CorrectWith(dst []byte, src []byte, repl string) []byte {
    sstr := rt.Mem2Str(src)
    sidx := 0

    /* state machine records the invalid postions */
    m := types.NewStateMachine()
    m.Sp = 0 // invalid utf8 numbers

    for sidx < len(sstr) {
        scur  := sidx
        ecode := native.ValidateUTF8(&sstr, &sidx, m)

        if m.Sp != 0 {
            if m.Sp > len(sstr) {
                panic("numbers of invalid utf8 exceed the string len!")
            }
        }
        
        for i := 0; i < m.Sp; i++ {
            ipos := m.Vt[i] // invalid utf8 position
            dst  = append(dst, sstr[scur:ipos]...)
            dst  = append(dst, repl...)
            scur = m.Vt[i] + 1
        }
        /* append the remained valid utf8 bytes */
        dst = append(dst, sstr[scur:sidx]...)

        /* not enough space, reset and continue */
        if ecode != 0 {
            m.Sp = 0
        }
    }

    types.FreeStateMachine(m)
    return dst
}

// Validate is a simd-accelereated drop-in replacement for the standard library's utf8.Valid.
func Validate(src []byte) bool {
    return ValidateString(rt.Mem2Str(src))
}

// ValidateString as Validate, but for string.
func ValidateString(src string) bool {
    return native.ValidateUTF8Fast(&src) == 0
}