/*
 * Copyright 2021 ByteDance Inc.
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

package encoder

import (
    `encoding/json`
    `unsafe`

    `github.com/bytedance/sonic/loader`
)

//go:nosplit
func padd(p unsafe.Pointer, v int) unsafe.Pointer {
    return unsafe.Pointer(uintptr(p) + uintptr(v))
}

//go:nosplit
func ptoenc(p loader.Function) _Encoder {
    return *(*_Encoder)(unsafe.Pointer(&p))
}

func compact(p *[]byte, v []byte) error {
    buf := newBuffer()
    err := json.Compact(buf, v)

    /* check for errors */
    if err != nil {
        return err
    }

    /* add to result */
    v = buf.Bytes()
    *p = append(*p, v...)

    /* return the buffer into pool */
    freeBuffer(buf)
    return nil
}
