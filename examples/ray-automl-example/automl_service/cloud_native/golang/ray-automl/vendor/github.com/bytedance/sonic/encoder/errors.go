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
    `fmt`
    `reflect`
    `strconv`
    `unsafe`

    `github.com/bytedance/sonic/internal/rt`
)

var _ERR_too_deep = &json.UnsupportedValueError {
    Str   : "Value nesting too deep",
    Value : reflect.ValueOf("..."),
}

var _ERR_nan_or_infinite = &json.UnsupportedValueError {
    Str   : "NaN or ±Infinite",
    Value : reflect.ValueOf("NaN or ±Infinite"),
}

func error_type(vtype reflect.Type) error {
    return &json.UnsupportedTypeError{Type: vtype}
}

func error_number(number json.Number) error {
    return &json.UnsupportedValueError {
        Str   : "invalid number literal: " + strconv.Quote(string(number)),
        Value : reflect.ValueOf(number),
    }
}

func error_marshaler(ret []byte, pos int) error {
    return fmt.Errorf("invalid Marshaler output json syntax at %d: %q", pos, ret)
}

const (
    panicNilPointerOfNonEmptyString int = 1 + iota
)

func goPanic(code int, val unsafe.Pointer) {
    switch(code){
    case panicNilPointerOfNonEmptyString:
        panic(fmt.Sprintf("val: %#v has nil pointer while its length is not zero!", (*rt.GoString)(val)))
    default:
        panic("encoder error!")
    }
}