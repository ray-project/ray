//go:build go1.15 && !go1.18
// +build go1.15,!go1.18

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

package loader

import (

    `github.com/bytedance/sonic/internal/loader`
)

func (self Loader) LoadOne(text []byte, funcName string, frameSize int, argSize int, argStackmap []bool, localStackmap []bool) Function {
    return Function(loader.Loader(text).Load(funcName, frameSize, argSize, argStackmap, localStackmap))
}

func Load(modulename string, filenames []string, funcs []Func, text []byte) (out []Function) {
    panic("not implemented") 
}