// +build go1.15,!go1.17

//
// Copyright 2021 ByteDance Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "go_asm.h"
#include "funcdata.h"
#include "textflag.h"

TEXT ·decodeValueStub(SB), NOSPLIT, $0 - 72
    NO_LOCAL_POINTERS
    PXOR  X0, X0
    MOVOU X0, rv+48(FP)
    MOVQ  st+0(FP), BX
    MOVQ  sp+8(FP), R12
    MOVQ  sn+16(FP), R13
    MOVQ  ic+24(FP), R14
    MOVQ  vp+32(FP), R15
    MOVQ  df+40(FP), R10
    MOVQ  ·_subr_decode_value(SB), AX
    CALL  AX
    MOVQ  R14, rp+48(FP)
    MOVQ  R11, ex+56(FP)
    RET
