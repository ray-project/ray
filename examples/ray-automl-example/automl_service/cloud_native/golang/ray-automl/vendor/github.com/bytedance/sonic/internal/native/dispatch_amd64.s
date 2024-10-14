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

TEXT ·Quote(SB), NOSPLIT, $0 - 48
    CMPB github·com∕bytedance∕sonic∕internal∕cpu·HasAVX2(SB), $0
    JE   2(PC)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕avx2·__quote(SB)
    CMPB github·com∕bytedance∕sonic∕internal∕cpu·HasAVX(SB), $0
    JE   2(PC)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕avx·__quote(SB)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕sse·__quote(SB)

TEXT ·Unquote(SB), NOSPLIT, $0 - 48
    CMPB github·com∕bytedance∕sonic∕internal∕cpu·HasAVX2(SB), $0
    JE   2(PC)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕avx2·__unquote(SB)
    CMPB github·com∕bytedance∕sonic∕internal∕cpu·HasAVX(SB), $0
    JE   2(PC)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕avx·__unquote(SB)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕sse·__unquote(SB)

TEXT ·HTMLEscape(SB), NOSPLIT, $0 - 40
    CMPB github·com∕bytedance∕sonic∕internal∕cpu·HasAVX2(SB), $0
    JE   2(PC)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕avx2·__html_escape(SB)
    CMPB github·com∕bytedance∕sonic∕internal∕cpu·HasAVX(SB), $0
    JE   2(PC)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕avx·__html_escape(SB)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕sse·__html_escape(SB)

TEXT ·Value(SB), NOSPLIT, $0 - 48
    CMPB github·com∕bytedance∕sonic∕internal∕cpu·HasAVX2(SB), $0
    JE   2(PC)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕avx2·__value(SB)
    CMPB github·com∕bytedance∕sonic∕internal∕cpu·HasAVX(SB), $0
    JE   2(PC)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕avx·__value(SB)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕sse·__value(SB)

TEXT ·SkipOne(SB), NOSPLIT, $0 - 40
    CMPB github·com∕bytedance∕sonic∕internal∕cpu·HasAVX2(SB), $0
    JE   2(PC)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕avx2·__skip_one(SB)
    CMPB github·com∕bytedance∕sonic∕internal∕cpu·HasAVX(SB), $0
    JE   2(PC)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕avx·__skip_one(SB)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕sse·__skip_one(SB)

TEXT ·SkipOneFast(SB), NOSPLIT, $0 - 24
    CMPB github·com∕bytedance∕sonic∕internal∕cpu·HasAVX2(SB), $0
    JE   2(PC)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕avx2·__skip_one_fast(SB)
    CMPB github·com∕bytedance∕sonic∕internal∕cpu·HasAVX(SB), $0
    JE   2(PC)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕avx·__skip_one_fast(SB)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕sse·__skip_one_fast(SB)

TEXT ·GetByPath(SB), NOSPLIT, $0 - 32
    CMPB github·com∕bytedance∕sonic∕internal∕cpu·HasAVX2(SB), $0
    JE   2(PC)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕avx2·__get_by_path(SB)
    CMPB github·com∕bytedance∕sonic∕internal∕cpu·HasAVX(SB), $0
    JE   2(PC)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕avx·__get_by_path(SB)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕sse·__get_by_path(SB)

TEXT ·ValidateOne(SB), NOSPLIT, $0 - 32
    CMPB github·com∕bytedance∕sonic∕internal∕cpu·HasAVX2(SB), $0
    JE   2(PC)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕avx2·__validate_one(SB)
    CMPB github·com∕bytedance∕sonic∕internal∕cpu·HasAVX(SB), $0
    JE   2(PC)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕avx·__validate_one(SB)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕sse·__validate_one(SB)

TEXT ·ValidateUTF8(SB), NOSPLIT, $0 - 40
    CMPB github·com∕bytedance∕sonic∕internal∕cpu·HasAVX2(SB), $0
    JE   2(PC)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕avx2·__validate_utf8(SB)
    CMPB github·com∕bytedance∕sonic∕internal∕cpu·HasAVX(SB), $0
    JE   2(PC)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕avx·__validate_utf8(SB)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕sse·__validate_utf8(SB)

TEXT ·ValidateUTF8Fast(SB), NOSPLIT, $0 - 16
    CMPB github·com∕bytedance∕sonic∕internal∕cpu·HasAVX2(SB), $0
    JE   2(PC)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕avx2·__validate_utf8_fast(SB)
    CMPB github·com∕bytedance∕sonic∕internal∕cpu·HasAVX(SB), $0
    JE   2(PC)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕avx·__validate_utf8_fast(SB)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕sse·__validate_utf8_fast(SB)
    
TEXT ·I64toa(SB), NOSPLIT, $0 - 32
    CMPB github·com∕bytedance∕sonic∕internal∕cpu·HasAVX2(SB), $0
    JE   2(PC)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕avx2·__i64toa(SB)
    CMPB github·com∕bytedance∕sonic∕internal∕cpu·HasAVX(SB), $0
    JE   2(PC)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕avx·__i64toa(SB)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕sse·__i64toa(SB)

TEXT ·U64toa(SB), NOSPLIT, $0 - 32
    CMPB github·com∕bytedance∕sonic∕internal∕cpu·HasAVX2(SB), $0
    JE   2(PC)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕avx2·__u64toa(SB)
    CMPB github·com∕bytedance∕sonic∕internal∕cpu·HasAVX(SB), $0
    JE   2(PC)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕avx·__u64toa(SB)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕sse·__u64toa(SB)

TEXT ·F64toa(SB), NOSPLIT, $0 - 32
    CMPB github·com∕bytedance∕sonic∕internal∕cpu·HasAVX2(SB), $0
    JE   2(PC)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕avx2·__f64toa(SB)
    CMPB github·com∕bytedance∕sonic∕internal∕cpu·HasAVX(SB), $0
    JE   2(PC)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕avx·__f64toa(SB)
    JMP  github·com∕bytedance∕sonic∕internal∕native∕sse·__f64toa(SB)

