package base64x

import (
    `fmt`
    `os`

    `github.com/klauspost/cpuid/v2`
)

func hasAVX2() bool {
    switch v := os.Getenv("B64X_MODE"); v {
        case ""       : fallthrough
        case "auto"   : return cpuid.CPU.Has(cpuid.AVX2)
        case "noavx2" : return false
        default       : panic(fmt.Sprintf("invalid mode: '%s', should be one of 'auto', 'noavx2'", v))
    }
}