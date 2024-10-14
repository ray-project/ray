//go:generate make
package base64x

import (
    `unsafe`
)

//go:nosplit
//go:noescape
//goland:noinspection GoUnusedParameter
func __b64encode(out *[]byte, src *[]byte, mode int)

//go:nosplit
//go:noescape
//goland:noinspection GoUnusedParameter
func __b64decode(out *[]byte, src unsafe.Pointer, len int, mode int) (ret int)
