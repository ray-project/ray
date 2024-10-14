package base64x

import (
    `reflect`
    `unsafe`
)

func mem2str(v []byte) (s string) {
    (*reflect.StringHeader)(unsafe.Pointer(&s)).Len  = (*reflect.SliceHeader)(unsafe.Pointer(&v)).Len
    (*reflect.StringHeader)(unsafe.Pointer(&s)).Data = (*reflect.SliceHeader)(unsafe.Pointer(&v)).Data
    return
}

func str2mem(s string) (v []byte) {
    (*reflect.SliceHeader)(unsafe.Pointer(&v)).Cap  = (*reflect.StringHeader)(unsafe.Pointer(&s)).Len
    (*reflect.SliceHeader)(unsafe.Pointer(&v)).Len  = (*reflect.StringHeader)(unsafe.Pointer(&s)).Len
    (*reflect.SliceHeader)(unsafe.Pointer(&v)).Data = (*reflect.StringHeader)(unsafe.Pointer(&s)).Data
    return
}

func mem2addr(v []byte) unsafe.Pointer {
    return *(*unsafe.Pointer)(unsafe.Pointer(&v))
}
