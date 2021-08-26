package ray

/*
   #cgo CFLAGS: -I${SRCDIR}/../../../src/ray/core_worker/lib/golang
   #cgo LDFLAGS: -lcore_worker_library_go
   #cgo linux,amd64 LDFLAGS: -Wl,-rpath,${SRCDIR}/packaged/lib/linux-amd64 -L${SRCDIR}/packaged/lib/linux-amd64
   #cgo darwin,amd64 LDFLAGS: -Wl,-rpath,${SRCDIR}/packaged/lib/darwin-amd64 -L${SRCDIR}/packaged/lib/darwin-amd64
   #include <string.h>
   #include "go_worker.h"
*/
import "C"

import (
    _ "github.com/ray-project/ray-go-worker/pkg/ray/packaged/lib"
)