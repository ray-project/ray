package main

import (
    _ "github.com/ray-project/ray-go-worker/pkg/actor"
    "github.com/ray-project/ray-go-worker/pkg/ray/worker"
)

func main() {
    worker.Run()
}
