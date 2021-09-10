package main

import (
    _ "github.com/ray-project/ray-go-worker/example/actor"
    "github.com/ray-project/ray-go-worker/pkg/ray"
)

func main() {
    ray.Run()
}
