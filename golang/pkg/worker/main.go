package main

import (
    _ "github.com/ray-project/ray-go-worker/pkg/actor"
    "github.com/ray-project/ray-go-worker/pkg/ray"
)

func main() {
    ray.Run()
}
