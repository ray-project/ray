package main

import "github.com/ray-project/ray-go-worker/pkg/ray"

func main() {
    ray.Init("127.0.0.1:6379","5241590000000000")
}
