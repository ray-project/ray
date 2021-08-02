package main

import "github.com/ray-project/ray-go-worker/pkg/ray"
import "fmt"

func main() {
    ray.Init("127.0.0.1:6379","5241590000000000")
    ActorHandle<Counter> actor = ray.Actor(Counter, 1).remote();
    fmt.Println("ok!")
}
