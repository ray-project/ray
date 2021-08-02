package main

import (
    "github.com/ray-project/ray-go-worker/pkg/actor"
    "github.com/ray-project/ray-go-worker/pkg/ray"
)
import "fmt"

func main() {
    ray.Init("127.0.0.1:6379", "5241590000000000")
    actor_ref := ray.Actor((*actor.Count)(nil)).Remote()
    actor_ref.Task("Get")
    fmt.Println("ok!")
}

type Summable int

func (s Summable) Add(n int) int {
    return int(s) + n
}