package main

import (
    "time"

    "github.com/ray-project/ray-go-worker/pkg/actor"
    "github.com/ray-project/ray-go-worker/pkg/ray"
    "github.com/ray-project/ray-go-worker/pkg/util"
)
import "fmt"

func main() {
    ray.Init("127.0.0.1:6379", "5241590000000000")
    util.Logger.Infof("finish init")
    actor_ref := ray.Actor((*actor.Count)(nil)).Remote()
    if actor_ref == nil {
        util.Logger.Infof("failed to create actor ref")
    }
    util.Logger.Infof("created actor ref")
    //var f ray.Convert = actor.Count.Increase
    //reflect.ValueOf(actor.Count)
    // reflect.ValueOf(actor.Count.Get)
    actor_ref.Task((*actor.Count).Get).Remote().Get()
    fmt.Println("ok!")
    time.Sleep(time.Minute * 5)
}

type Summable int

func (s Summable) Add(n int) int {
    return int(s) + n
}
