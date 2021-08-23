package main

import (
    "github.com/ray-project/ray-go-worker/pkg/actor"
    "github.com/ray-project/ray-go-worker/pkg/ray"
    "github.com/ray-project/ray-go-worker/pkg/util"
)

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
    _ = actor_ref.Task((*actor.Count).Increase1).Remote().Get()
    util.Logger.Infof("invoke increase")
    values := actor_ref.Task((*actor.Count).Get).Remote().Get()
    util.Logger.Infof("invoke get")
    getValue, ok := values[0].(*int)
    if !ok {
        util.Logger.Infof("remote get failed")
    } else {
        util.Logger.Infof("remote get result:%d", *getValue)
    }

    values = actor_ref.Task((*actor.Count).Hello).Remote().Get()

    hello, ok := values[0].(*string)
    if !ok {
        util.Logger.Infof("failed to get string")
    } else {
        util.Logger.Infof("get string:%s", *hello)
    }
}
