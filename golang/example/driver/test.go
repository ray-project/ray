package main

import (
    "github.com/ray-project/ray-go-worker/example/actor"
    "github.com/ray-project/ray-go-worker/pkg/ray"
    "github.com/ray-project/ray-go-worker/pkg/util"
)

func main() {
    ray.Init("127.0.0.1:6379", "5241590000000000")
    util.Logger.Infof("finished init ray")
    actor_ref, err := ray.Actor((*actor.Counter)(nil)).Remote()
    if err != nil {
        panic(err)
    }
    util.Logger.Infof("created actor ref")

    _, err = actor_ref.Task((*actor.Counter).Increase1).Remote().Get()
    if err != nil {
        panic(err)
    }
    util.Logger.Infof("invoked increase")

    values, err := actor_ref.Task((*actor.Counter).Get).Remote().Get()
    if err != nil {
        panic(err)
    }
    util.Logger.Infof("invoked get")
    getValue, ok := values[0].(*int)
    if !ok {
        util.Logger.Infof("remote get failed")
    } else {
        util.Logger.Infof("remote get result:%d", *getValue)
    }

    values, err = actor_ref.Task((*actor.Counter).Hello).Remote().Get()
    if err != nil {
        panic(err)
    }
    hello, ok := values[0].(*string)
    if !ok {
        util.Logger.Infof("failed to get string")
    } else {
        util.Logger.Infof("get string:%s", *hello)
    }
    ray.Shutdown()
}
