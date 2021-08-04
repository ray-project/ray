package main

import (
    "flag"

    _ "github.com/ray-project/ray-go-worker/pkg/actor"
    "github.com/ray-project/ray-go-worker/pkg/ray"
    "github.com/ray-project/ray-go-worker/pkg/ray/generated"
)

func main() {
    nodeManagerPort := flag.Int("node-manager-port", 0, "")
    nodeManagerAddress := flag.String("node-manager-address", "", "")
    objectStoreSocket := flag.String("object-store-socket-name", "", "")
    rayletSocket := flag.String("raylet-socket-name", "", "")
    sessionDir := flag.String("session-dir", "", "")
    redisAddress := flag.String("redis-address", "", "")
    redisPassword := flag.String("redis-password", "", "")

    flag.Parse()
    ray.SetAddress(*redisAddress)
    ray.SetPassword(*redisPassword)
    ray.SetNodeManagerAddress(*nodeManagerAddress)
    ray.SetNodeManagerPort(int32(*nodeManagerPort))
    ray.SetObjectStoreSocket(*objectStoreSocket)
    ray.SetRayletSocket(*rayletSocket)
    ray.SetSessionDir(*sessionDir)

    ray.InnerInit(ray.GetAddress(), ray.GetPassword(), generated.WorkerType_WORKER)
    ray.Run()
}
