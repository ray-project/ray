package ray

import (
    "flag"

    "github.com/ray-project/ray-go-worker/pkg/ray/generated"
)

func Run() {
    nodeManagerPort := flag.Int("node-manager-port", 0, "")
    nodeManagerAddress := flag.String("node-manager-address", "", "")
    objectStoreSocket := flag.String("object-store-socket-name", "", "")
    rayletSocket := flag.String("raylet-socket-name", "", "")
    sessionDir := flag.String("session-dir", "", "")
    redisAddress := flag.String("redis-address", "", "")
    redisPassword := flag.String("redis-password", "", "")

    flag.Parse()
    SetAddress(*redisAddress)
    SetPassword(*redisPassword)
    SetNodeManagerAddress(*nodeManagerAddress)
    SetNodeManagerPort(int32(*nodeManagerPort))
    SetObjectStoreSocket(*objectStoreSocket)
    SetRayletSocket(*rayletSocket)
    SetSessionDir(*sessionDir)

    innerInit(GetAddress(), GetPassword(), generated.WorkerType_WORKER)
    innerRun()
}
