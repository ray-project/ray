package ray

import (
    "flag"

    common "github.com/ray-project/ray-go-worker/pkg/ray/generated/common"
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

    innerInit(GetAddress(), GetPassword(), common.WorkerType_WORKER)
    innerRun()
}
