package reporter

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// rayletWorkerPIDsTimeout bounds the GetWorkerPIDs call, mirroring the Python
// RayletClient.async_get_worker_pids default timeout of 1000ms.
const rayletWorkerPIDsTimeout = time.Second

// workerPIDFetcher returns the pids of the live worker and driver processes
// registered with the raylet, mirroring Python RayletClient.async_get_worker_pids.
// It abstracts the raylet gRPC call so worker discovery can be unit tested
// against a fake instead of a real raylet.
type workerPIDFetcher func() ([]int32, error)

// newRayletWorkerPIDFetcher builds a fetcher that queries the node manager gRPC
// service on nodeIP:port. Each call dials a short-lived connection, matching the
// head-side fetchNodeStats choice: the node manager is local, so a stub cache
// would add lifecycle complexity (a connection to close) for no benefit.
//
// A dial failure, an RPC failure or a timeout is logged and returned as an
// error; the caller degrades the worker list to empty rather than failing the
// collection cycle, matching the Python agent which catches
// (GetTimeoutError, RpcError) and returns [].
func newRayletWorkerPIDFetcher(nodeIP string, port int) workerPIDFetcher {
	address := net.JoinHostPort(nodeIP, strconv.Itoa(port))
	return func() ([]int32, error) {
		ctx, cancel := context.WithTimeout(context.Background(), rayletWorkerPIDsTimeout)
		defer cancel()

		conn, err := grpc.DialContext(ctx, address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock())
		if err != nil {
			log.Log.Error(err, "failed to get worker pids from raylet", "address", address)
			return nil, err
		}
		defer conn.Close()

		reply, err := proto.NewNodeManagerServiceClient(conn).GetWorkerPIDs(ctx, &proto.GetWorkerPIDsRequest{})
		if err != nil {
			log.Log.Error(err, "failed to get worker pids from raylet", "address", address)
			return nil, err
		}
		return reply.GetPids(), nil
	}
}
