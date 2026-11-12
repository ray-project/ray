package reporter

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
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
// Every dial presents the cluster's credentials resolved from the environment,
// as the C++ BuildChannel does: under RAY_AUTH_MODE=token the raylet rejects
// unauthenticated requests (src/ray/rpc/server_call.h token enforcement, Bearer
// header format from src/ray/rpc/authentication/authentication_token.h), so a
// bare dial would degrade the worker list to empty on every collection cycle.
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

		// Resolved per dial, matching the C++ BuildChannel: every client
		// channel presents the cluster's credentials under token auth, and a
		// per-dial resolution picks up a rotated projected token.
		dialOpts, err := gcs.DialOptionsFromEnv()
		if err != nil {
			log.Log.Error(err, "failed to resolve raylet dial options", "address", address)
			return nil, err
		}
		dialOpts.ConnectTimeoutMs = rayletWorkerPIDsTimeout.Milliseconds()
		conn, err := gcs.DialGCS(ctx, address, dialOpts)
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
