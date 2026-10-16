package plugin

import (
	"context"
	"fmt"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/proto"
)

// GCSHandshakeClient is the minimal GCS surface the driver handshake needs.
// Declared as an interface (not gcs.Client / api.GCSClient) so both the plugin
// Initialize (api.GCSClient) and the dashboard head (gcs.Client) share it.
type GCSHandshakeClient interface {
	GetNodeToConnect(ctx context.Context, nodeIpAddress string) (*proto.GcsNodeInfo, error)
	NextJobID(ctx context.Context) (ids.JobID, error)
}

// DriverHandshake fills the Driver-mode initialization fields that require GCS
// lookups: node connection info (plasma store socket, raylet socket, node
// manager port, node IP) and, when no JobID was provided, a fresh JobID
// allocated from GCS. It is the single source of truth shared by the plugin's
// exported Initialize and the dashboard head's pre-handshake, so the fill
// rules cannot drift between the two callers.
//
// Fill semantics:
//   - Sockets / node manager port: filled only when the caller left them empty
//     (explicit caller config wins).
//   - NodeIPAddress: always overridden when the node's registered address is
//     non-empty, so the driver connects to the IP the raylet registered with
//     (this mirrors the node-IP alignment fix; the address from GCS is
//     authoritative over auto-detection).
//   - JobID: allocated from GCS only when the caller did not provide one.
//
// A failed node-info fetch is logged and tolerated: base.Initialize's relaxed
// Driver-mode validation lets C++ core worker surface the real connectivity
// error. A failed JobID allocation is fatal, because the driver cannot proceed
// without a JobID.
func DriverHandshake(ctx context.Context, client GCSHandshakeClient, opts *options.InitializeOptions) error {
	if client == nil {
		return fmt.Errorf("driver handshake: nil gcs client")
	}
	if opts == nil {
		return fmt.Errorf("driver handshake: nil options")
	}

	// Fetch node info only when a connection field is still missing.
	if opts.Runtime.StoreSocket == "" || opts.Runtime.RayletSocket == "" ||
		opts.Network.NodeManagerPort == 0 || opts.Network.NodeIPAddress == "" {
		nodeInfo, err := client.GetNodeToConnect(ctx, opts.Network.NodeIPAddress)
		if err != nil {
			log.Log.Error(err, "driver handshake: failed to fetch node info from GCS")
		} else if nodeInfo != nil {
			if opts.Runtime.StoreSocket == "" && nodeInfo.ObjectStoreSocketName != "" {
				opts.Runtime.StoreSocket = nodeInfo.ObjectStoreSocketName
			}
			if opts.Runtime.RayletSocket == "" && nodeInfo.RayletSocketName != "" {
				opts.Runtime.RayletSocket = nodeInfo.RayletSocketName
			}
			if opts.Network.NodeManagerPort == 0 && nodeInfo.NodeManagerPort != 0 {
				opts.Network.NodeManagerPort = int32(nodeInfo.NodeManagerPort)
			}
			// GCS node address is authoritative over the caller's value.
			if nodeInfo.NodeManagerAddress != "" {
				opts.Network.NodeIPAddress = nodeInfo.NodeManagerAddress
			}
		}
	}

	// Allocate a fresh JobID when the caller did not provide one.
	if opts.Job.JobID == "" {
		jobID, err := client.NextJobID(ctx)
		if err != nil {
			return fmt.Errorf("driver handshake: failed to get next JobID from GCS: %w", err)
		}
		opts.Job.JobID = jobID.Hex()
	}
	return nil
}
