package common

const (
	// Head used as pod type to decide create service or not, for now only create service for head.
	Head = "head"

	// Belows used as label key
	//rayclusterComponent is the pod name for this pod for selecting pod by pod name.
	rayclusterComponent = "raycluster.component"
	// rayIoComponent is the identifier for created by ray-operator for selecting pod by operator name.
	rayIoComponent = "rayclusters.ray.io/component-name"
	// RayClusterOwnerKey is the ray cluster instance name for selecting pod by instance name.
	RayClusterOwnerKey = "raycluster.instance.name"
	// ClusterPodType is the pod type label key for selecting pod by type.
	ClusterPodType = "raycluster.pod.type"

	// rayOperator is the value of ray-operator used as identifier for the pod
	rayOperator = "ray-operator"

	// Use as separator for pod name, for example, raycluster-small-size-worker-0
	DashSymbol = "-"

	// Use as default port
	defaultHTTPServerPort = 30021
	defaultRedisPort      = 6379

	// Check node if ready by checking the path exists or not
	PodReadyFilepath = "POD_READY_FILEPATH"

	// Use as container env variable
	namespace   = "NAMESPACE"
	clusterName = "CLUSTER_NAME"
)
