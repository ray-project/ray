package common

const (
	// use as pod type decision
	Head = "head"

	// use as label key
	rayclusterComponent = "raycluster.component"
	rayIoComponent      = "rayclusters.ray.io/component-name"
	rayOperator         = "ray-operator"
	RayClusterOwnerKey  = "raycluster.instance.name"
	ClusterPodType      = "raycluster.pod.type"

	// use as separator
	DashSymbol = "-"

	// use as default port
	defaultHTTPServerPort = "30021"
	defaultRedisPort      = "6379"

	// check node ready
	PodReadyFilepath = "POD_READY_FILEPATH"

	// use as container env variable
	namespace   = "NAMESPACE"
	clusterName = "CLUSTER_NAME"
)
