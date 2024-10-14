package common

const (
	OperatorAddress = "operator-address"
	OperatorPort    = "7070"
	OperatorName    = "ray-automl-controller"

	ProxyLabelSelector         = "ray.automl.proxy.name"
	TrainerLabelSelector       = "ray.automl.trainer.name"
	TrainerWorkerLabelSelector = "ray.automl.trainer.worker.group"

	Image          = "IMAGE"
	Namespace      = "NAMESPACE"
	Replicas       = "replicas"
	Cpu            = "cpu"
	Memory         = "memory"
	Disk           = "disk"
	GpuCard        = "gpuCard"
	GpuCardDefault = "nvidia.com/gpu"
	Gpu            = "gpu"
	DASH           = "-"
)
