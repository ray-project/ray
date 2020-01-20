package common

import rayiov1alpha1 "ray-operator/api/v1alpha1"

// LabelsForCluster returns the labels for selecting the resources
// belonging to the given RayCluster CR name.
func LabelsForCluster(instance rayiov1alpha1.RayCluster, name string, podTypeName string, extend map[string]string) (ret map[string]string) {
	ret = map[string]string{
		RayclusterComponent: name,
		RayIoComponent:      rayOperator,
		RayClusterOwnerKey:  instance.Name,
		ClusterPodType:      podTypeName,
	}
	for k, v := range extend {
		ret[k] = v
	}
	return
}
