package common

import rayiov1alpha1 "ray-operator/api/v1alpha1"

// The function labelsForCluster returns the labels for selecting the resources
// belonging to the given RayCluster CR name.
// NOTE: be careful about this label generator! any typo will cause deploy failure.
func labelsForCluster(instance rayiov1alpha1.RayCluster, name string, extend map[string]string) (ret map[string]string) {
	ret = map[string]string{
		rayclusterComponent: name,
		rayIoComponent:      rayOperator,
		RayClusterOwnerKey:  instance.Name,
	}
	for k, v := range extend {
		ret[k] = v
	}
	return
}
