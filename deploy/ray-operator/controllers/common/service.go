package common

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	rayiov1alpha1 "ray-operator/api/v1alpha1"
	"ray-operator/controllers/utils"
	"strings"
)

type ServiceConfig struct {
	RayCluster rayiov1alpha1.RayCluster
	PodName    string
}

func DefaultServiceConfig(instance rayiov1alpha1.RayCluster, podName string) *ServiceConfig {
	return &ServiceConfig{
		RayCluster: instance,
		PodName:    podName,
	}
}

// Build service for pod, for now only head pod will have service.
func ServiceForPod(conf *ServiceConfig) *corev1.Service {
	name := conf.PodName
	if strings.Contains(conf.PodName, Head) {
		name = utils.Before(conf.PodName, Head) + "head"
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: conf.RayCluster.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{Name: "redis", Port: int32(defaultRedisPort)}},
			ClusterIP: "None",
			// select this raycluster's component
			Selector: map[string]string{
				rayclusterComponent: conf.PodName,
			},
		},
	}

	return svc
}
