package common

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	rayiov1alpha1 "ray-operator/api/v1alpha1"
	"ray-operator/utils"
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

func ServiceForPod(conf *ServiceConfig) *corev1.Service {
	name := conf.PodName
	if strings.Contains(conf.PodName, Head) {
		name = utils.Before(conf.PodName, Head) + "head"
	}

	svc := &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "Service",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: conf.RayCluster.Namespace,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			// select this raycluster's component
			Selector: map[string]string{
				rayclusterComponent: conf.PodName,
			},
		},
	}

	return svc
}
