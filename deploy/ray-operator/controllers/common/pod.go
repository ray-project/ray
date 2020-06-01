package common

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	rayiov1alpha1 "ray-operator/api/v1alpha1"
	"strings"
)

type PodConfig struct {
	RayCluster  *rayiov1alpha1.RayCluster
	PodTypeName string
	PodName     string
	Extension   rayiov1alpha1.Extension
}

func DefaultPodConfig(instance *rayiov1alpha1.RayCluster, podTypeName string, podName string) *PodConfig {
	return &PodConfig{
		RayCluster:  instance,
		PodTypeName: podTypeName,
		PodName:     podName,
	}
}

func BuildPod(conf *PodConfig) *corev1.Pod {
	rayLabels := labelsForCluster(*conf.RayCluster, conf.PodName, conf.PodTypeName, conf.Extension.Labels)

	// Build the containers for the pod (there is currently only one).
	containers := []corev1.Container{buildContainer(conf)}

	spec := corev1.PodSpec{
		Volumes:            conf.Extension.Volumes,
		Containers:         containers,
		Affinity:           conf.Extension.Affinity,
		Tolerations:        conf.Extension.Tolerations,
		ServiceAccountName: conf.RayCluster.Namespace,
	}

	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        conf.PodName,
			Namespace:   conf.RayCluster.Namespace,
			Labels:      rayLabels,
			Annotations: conf.Extension.Annotations,
		},
		Spec: spec,
	}

	return pod
}

func buildContainer(conf *PodConfig) corev1.Container {
	image := conf.RayCluster.Spec.Images.DefaultImage
	if conf.Extension.Image != "" {
		image = conf.Extension.Image
	}

	// Add instance name and namespace to container env to identify cluster pods.
	// Add pod IP address to container env.
	containerEnv := append(conf.Extension.ContainerEnv,
		corev1.EnvVar{Name: namespace, Value: conf.RayCluster.Namespace},
		corev1.EnvVar{Name: clusterName, Value: conf.RayCluster.Name},
		corev1.EnvVar{Name: "MY_POD_IP", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"}}},
	)

	return corev1.Container{
		Name:            strings.ToLower(conf.PodTypeName),
		Image:           image,
		Command:         []string{"/bin/bash", "-c", "--"},
		Args:            []string{conf.Extension.Command},
		Env:             containerEnv,
		Resources:       conf.Extension.Resources,
		VolumeMounts:    conf.Extension.VolumeMounts,
		ImagePullPolicy: conf.RayCluster.Spec.ImagePullPolicy,
		Ports: []corev1.ContainerPort{
			{
				ContainerPort: int32(defaultRedisPort),
				Name:          "redis",
			},
			{
				ContainerPort: int32(defaultHTTPServerPort),
				Name:          "http-server",
			},
		},
	}
}
