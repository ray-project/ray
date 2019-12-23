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

// Build a pod for the cluster instance.
func BuildPod(conf *PodConfig) *corev1.Pod {
	// build label for cluster
	rayLabels := labelsForCluster(*conf.RayCluster, conf.PodName, conf.PodTypeName, conf.Extension.Labels)

	// build container for pod, now only handle one container for each pod
	var containers []corev1.Container
	container := buildContainer(conf)
	containers = append(containers, container)

	// create volume
	volumes := conf.Extension.Volumes

	spec := corev1.PodSpec{
		Volumes:            volumes,
		Containers:         containers,
		Affinity:           conf.Extension.Affinity,
		Tolerations:        conf.Extension.Tolerations,
		ServiceAccountName: conf.RayCluster.Namespace,
	}

	// build annotations and store podCompareHash for comparison
	annotations := conf.Extension.Annotations

	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        conf.PodName,
			Namespace:   conf.RayCluster.Namespace,
			Labels:      rayLabels,
			Annotations: annotations,
		},
		Spec: spec,
	}

	return pod
}

// Build container for pod.
func buildContainer(conf *PodConfig) corev1.Container {

	redisPort := defaultRedisPort
	httpServerPort := defaultHTTPServerPort
	jobManagerPort := defaultRedisPort

	// get pod file path to check if the pod container ready or not
	var podReadyFilepath string
	for _, env := range conf.Extension.ContainerEnv {
		if strings.EqualFold(env.Name, PodReadyFilepath) {
			podReadyFilepath = env.Value
			break
		}
	}

	// assign image by typeName
	image := conf.RayCluster.Spec.Images.DefaultImage
	if conf.Extension.Image != "" {
		image = conf.Extension.Image
	}

	volumeMounts := conf.Extension.VolumeMounts

	// add instance name and namespace to container env to identify cluster pods
	var containerEnv []corev1.EnvVar
	containerEnv = conf.Extension.ContainerEnv
	containerEnv = append(containerEnv,
		corev1.EnvVar{Name: namespace, Value: conf.RayCluster.Namespace},
		corev1.EnvVar{Name: clusterName, Value: conf.RayCluster.Name})

	container := corev1.Container{
		Name:            strings.ToLower(conf.PodTypeName),
		Image:           image,
		Command:         []string{"/bin/bash", "-c", "--"},
		Args:            []string{conf.Extension.Command},
		Env:             containerEnv,
		Resources:       conf.Extension.Resources,
		VolumeMounts:    volumeMounts,
		ImagePullPolicy: conf.RayCluster.Spec.ImagePullPolicy,
		Ports: []corev1.ContainerPort{
			{
				ContainerPort: int32(redisPort),
				Name:          "redis",
			},
			{
				ContainerPort: int32(httpServerPort),
				Name:          "http-server",
			},
			{
				ContainerPort: int32(jobManagerPort),
				Name:          "job-manager",
			},
		},
		ReadinessProbe: &corev1.Probe{
			Handler: corev1.Handler{
				Exec: &corev1.ExecAction{Command: []string{"cat", podReadyFilepath}},
			},
			InitialDelaySeconds: 15,
			SuccessThreshold:    2,
		},
	}
	return container
}
