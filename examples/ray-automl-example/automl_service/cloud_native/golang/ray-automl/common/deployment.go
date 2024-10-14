package common

import (
	"github.com/go-logr/logr"
	automlv1 "github.com/ray-automl/apis/automl/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"strings"
)

func NewDeployment(object interface{}, log logr.Logger) *appsv1.Deployment {
	deployment := &appsv1.Deployment{}
	switch object.(type) {
	case *automlv1.Trainer:
		deployment = NewDeploymentInstanceTrainer(object.(*automlv1.Trainer))
		log.Info("create deployment for NewDeploymentInstanceTrainer", "deployment", deployment)
	case *automlv1.Proxy:
		deployment = NewDeploymentInstanceProxy(object.(*automlv1.Proxy))
		log.Info("create deployment for NewDeploymentInstanceProxy", "deployment", deployment)
	}
	return deployment
}

func GetMainContainer(podSpec corev1.PodTemplateSpec, containerName string) (int, *corev1.Container) {
	for index, container := range podSpec.Spec.Containers {
		if strings.EqualFold(container.Name, containerName) {
			return index, container.DeepCopy()
		}
	}
	return -1, nil
}
