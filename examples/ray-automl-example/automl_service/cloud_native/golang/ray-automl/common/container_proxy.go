package common

import (
	"fmt"
	automlv1 "github.com/ray-automl/apis/automl/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"strconv"
)

func NewDeploymentInstanceProxy(instance *automlv1.Proxy) *appsv1.Deployment {
	BuildProxyContainer(instance)
	deployment := &appsv1.Deployment{}
	deployment.Name = instance.Name
	deployment.Namespace = instance.Namespace
	deployment.Spec = instance.Spec.DeploySpec
	return deployment
}

func BuildProxyContainer(instance *automlv1.Proxy) {
	if instance.Spec.StartParams != nil {

		operatorAddress := fmt.Sprintf("%s.%s.svc.%s:%v",
			OperatorName,
			instance.Namespace,
			GetClusterDomainName(),
			OperatorPort)

		instance.Spec.StartParams[OperatorAddress] = operatorAddress
		if instance.Spec.StartParams[automlv1.ProxyGrpcPortNumber] == "" {
			instance.Spec.StartParams[automlv1.ProxyGrpcPortNumber] = strconv.FormatInt(int64(automlv1.ProxyContainerPortNumberDefault), 10)
		}
	}
	index, container := GetMainContainer(instance.Spec.DeploySpec.Template, automlv1.ProxyContainerName)
	if index < 0 {
		instance.Spec.DeploySpec.Template.Spec.Containers = append(instance.Spec.DeploySpec.Template.Spec.Containers, corev1.Container{Name: automlv1.ProxyContainerName})
	} else {
		paramMap := ConvertParamMap(instance.Spec.StartParams)
		container.Command = []string{"bash", "-c"}
		container.Args = []string{"python -m automl.proxy " + paramMap}
		container.Env = append(container.Env,
			corev1.EnvVar{
				Name: "MY_POD_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.name",
					},
				},
			}, corev1.EnvVar{
				Name: "MY_POD_IP",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "status.podIP",
					},
				},
			},
		)
		cpu := GetOrDefault(instance.Spec.StartParams, Cpu, "1")
		memory := GetOrDefault(instance.Spec.StartParams, Memory, "1Gi")
		disk := GetOrDefault(instance.Spec.StartParams, Disk, "1Gi")
		container.Resources.Limits = BuildResourceList(cpu, memory, disk)
		container.Resources.Requests = BuildResourceList(cpu, memory, disk)
		instance.Spec.DeploySpec.Template.Spec.Containers[index] = *container
	}
	instance.Spec.DeploySpec.Template.Labels = instance.Labels
	instance.Spec.DeploySpec.Template.Annotations = instance.Annotations
	instance.Spec.DeploySpec.Selector.MatchLabels = instance.Labels
}
