package common

import (
	"github.com/go-logr/logr"
	automlv1 "github.com/ray-automl/apis/automl/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"strconv"
)

func NewService(object interface{}, log logr.Logger) *corev1.Service {
	service := &corev1.Service{}
	switch object.(type) {
	case *automlv1.Trainer:
		service = NewServiceInstanceTrainer(object.(*automlv1.Trainer), log)
		log.Info("create service for NewServiceInstanceTrainer", "service", service)
	case *automlv1.Proxy:
		service = NewServiceInstanceProxy(object.(*automlv1.Proxy), log)
		log.Info("create service for NewServiceInstanceProxy", "service", service)
	}
	return service
}

func NewServiceInstanceProxy(instance *automlv1.Proxy, log logr.Logger) *corev1.Service {
	port := GetServicePort(instance.Spec.StartParams, automlv1.ProxyGrpcPortNumber, automlv1.ProxyContainerPortNumberDefault)

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        GenerateServiceName(instance.Name),
			Namespace:   instance.Namespace,
			Labels:      instance.Labels,
			Annotations: instance.Annotations,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  instance.Spec.DeploySpec.Selector.MatchLabels,
			Ports: []corev1.ServicePort{
				{
					Name:     automlv1.ProxyContainerName,
					Port:     port,
					Protocol: corev1.ProtocolTCP,
					TargetPort: intstr.IntOrString{
						Type:   intstr.Int,
						IntVal: port,
					},
				},
			},
		},
	}

	return svc
}

func GetServicePort(params map[string]string, servicePortName string, defaultPort int32) int32 {
	if portStr, contains := params[servicePortName]; contains {
		port, err := strconv.ParseInt(portStr, 10, 32)
		if err == nil {
			return int32(port)
		}
	}
	return defaultPort
}

func NewServiceInstanceTrainer(instance *automlv1.Trainer, log logr.Logger) *corev1.Service {
	port := GetServicePort(instance.Spec.StartParams, automlv1.TrainerContainerPortNumber, automlv1.TrainerContainerPortNumberDefault)

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        GenerateServiceName(instance.Name),
			Namespace:   instance.Namespace,
			Labels:      instance.Labels,
			Annotations: instance.Annotations,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  instance.Spec.DeploySpec.Selector.MatchLabels,
			Ports: []corev1.ServicePort{
				{
					Name:     automlv1.TrainerContainerName,
					Port:     port,
					Protocol: corev1.ProtocolTCP,
					TargetPort: intstr.IntOrString{
						Type:   intstr.Int,
						IntVal: port,
					},
				},
			},
		},
	}

	return svc
}
