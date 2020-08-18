package common

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var svcConf = &ServiceConfig{
	RayCluster: *createSomeRayCluster(),
	PodName:    "raycluster-sample-head",
}

func TestServiceForPod(t *testing.T) {

	actualResult := ServiceForPod(svcConf)
	expectedResult := createSomeService()

	if !reflect.DeepEqual(expectedResult.ObjectMeta.Name, actualResult.ObjectMeta.Name) {
		t.Fatalf("Expected %v but got %v", expectedResult.ObjectMeta.Name, actualResult.ObjectMeta.Name)
	}

}

func createSomeService() (svc *corev1.Service) {

	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Service",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "raycluster-sample-head",
			Namespace: "default",
		},
	}
}
