package common

import (
	rayiov1alpha1 "ray-operator/api/v1alpha1"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
)

func TestBuildPod(t *testing.T) {

	actualResult := BuildPod(createSomePodConfig())
	expectedResult := CreateSomePod()

	if !reflect.DeepEqual(expectedResult.ObjectMeta.Name, actualResult.ObjectMeta.Name) {
		t.Fatalf("Expected %v but got %v", expectedResult.ObjectMeta.Name, actualResult.ObjectMeta.Name)
	}
	//TODO add more checks for the expected pod
}

func CreateSomePod() (pod *corev1.Pod) {

	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "raycluster-sample-small-group-worker-0",
			Namespace: "default",
		},
	}
}

func createSomePodConfig() (conf *PodConfig) {
	return &PodConfig{
		RayCluster:  createSomeRayCluster(),
		PodTypeName: "worker",
		PodName:     "raycluster-sample-small-group-worker-0",
		Extension:   createSomeExtension(),
	}
}

func createSomeRayCluster() (rc *rayiov1alpha1.RayCluster) {
	return &rayiov1alpha1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "raycluster-sample",
			Namespace: "default",
		},
		Spec: rayiov1alpha1.RayClusterSpec{
			ClusterName: "raycluster-sample",
			Images: rayiov1alpha1.RayClusterImage{
				DefaultImage: "rayproject/autoscaler",
			},
			Extensions: []rayiov1alpha1.Extension{
				rayiov1alpha1.Extension{
					Replicas:  pointer.Int32Ptr(3),
					Type:      rayiov1alpha1.ReplicaTypeWorker,
					GroupName: "small-group",
					Command:   "ray start --block --node-ip-address=$MY_POD_IP --address=$CLUSTER_NAME-head:6379 --object-manager-port=12345 --node-manager-port=12346 --object-store-memory=100000000 --num-cpus=1",
				},
				rayiov1alpha1.Extension{
					Replicas:  pointer.Int32Ptr(1),
					Type:      rayiov1alpha1.ReplicaTypeHead,
					GroupName: "headgroup",
					Command:   "ray start --head --block --redis-port=6379 --node-ip-address=$MY_POD_IP --object-manager-port=12345 --node-manager-port=12346 --object-store-memory=100000000 --num-cpus=1",
				},
			},
		},
	}

}

func createSomeExtension() (ext rayiov1alpha1.Extension) {
	return rayiov1alpha1.Extension{
		Replicas:  pointer.Int32Ptr(3),
		Type:      rayiov1alpha1.ReplicaTypeWorker,
		GroupName: "small-group",
	}
}
