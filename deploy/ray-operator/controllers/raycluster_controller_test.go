/*

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	rayiov1alpha1 "ray-operator/api/v1alpha1"
	"reflect"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/utils/pointer"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	// +kubebuilder:scaffold:imports
)

var _ = Context("Inside the default namespace", func() {
	ctx := context.TODO()
	SetupTest(ctx)

	Describe("When creating a raycluster", func() {

		It("should create a raycluster object", func() {
			myRayCluster := &rayiov1alpha1.RayCluster{
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
						},
						rayiov1alpha1.Extension{
							Replicas:  pointer.Int32Ptr(1),
							Type:      rayiov1alpha1.ReplicaTypeHead,
							GroupName: "headgroup",
						},
					},
				},
			}

			err := K8sClient.Create(ctx, myRayCluster)
			Expect(err).NotTo(HaveOccurred(), "failed to create test RayCluster resource")

		})

		It("should create a new worker pod resource", func() {
			pod := &corev1.Pod{}
			Eventually(
				getResourceFunc(ctx, client.ObjectKey{Name: "raycluster-sample-small-group-worker-0", Namespace: "default"}, pod),
				time.Second*15, time.Millisecond*500).Should(BeNil(), "My pod = %v", pod)
			Expect(pod.Status.Phase).Should((Or(Equal(v1.PodRunning), Equal(v1.PodPending))))
		})

		It("should create more than 1 worker", func() {
			var podList corev1.PodList
			K8sClient.List(context.Background(), &podList, &client.ListOptions{Namespace: "default"})
			Expect(len(podList.Items)).Should(BeNumerically(">=", 3))
		})

		It("should re-create a deleted worker", func() {
			var podList corev1.PodList
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "raycluster-sample-small-group-worker-0",
					Namespace: "default",
				},
			}
			err := K8sClient.Delete(context.Background(), pod,
				&client.DeleteOptions{GracePeriodSeconds: pointer.Int64Ptr(0)})

			Expect(err).NotTo(HaveOccurred(), "failed delete a pod")

			Eventually(
				listResourceFunc(context.Background(), &podList, &client.ListOptions{Namespace: "default"}),
				time.Second*15, time.Millisecond*500).Should(BeNil(), "My pod list= %v", podList)

			//at least 3 pods should be in none-failed phase
			count := 0
			for _, aPod := range podList.Items {
				if reflect.DeepEqual(aPod.Status.Phase, v1.PodRunning) || reflect.DeepEqual(aPod.Status.Phase, v1.PodPending) {
					count++
				}
			}
			Expect(count).Should(BeNumerically(">=", 3))

		})
	})
})

func getResourceFunc(ctx context.Context, key client.ObjectKey, obj runtime.Object) func() error {
	return func() error {
		return K8sClient.Get(ctx, key, obj)
	}
}

func listResourceFunc(ctx context.Context, list runtime.Object, opt client.ListOption) func() error {
	return func() error {
		return K8sClient.List(ctx, list, opt)
	}
}
