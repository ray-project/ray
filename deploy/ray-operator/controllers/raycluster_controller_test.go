package controllers

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilrand "k8s.io/apimachinery/pkg/util/rand"
	"sigs.k8s.io/controller-runtime/pkg/client"

	// +kubebuilder:scaffold:imports
	rayv1alpha1 "ray-operator/api/v1alpha1"
	"ray-operator/controllers/common"
)

var _ = Describe("RayCluster Controller", func() {
	SetDefaultEventuallyTimeout(time.Second * 60)
	SetDefaultEventuallyPollingInterval(time.Second * 1)

	rayClusterName := "raycluster-test"

	BeforeEach(func() {
		rayClusterName = fmt.Sprintf("raycluster-test-%s", utilrand.String(8))
		// failed test runs that don't clean up leave resources behind.
		rayCluster := &rayv1alpha1.RayCluster{}
		if err := k8sClient.DeleteAllOf(context.Background(), rayCluster, client.InNamespace("default")); err != nil {
			panic(err.Error())
		}
	})

	AfterEach(func() {
		// Add any teardown steps that needs to be executed after each test
	})

	Context("RayCluster", func() {
		It("Should be created correctly", func() {
			key := types.NamespacedName{
				Name:      rayClusterName,
				Namespace: "default",
			}

			defaultImage := "default-docker-image"
			clusterName := "test"
			workerGroupName := "group-worker"
			headerGroupName := "group-header"
			var defaultReplicas int32 = 1

			toCreate := &rayv1alpha1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      key.Name,
					Namespace: key.Namespace,
				},
				Spec: rayv1alpha1.RayClusterSpec{
					ClusterName: clusterName,
					Images: rayv1alpha1.RayClusterImage{
						DefaultImage: defaultImage,
					},
					Extensions: []rayv1alpha1.Extension{
						{
							Type:      rayv1alpha1.ReplicaTypeWorker,
							GroupName: workerGroupName,
							Replicas:  &defaultReplicas,
						},
						{
							Type:      rayv1alpha1.ReplicaTypeHead,
							GroupName: headerGroupName,
							Replicas:  &defaultReplicas,
						},
					},
				},
			}
			Expect(k8sClient.Create(context.TODO(), toCreate)).Should(Succeed())

			By("Expected to get two pods")
			Eventually(func() int {
				pods := &corev1.PodList{}
				if err := k8sClient.List(context.TODO(), pods,
					client.InNamespace(key.Namespace),
					client.MatchingLabels{
						common.RayClusterOwnerKey: toCreate.Name,
					}); err == nil {
					return len(pods.Items)
				}
				return -1
			}).Should(Equal(2))
		})
	})

})
