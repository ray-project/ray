package http

import (
	"github.com/go-logr/logr"
	"github.com/google/uuid"
	automlv1 "github.com/ray-automl/apis/automl/v1"
	"github.com/ray-automl/common"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
	"os"
	"strconv"
	"strings"
)

const (
	WorkerId = "worker-id"
)

func NewDeploymentInstanceWorker(workerStartReq *WorkerStartReq, log logr.Logger) []*appsv1.StatefulSet {
	workerDeployments := make([]*appsv1.StatefulSet, 0)
	if workerStartReq.Workers != nil && len(workerStartReq.Workers) > 0 {
		for group, params := range workerStartReq.Workers {

			log.Info("group and params", "group", group, "params", params)

			workerDeployment := &appsv1.StatefulSet{}
			workerDeployment.Name = workerStartReq.Name
			if workerStartReq.Name == "" {
				workerDeployment.Name = buildGroupNameWithRandomSuffix(automlv1.TrainerWorkerContainerName, params[automlv1.TrainerWorkerGroup])
			}
			workerDeployment.Namespace = workerStartReq.Namespace
			workerDeployment.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
			workerDeployment.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever

			container := &corev1.Container{Name: automlv1.TrainerWorkerContainerName}
			workerParams := map[string]string{}
			workerParams[automlv1.TrainerAddress] = workerStartReq.TrainerAddress
			workerParams[WorkerId] = "$MY_POD_NAME"

			paramMap := common.ConvertParamMap(workerParams)

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
				}, corev1.EnvVar{
					Name: "NAMESPACE",
					ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{
							FieldPath: "metadata.namespace",
						},
					},
				},
			)

			container.Command = []string{"bash", "-c"}
			container.Args = []string{"python -m automl.worker " + paramMap}
			container.Image = os.Getenv(common.Image)
			if workerStartReq.Image != "" {
				container.Image = workerStartReq.Image
			}

			replicas := common.GetOrDefault(params, common.Replicas, "1")
			cpu := common.GetOrDefault(params, common.Cpu, os.Getenv(automlv1.WorkerCpu))
			memory := common.GetOrDefault(params, common.Memory, os.Getenv(automlv1.WorkerMemory))
			disk := common.GetOrDefault(params, common.Disk, os.Getenv(automlv1.WorkerDisk))
			gpuCard := common.GetOrDefault(params, common.GpuCard, common.GpuCardDefault)
			gpu := common.GetOrDefault(params, common.Gpu, "0")

			container.Resources.Limits = common.BuildResourceList(cpu, memory, disk)
			container.Resources.Requests = common.BuildResourceList(cpu, memory, disk)

			if gpu != "" && gpu != "0" {
				gpuResourceName := corev1.ResourceName(gpuCard)
				quantity := resource.MustParse(gpu)
				container.Resources.Limits[gpuResourceName] = quantity
			}

			workerDeployment.Spec.Template.Spec.Containers = append(workerDeployment.Spec.Template.Spec.Containers, *container)

			workerDeployment.Labels = make(map[string]string)
			workerDeployment.Labels[common.TrainerWorkerLabelSelector] = workerDeployment.Name
			workerDeployment.Annotations = make(map[string]string)
			replica, _ := strconv.Atoi(replicas)
			workerDeployment.Spec.Replicas = pointer.Int32(int32(replica))
			if workerDeployment.Spec.Selector == nil {
				workerDeployment.Spec.Selector = &metav1.LabelSelector{
					MatchLabels: workerDeployment.Labels,
				}
			}
			workerDeployment.Spec.Template.Labels = workerDeployment.Labels
			workerDeployment.Spec.Template.Annotations = workerDeployment.Annotations
			workerDeployments = append(workerDeployments, workerDeployment.DeepCopy())
		}
	}
	return workerDeployments
}

func buildGroupNameWithRandomSuffix(podType, group string) string {
	if group != "" {
		return podType + common.DASH + group + common.DASH + strings.ToLower(uuid.New().String()[:4])
	} else {
		return podType + common.DASH + strings.ToLower(uuid.New().String()[:4])
	}
}
