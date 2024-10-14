/*
Copyright 2023.

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

package v1

import (
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

const (
	TrainerWorkerContainerName        = "worker"
	TrainerWorkerGroup                = "group"
	TrainerContainerName              = "trainer"
	TrainerContainerPortNumber        = "trainer-port"
	TrainerGrpcPortNumber             = "grpc-port"
	TrainerId                         = "trainer-id"
	TaskId                            = "task-id"
	Hostname                          = "host-name"
	TrainerAddress                    = "trainer-address"
	TrainerContainerPortNumberDefault = int32(2345)

	TrainerCpu    = "TRAINER_CPU"
	TrainerMemory = "TRAINER_MEMORY"
	TrainerDisk   = "TRAINER_DISK"

	WorkerCpu    = "WORKER_CPU"
	WorkerMemory = "WORKER_MEMORY"
	WorkerDisk   = "WORKER_DISK"
)

// TrainerSpec defines the desired state of Trainer
type TrainerSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	Image string `json:"image,omitempty"`
	// StartParams are the params of the start command
	StartParams map[string]string            `json:"startParams,omitempty"`
	DeploySpec  appsv1.DeploymentSpec        `json:"deploySpec,omitempty"`
	Workers     map[string]map[string]string `json:"workers,omitempty"`
}

// TrainerStatus defines the observed state of Trainer
type TrainerStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// Trainer is the Schema for the trainers API
// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type Trainer struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TrainerSpec   `json:"spec,omitempty"`
	Status TrainerStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// TrainerList contains a list of Trainer
type TrainerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Trainer `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Trainer{}, &TrainerList{})
}
