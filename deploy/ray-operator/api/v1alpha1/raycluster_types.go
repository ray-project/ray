package v1alpha1

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// RayClusterSpec defines the desired state of RayCluster
type RayClusterSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// ClusterName is unique identifier for RayCluster in one namespace.
	ClusterName string `json:"clusterName"`
	// Docker image.
	Images RayClusterImage `json:"images"`
	// Image pull policy.
	// One of Always, Never, IfNotPresent.
	// Defaults to Always if :latest tag is specified, or IfNotPresent otherwise.
	// Cannot be updated.
	ImagePullPolicy v1.PullPolicy `json:"imagePullPolicy,omitempty"`
	// Specification of the desired behavior of pod group
	Extensions []Extension `json:"extensions,omitempty"`
}

// RayCluster pod type
type ReplicaType string

const (
	// ReplicaTypeHead means that this pod will be ray cluster head
	ReplicaTypeHead ReplicaType = "head"
	// ReplicaTypeWorker means that this pod will be ray cluster worker
	ReplicaTypeWorker ReplicaType = "worker"
)

// RayClusterStatus defines the observed state of RayCluster
type RayClusterStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// +kubebuilder:object:root=true

// RayCluster is the Schema for the RayClusters API
type RayCluster struct {
	// Standard object metadata.
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Specification of the desired behavior of the RayCluster.
	Spec   RayClusterSpec   `json:"spec,omitempty"`
	Status RayClusterStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RayClusterList contains a list of RayCluster
type RayClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RayCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RayCluster{}, &RayClusterList{})
}

type RayClusterImage struct {
	// Default docker image
	DefaultImage string `json:"defaultImage,omitempty"`
}

type Extension struct {
	// Number of desired pods in this pod group. This is a pointer to distinguish between explicit
	// zero and not specified. Defaults to 1.
	Replicas *int32 `json:"replicas"`

	// The pod type in this group worker/head.
	Type ReplicaType `json:"type,omitempty"`

	// Pod docker image
	Image string `json:"image,omitempty"`

	// Logical groupName for worker in same group, it's used for heterogeneous feature to distinguish different groups.
	// GroupName is the unique identifier for the pods with the same configuration in one Ray Cluster
	GroupName string `json:"groupName,omitempty"`

	// Command to start ray
	Command string `json:"command,omitempty"`

	// Labels for pod, raycluster.component and rayclusters.ray.io/component-name are default labels, do not overwrite them.
	// Labels are intended to be used to specify identifying attributes of objects that are meaningful and relevant to users,
	// but do not directly imply semantics to the core system. Labels can be used to organize and to select subsets of objects.
	Labels map[string]string `json:"labels,omitempty"`

	// The service acccount name.
	ServiceAccountName string `json:"serviceAccountName,omitempty"`

	// NodeSelector specifies a map of key-value pairs. For the pod to be eligible
	// to run on a node, the node must have each of the indicated key-value pairs as
	// labels. Optional.
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Affinity is a group of affinity scheduling rules. Optional.
	// It allows you to constrain which nodes your pod is eligible to be scheduled on, based on labels on the node.
	Affinity *v1.Affinity `json:"affinity,omitempty"`

	// ResourceRequirements describes the compute resource requirements.
	Resources v1.ResourceRequirements `json:"resources,omitempty"`

	// The pod this Toleration is attached to tolerates any taint that matches
	// the triple <key,value,effect> using the matching operator <operator>. Optional.
	// Tolerations are applied to pods, and allow (but do not require) the pods to schedule onto nodes with matching taints.
	Tolerations []v1.Toleration `json:"tolerations,omitempty"`

	// EnvVar represents an environment variable present in a Container. Optional.
	// Define environment variables for a container. This can be helpful in defining variables before setting up the container.
	ContainerEnv []v1.EnvVar `json:"containerEnv,omitempty"`

	// Head service suffix. So head can be accessed by domain name: {namespace}.svc , follows Kubernetes standard.
	HeadServiceSuffix string `json:"headServiceSuffix,omitempty"`

	// Annotations is an unstructured key value map stored with a resource that may be
	// set by external tools to store and retrieve arbitrary metadata. They are not
	// queryable and should be preserved when modifying objects. Optional.
	// Usage refers to https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/
	Annotations map[string]string `json:"annotations,omitempty"`

	// Volume represents a named volume in a pod that may be accessed by any container in the pod. Optional.
	// At its core, a volume is just a directory, possibly with some data in it, which is accessible to the Containers in a Pod.
	Volumes []v1.Volume `json:"volumes,omitempty"`

	// VolumeMount describes a mounting of a Volume within a container. Optional.
	VolumeMounts []v1.VolumeMount `json:"volumeMounts,omitempty"`
}
