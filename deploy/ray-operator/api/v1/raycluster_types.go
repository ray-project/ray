package v1

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
	ClusterName     string          `json:"clusterName"`
	Images          RayClusterImage `json:"images"`
	ImagePullPolicy string          `json:"imagePullPolicy,omitempty"`
	Extensions      []Extension     `json:"extensions,omitempty"`
}

// RayClusterStatus defines the observed state of RayCluster
type RayClusterStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RayCluster is the Schema for the RayClusters API
// +k8s:openapi-gen=true
type RayCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

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
	DefaultImage string `json:"defaultImage,omitempty"`
}

type Extension struct {
	// the size of this pod group
	Size int32 `json:"size"`

	// worker/head
	Type string `json:"type,omitempty"`

	// pod image
	Image string `json:"image,omitempty"`

	// logic groupName for worker in same group
	GroupName string `json:"groupName,omitempty"`

	// the concrete pod name list, separate by ","
	IdList []string `json:"idList,omitempty"`

	// labels for pod, raycluster.component and rayclusters.ray.io/component-name are default labels, do not overwrite them.
	Labels map[string]string `json:"labels,omitempty"`

	// NodeSelector specifies a map of key-value pairs. For the pod to be eligible
	// to run on a node, the node must have each of the indicated key-value pairs as
	// labels.
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// the affinity for pod
	Affinity *v1.Affinity `json:"affinity,omitempty"`

	// the resource requirements for this group pod.
	Resources v1.ResourceRequirements `json:"resources,omitempty"`

	// Tolerations specifies the pod's tolerations.
	Tolerations []v1.Toleration `json:"tolerations,omitempty"`

	// List of environment variables to set in the container.
	ContainerEnv []v1.EnvVar `json:"containerEnv,omitempty"`

	// Head service suffix. So head can be accessed by domain name
	HeadServiceSuffix string `json:"headServiceSuffix,omitempty"`

	// Annotations for pod
	Annotations map[string]string `json:"annotations,omitempty"`

	// Volume for the pod group
	Volumes []v1.Volume `json:"volumes,omitempty"`

	// VolumeMount for the pod group
	VolumeMounts []v1.VolumeMount `json:"volumeMounts,omitempty"`
}
