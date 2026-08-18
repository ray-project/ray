---
myst:
  html_meta:
    description: "Generated reference for the KubeRay ray.io/v1 custom resource definitions, covering every field of RayCluster, RayCronJob, RayJob, and RayService."
---

<!--
GENERATED FILE -- DO NOT EDIT.

Vendored from ray-project/kuberay:docs/reference/api.md at release tag v1.6.2.
The permalink pins the commit that last modified the artifact at that ref:
  https://github.com/ray-project/kuberay/blob/0a9eabde561ab59ab4bbe626072c359da9214484/docs/reference/api.md

Upstream generates this file with elastic/crd-ref-docs from the Go CRD types in
ray-operator/apis/ray/, and a consistency-check CI job fails the build if it
drifts from those types. That job runs on release-* branches as well as master,
so a release tag's artifact is verified against that branch's CRD types.

The ray.io/v1alpha1 section is stripped on the way in; that API version is
deprecated and slated for removal.

Edits belong upstream in the CRD Go types, not here. Changes made here are
overwritten by the next sync.
-->

(kuberay-crd-api-reference)=
# KubeRay CRD API reference

This page is the generated field reference for the `ray.io/v1` custom resource definitions. It covers `RayCluster`, `RayCronJob`, `RayJob`, and `RayService`, along with the supporting types their fields refer to.

The fields are those of KubeRay v1.6.2. Fields added to KubeRay after that release don't appear here.


## Resource Types
- [RayCluster](#raycluster)
- [RayCronJob](#raycronjob)
- [RayJob](#rayjob)
- [RayService](#rayservice)



## AuthMode

_Underlying type:_ _string_

AuthMode describes the authentication mode for the Ray cluster.



_Appears in:_
- [AuthOptions](#authoptions)

| Field | Description |
| --- | --- |
| `disabled` | AuthModeDisabled disables authentication.<br /> |
| `token` | AuthModeToken enables token-based authentication.<br /> |


## AuthOptions



AuthOptions defines the authentication options for a RayCluster.



_Appears in:_
- [RayClusterSpec](#rayclusterspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `enableK8sTokenAuth` _boolean_ | EnableK8sTokenAuth enables Kubernetes-delegated token authentication.<br />When true, the RAY_ENABLE_K8S_TOKEN_AUTH environment variable is set to "true"<br />across all Ray Pods, and Ray will delegate authentication to the K8s API server.<br />NOTE: The Kubernetes ServiceAccount token mounted to Raylets must be granted<br />the `ray:write` custom verb via RBAC for this to function correctly.<br />WARNING: This feature is intended for standalone RayCluster objects and is<br />currently unsupported for RayJob or RayService resources. |  |  |
| `secretName` _string_ | SecretName is the name of the Secret that contains the authentication token.<br />If set, KubeRay will skip generating a Secret object per RayCluster containing a token.<br />The Secret must have a data key `auth_token` that contains the value of the token. |  |  |
| `mode` _[AuthMode](#authmode)_ | Mode specifies the authentication mode.<br />Supported values are "disabled" and "token".<br />Defaults to "token". |  | Enum: [disabled token] <br /> |


## AutoscalerOptions



AutoscalerOptions specifies optional configuration for the Ray autoscaler.



_Appears in:_
- [RayClusterSpec](#rayclusterspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `resources` _[ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#resourcerequirements-v1-core)_ | Resources specifies optional resource request and limit overrides for the autoscaler container.<br />Default values: 500m CPU request and limit. 512Mi memory request and limit. |  |  |
| `image` _string_ | Image optionally overrides the autoscaler's container image. This override is provided for autoscaler testing and development. |  |  |
| `imagePullPolicy` _[PullPolicy](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#pullpolicy-v1-core)_ | ImagePullPolicy optionally overrides the autoscaler container's image pull policy. This override is provided for autoscaler testing and development. |  |  |
| `securityContext` _[SecurityContext](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#securitycontext-v1-core)_ | SecurityContext defines the security options the container should be run with.<br />If set, the fields of SecurityContext override the equivalent fields of PodSecurityContext.<br />More info: https://kubernetes.io/docs/tasks/configure-pod-container/security-context/ |  |  |
| `idleTimeoutSeconds` _integer_ | IdleTimeoutSeconds is the number of seconds to wait before scaling down a worker pod which is not using Ray resources.<br />Defaults to 60 (one minute). It is not read by the KubeRay operator but by the Ray autoscaler. |  |  |
| `upscalingMode` _[UpscalingMode](#upscalingmode)_ | UpscalingMode is "Conservative", "Default", or "Aggressive."<br />Conservative: Upscaling is rate-limited; the number of pending worker pods is at most the size of the Ray cluster.<br />Default: Upscaling is not rate-limited.<br />Aggressive: An alias for Default; upscaling is not rate-limited.<br />It is not read by the KubeRay operator but by the Ray autoscaler. |  | Enum: [Default Aggressive Conservative] <br /> |
| `version` _[AutoscalerVersion](#autoscalerversion)_ | Version is the version of the Ray autoscaler.<br />Setting this to v1 will explicitly use autoscaler v1.<br />Setting this to v2 will explicitly use autoscaler v2.<br />If this isn't set, the Ray version determines the autoscaler version.<br />In Ray 2.47.0 and later, the default autoscaler version is v2. It's v1 before that. |  | Enum: [v1 v2] <br /> |
| `env` _[EnvVar](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#envvar-v1-core) array_ | Optional list of environment variables to set in the autoscaler container. |  |  |
| `envFrom` _[EnvFromSource](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#envfromsource-v1-core) array_ | Optional list of sources to populate environment variables in the autoscaler container. |  |  |
| `volumeMounts` _[VolumeMount](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#volumemount-v1-core) array_ | Optional list of volumeMounts.  This is needed for enabling TLS for the autoscaler container. |  |  |


## AutoscalerVersion

_Underlying type:_ _string_



_Validation:_
- Enum: [v1 v2]

_Appears in:_
- [AutoscalerOptions](#autoscaleroptions)

| Field | Description |
| --- | --- |
| `v1` |  |
| `v2` |  |


## ClusterUpgradeOptions



These options are currently only supported for the IncrementalUpgrade type.



_Appears in:_
- [RayServiceUpgradeStrategy](#rayserviceupgradestrategy)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `maxSurgePercent` _integer_ | The capacity of serve requests the upgraded cluster should scale to handle each interval.<br />Defaults to 100%. | 100 |  |
| `stepSizePercent` _integer_ | The percentage of traffic to switch to the upgraded RayCluster at a set interval after scaling by MaxSurgePercent. |  |  |
| `intervalSeconds` _integer_ | The interval in seconds between transferring StepSize traffic from the old to new RayCluster. |  |  |
| `gatewayClassName` _string_ | The name of the Gateway Class installed by the Kubernetes Cluster admin. |  |  |


## DeletionCondition



DeletionCondition specifies the trigger conditions for a deletion action.
Exactly one of JobStatus or JobDeploymentStatus must be specified:
  - JobStatus (application-level): Match the Ray job execution status.
  - JobDeploymentStatus (infrastructure-level): Match the RayJob deployment lifecycle status. This is particularly useful for cleaning up resources when Ray jobs fail to be submitted.



_Appears in:_
- [DeletionRule](#deletionrule)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `ttlSeconds` _integer_ | TTLSeconds is the time in seconds from when the JobStatus or JobDeploymentStatus<br />reaches the specified terminal state to when this deletion action should be triggered.<br />The value must be a non-negative integer. | 0 | Minimum: 0 <br /> |


## DeletionPolicy



DeletionPolicy is the legacy single-stage deletion policy.
Deprecated: This struct is part of the legacy API. Use DeletionRule for new configurations.



_Appears in:_
- [DeletionStrategy](#deletionstrategy)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `policy` _[DeletionPolicyType](#deletionpolicytype)_ | Policy is the action to take when the condition is met.<br />This field is logically required when using the legacy OnSuccess/OnFailure policies.<br />It is marked as '+optional' at the API level to allow the 'deletionRules' field to be used instead. |  | Enum: [DeleteCluster DeleteWorkers DeleteSelf DeleteNone] <br /> |


## DeletionPolicyType

_Underlying type:_ _string_





_Appears in:_
- [DeletionPolicy](#deletionpolicy)
- [DeletionRule](#deletionrule)

| Field | Description |
| --- | --- |
| `DeleteCluster` |  |
| `DeleteWorkers` |  |
| `DeleteSelf` |  |
| `DeleteNone` |  |


## DeletionRule



DeletionRule defines a single deletion action and its trigger condition.
This is the new, recommended way to define deletion behavior.



_Appears in:_
- [DeletionStrategy](#deletionstrategy)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `policy` _[DeletionPolicyType](#deletionpolicytype)_ | Policy is the action to take when the condition is met. This field is required. |  | Enum: [DeleteCluster DeleteWorkers DeleteSelf DeleteNone] <br /> |
| `condition` _[DeletionCondition](#deletioncondition)_ | The condition under which this deletion rule is triggered. This field is required. |  |  |


## DeletionStrategy



DeletionStrategy configures automated cleanup after the RayJob reaches a terminal state.
Two mutually exclusive styles are supported:

- Legacy: provide both onSuccess and onFailure (deprecated; removal planned for 1.6.0). May be combined with shutdownAfterJobFinishes and (optionally) global TTLSecondsAfterFinished.
- Rules: provide deletionRules (non-empty list). Rules mode is incompatible with shutdownAfterJobFinishes, legacy fields, and the global TTLSecondsAfterFinished (use per-rule condition.ttlSeconds instead).

Semantics:
  - A non-empty deletionRules selects rules mode; empty lists are treated as unset.
  - Legacy requires both onSuccess and onFailure; specifying only one is invalid.
  - Global TTLSecondsAfterFinished > 0 requires shutdownAfterJobFinishes=true; therefore it cannot be used with rules mode or with legacy alone (no shutdown).
  - Feature gate RayJobDeletionPolicy must be enabled when this block is present.

Validation:
  - CRD XValidations prevent mixing legacy fields with deletionRules and enforce legacy completeness.
  - Controller logic enforces rules vs shutdown exclusivity and TTL constraints.
  - onSuccess/onFailure are deprecated; migration to deletionRules is encouraged.



_Appears in:_
- [RayJobSpec](#rayjobspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `onSuccess` _[DeletionPolicy](#deletionpolicy)_ | OnSuccess is the deletion policy for a successful RayJob.<br />Deprecated: Use `deletionRules` instead for more flexible, multi-stage deletion strategies.<br />This field will be removed in release 1.6.0. |  |  |
| `onFailure` _[DeletionPolicy](#deletionpolicy)_ | OnFailure is the deletion policy for a failed RayJob.<br />Deprecated: Use `deletionRules` instead for more flexible, multi-stage deletion strategies.<br />This field will be removed in release 1.6.0. |  |  |
| `deletionRules` _[DeletionRule](#deletionrule) array_ | DeletionRules is a list of deletion rules, processed based on their trigger conditions.<br />While the rules can be used to define a sequence, if multiple rules are overdue (e.g., due to controller downtime),<br />the most impactful rule (e.g., DeleteSelf) will be executed first to prioritize resource cleanup. |  | MinItems: 1 <br /> |




## GcsFaultToleranceOptions



GcsFaultToleranceOptions contains configs for GCS FT



_Appears in:_
- [RayClusterSpec](#rayclusterspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `redisUsername` _[RedisCredential](#rediscredential)_ |  |  |  |
| `redisPassword` _[RedisCredential](#rediscredential)_ |  |  |  |
| `externalStorageNamespace` _string_ |  |  |  |
| `redisAddress` _string_ |  |  |  |


## HeadGroupSpec



HeadGroupSpec are the spec for the head pod



_Appears in:_
- [RayClusterSpec](#rayclusterspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `template` _[PodTemplateSpec](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#podtemplatespec-v1-core)_ | Template is the exact pod template used in K8s deployments, statefulsets, etc. |  |  |
| `headService` _[Service](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#service-v1-core)_ | HeadService is the Kubernetes service of the head pod. |  |  |
| `enableIngress` _boolean_ | EnableIngress indicates whether operator should create ingress object for head service or not. |  |  |
| `resources` _object (keys:string, values:string)_ | Resources specifies the resource quantities for the head group.<br />These values override the resources passed to `rayStartParams` for the group, but<br />have no effect on the resources set at the K8s Pod container level. |  |  |
| `labels` _object (keys:string, values:string)_ | Labels specifies the Ray node labels for the head group.<br />These labels will also be added to the Pods of this head group and override the `--labels`<br />argument passed to `rayStartParams`. |  |  |
| `rayStartParams` _object (keys:string, values:string)_ | RayStartParams are the params of the start command: node-manager-port, object-store-memory, ... |  |  |
| `serviceType` _[ServiceType](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#servicetype-v1-core)_ | ServiceType is Kubernetes service type of the head service. it will be used by the workers to connect to the head pod |  |  |




## JobSubmissionMode

_Underlying type:_ _string_





_Appears in:_
- [RayJobSpec](#rayjobspec)

| Field | Description |
| --- | --- |
| `K8sJobMode` |  |
| `HTTPMode` |  |
| `InteractiveMode` |  |
| `SidecarMode` |  |


## RayCluster



RayCluster is the Schema for the RayClusters API





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `ray.io/v1` | | |
| `kind` _string_ | `RayCluster` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[RayClusterSpec](#rayclusterspec)_ | Specification of the desired behavior of the RayCluster. |  |  |




## RayClusterSpec



RayClusterSpec defines the desired state of RayCluster



_Appears in:_
- [RayCluster](#raycluster)
- [RayJobSpec](#rayjobspec)
- [RayServiceSpec](#rayservicespec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `upgradeStrategy` _[RayClusterUpgradeStrategy](#rayclusterupgradestrategy)_ | UpgradeStrategy defines the scaling policy used when upgrading the RayCluster |  |  |
| `authOptions` _[AuthOptions](#authoptions)_ | AuthOptions specifies the authentication options for the RayCluster. |  |  |
| `suspend` _boolean_ | Suspend indicates whether a RayCluster should be suspended.<br />A suspended RayCluster will have head pods and worker pods deleted. |  |  |
| `managedBy` _string_ | ManagedBy is an optional configuration for the controller or entity that manages a RayCluster.<br />The value must be either 'ray.io/kuberay-operator' or 'kueue.x-k8s.io/multikueue'.<br />The kuberay-operator reconciles a RayCluster which doesn't have this field at all or<br />the field value is the reserved string 'ray.io/kuberay-operator',<br />but delegates reconciling the RayCluster with 'kueue.x-k8s.io/multikueue' to the Kueue.<br />The field is immutable. |  |  |
| `autoscalerOptions` _[AutoscalerOptions](#autoscaleroptions)_ | AutoscalerOptions specifies optional configuration for the Ray autoscaler. |  |  |
| `headServiceAnnotations` _object (keys:string, values:string)_ |  |  |  |
| `enableInTreeAutoscaling` _boolean_ | EnableInTreeAutoscaling indicates whether operator should create in tree autoscaling configs |  |  |
| `gcsFaultToleranceOptions` _[GcsFaultToleranceOptions](#gcsfaulttoleranceoptions)_ | GcsFaultToleranceOptions for enabling GCS FT |  |  |
| `headGroupSpec` _[HeadGroupSpec](#headgroupspec)_ | HeadGroupSpec is the spec for the head pod |  |  |
| `rayVersion` _string_ | RayVersion is used to determine the command for the Kubernetes Job managed by RayJob |  |  |
| `workerGroupSpecs` _[WorkerGroupSpec](#workergroupspec) array_ | WorkerGroupSpecs are the specs for the worker pods |  |  |


## RayClusterUpgradeStrategy







_Appears in:_
- [RayClusterSpec](#rayclusterspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `type` _[RayClusterUpgradeType](#rayclusterupgradetype)_ | Type represents the strategy used when upgrading the RayCluster Pods. Currently supports `Recreate` and `None`. |  | Enum: [Recreate None] <br /> |


## RayClusterUpgradeType

_Underlying type:_ _string_



_Validation:_
- Enum: [Recreate None]

_Appears in:_
- [RayClusterUpgradeStrategy](#rayclusterupgradestrategy)

| Field | Description |
| --- | --- |
| `Recreate` | During upgrade, Recreate strategy will delete all existing pods before creating new ones<br /> |
| `None` | No new pod will be created while the strategy is set to None<br /> |


## RayCronJob



RayCronJob is the Schema for the raycronjobs API





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `ray.io/v1` | | |
| `kind` _string_ | `RayCronJob` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[RayCronJobSpec](#raycronjobspec)_ |  |  |  |


## RayCronJobSpec







_Appears in:_
- [RayCronJob](#raycronjob)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `jobTemplate` _[RayJobSpec](#rayjobspec)_ | JobTemplate defines the job spec that will be created by cron scheduling |  |  |
| `schedule` _string_ | Schedule is the cron schedule string |  |  |
| `suspend` _boolean_ | Suspend tells the controller to suspend the scheduling, it does not apply to<br />scheduled RayJob. |  |  |


## RayJob



RayJob is the Schema for the rayjobs API





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `ray.io/v1` | | |
| `kind` _string_ | `RayJob` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[RayJobSpec](#rayjobspec)_ |  |  |  |


## RayJobSpec



RayJobSpec defines the desired state of RayJob



_Appears in:_
- [RayCronJobSpec](#raycronjobspec)
- [RayJob](#rayjob)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `activeDeadlineSeconds` _integer_ | ActiveDeadlineSeconds is the duration in seconds that the RayJob may be active before<br />KubeRay actively tries to terminate the RayJob; value must be positive integer. |  |  |
| `backoffLimit` _integer_ | Specifies the number of retries before marking this job failed.<br />Each retry creates a new RayCluster. | 0 |  |
| `rayClusterSpec` _[RayClusterSpec](#rayclusterspec)_ | RayClusterSpec is the cluster template to run the job |  |  |
| `submitterPodTemplate` _[PodTemplateSpec](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#podtemplatespec-v1-core)_ | SubmitterPodTemplate is the template for the pod that will run `ray job submit`. |  |  |
| `metadata` _object (keys:string, values:string)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `clusterSelector` _object (keys:string, values:string)_ | clusterSelector is used to select running rayclusters by labels |  |  |
| `submitterConfig` _[SubmitterConfig](#submitterconfig)_ | Configurations of submitter k8s job. |  |  |
| `managedBy` _string_ | ManagedBy is an optional configuration for the controller or entity that manages a RayJob.<br />The value must be either 'ray.io/kuberay-operator' or 'kueue.x-k8s.io/multikueue'.<br />The kuberay-operator reconciles a RayJob which doesn't have this field at all or<br />the field value is the reserved string 'ray.io/kuberay-operator',<br />but delegates reconciling the RayJob with 'kueue.x-k8s.io/multikueue' to the Kueue.<br />The field is immutable. |  |  |
| `deletionStrategy` _[DeletionStrategy](#deletionstrategy)_ | DeletionStrategy automates post-completion cleanup.<br />Choose one style or omit:<br />  - Legacy: both onSuccess & onFailure (deprecated; may combine with shutdownAfterJobFinishes and TTLSecondsAfterFinished).<br />  - Rules: deletionRules (non-empty) — incompatible with shutdownAfterJobFinishes, legacy fields, and global TTLSecondsAfterFinished (use per-rule condition.ttlSeconds).<br />Global TTLSecondsAfterFinished > 0 requires shutdownAfterJobFinishes=true.<br />Feature gate RayJobDeletionPolicy must be enabled when this field is set. |  |  |
| `entrypoint` _string_ | Entrypoint represents the command to start execution. |  |  |
| `runtimeEnvYAML` _string_ | RuntimeEnvYAML represents the runtime environment configuration<br />provided as a multi-line YAML string. |  |  |
| `jobId` _string_ | If jobId is not set, a new jobId will be auto-generated. |  |  |
| `submissionMode` _[JobSubmissionMode](#jobsubmissionmode)_ | SubmissionMode specifies how RayJob submits the Ray job to the RayCluster.<br />In "K8sJobMode", the KubeRay operator creates a submitter Kubernetes Job to submit the Ray job.<br />In "HTTPMode", the KubeRay operator sends a request to the RayCluster to create a Ray job.<br />In "InteractiveMode", the KubeRay operator waits for a user to submit a job to the Ray cluster.<br />In "SidecarMode", the KubeRay operator injects a container into the Ray head Pod that acts as the job submitter to submit the Ray job. | K8sJobMode |  |
| `entrypointResources` _string_ | EntrypointResources specifies the custom resources and quantities to reserve for the<br />entrypoint command. |  |  |
| `entrypointNumCpus` _float_ | EntrypointNumCpus specifies the number of cpus to reserve for the entrypoint command. |  |  |
| `entrypointNumGpus` _float_ | EntrypointNumGpus specifies the number of gpus to reserve for the entrypoint command. |  |  |
| `ttlSecondsAfterFinished` _integer_ | TTLSecondsAfterFinished is the TTL to clean up RayCluster.<br />It's only working when ShutdownAfterJobFinishes set to true. | 0 |  |
| `preRunningDeadlineSeconds` _integer_ | PreRunningDeadlineSeconds is the deadline in seconds for a RayJob to reach the Running state<br />from when it is first initialized (StartTime). If the RayJob does not transition to<br />Running within this time, it will be marked as Failed.<br />This is useful for cleaning up jobs stuck in Initializing or Waiting states.<br />If not set, there is no deadline. Value must be a positive integer. |  | Minimum: 1 <br /> |
| `shutdownAfterJobFinishes` _boolean_ | ShutdownAfterJobFinishes will determine whether to delete the ray cluster once rayJob succeed or failed. |  |  |
| `suspend` _boolean_ | suspend specifies whether the RayJob controller should create a RayCluster instance<br />If a job is applied with the suspend field set to true,<br />the RayCluster will not be created and will wait for the transition to false.<br />If the RayCluster is already created, it will be deleted.<br />In case of transition to false a new RayCluster will be created. |  |  |






## RayService



RayService is the Schema for the rayservices API





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `ray.io/v1` | | |
| `kind` _string_ | `RayService` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[RayServiceSpec](#rayservicespec)_ |  |  |  |






## RayServiceSpec



RayServiceSpec defines the desired state of RayService



_Appears in:_
- [RayService](#rayservice)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `rayClusterDeletionDelaySeconds` _integer_ | RayClusterDeletionDelaySeconds specifies the delay, in seconds, before deleting old RayClusters.<br />The default value is 60 seconds. |  | Minimum: 0 <br /> |
| `serviceUnhealthySecondThreshold` _integer_ | Deprecated: This field is not used anymore. ref: https://github.com/ray-project/kuberay/issues/1685 |  |  |
| `deploymentUnhealthySecondThreshold` _integer_ | Deprecated: This field is not used anymore. ref: https://github.com/ray-project/kuberay/issues/1685 |  |  |
| `serveService` _[Service](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#service-v1-core)_ | ServeService is the Kubernetes service for head node and worker nodes who have healthy http proxy to serve traffics. |  |  |
| `upgradeStrategy` _[RayServiceUpgradeStrategy](#rayserviceupgradestrategy)_ | UpgradeStrategy defines the scaling policy used when upgrading the RayService. |  |  |
| `managedBy` _string_ | ManagedBy is an optional configuration for the controller or entity that manages a RayService.<br />The value must be either 'ray.io/kuberay-operator' or 'kueue.x-k8s.io/multikueue'.<br />The kuberay-operator reconciles a RayService which doesn't have this field at all or<br />the field value is the reserved string 'ray.io/kuberay-operator',<br />but delegates reconciling the RayService with 'kueue.x-k8s.io/multikueue' to the Kueue.<br />The field is immutable. |  |  |
| `serveConfigV2` _string_ | Important: Run "make" to regenerate code after modifying this file<br />Defines the applications and deployments to deploy, should be a YAML multi-line scalar string. |  |  |
| `rayClusterConfig` _[RayClusterSpec](#rayclusterspec)_ |  |  |  |
| `excludeHeadPodFromServeSvc` _boolean_ | If the field is set to true, the value of the label `ray.io/serve` on the head Pod should always be false.<br />Therefore, the head Pod's endpoint will not be added to the Kubernetes Serve service. |  |  |




## RayServiceUpgradeStrategy







_Appears in:_
- [RayServiceSpec](#rayservicespec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `type` _[RayServiceUpgradeType](#rayserviceupgradetype)_ | Type represents the strategy used when upgrading the RayService. Currently supports `NewCluster`, `NewClusterWithIncrementalUpgrade` and `None`. |  |  |
| `clusterUpgradeOptions` _[ClusterUpgradeOptions](#clusterupgradeoptions)_ | ClusterUpgradeOptions defines the behavior of a NewClusterWithIncrementalUpgrade type.<br />RayServiceIncrementalUpgrade feature gate must be enabled to set ClusterUpgradeOptions. |  |  |


## RayServiceUpgradeType

_Underlying type:_ _string_





_Appears in:_
- [RayServiceUpgradeStrategy](#rayserviceupgradestrategy)

| Field | Description |
| --- | --- |
| `NewClusterWithIncrementalUpgrade` | During upgrade, NewClusterWithIncrementalUpgrade strategy will create an upgraded cluster to gradually scale<br />and migrate traffic to using Gateway API.<br /> |
| `NewCluster` | During upgrade, NewCluster strategy will create new upgraded cluster and switch to it when it becomes ready<br /> |
| `None` | No new cluster will be created while the strategy is set to None<br /> |


## RedisCredential



RedisCredential is the redis username/password or a reference to the source containing the username/password



_Appears in:_
- [GcsFaultToleranceOptions](#gcsfaulttoleranceoptions)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `valueFrom` _[EnvVarSource](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#envvarsource-v1-core)_ |  |  |  |
| `value` _string_ |  |  |  |


## ScaleStrategy



ScaleStrategy to remove workers



_Appears in:_
- [WorkerGroupSpec](#workergroupspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `workersToDelete` _string array_ | WorkersToDelete workers to be deleted |  |  |


## SubmitterConfig







_Appears in:_
- [RayJobSpec](#rayjobspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `backoffLimit` _integer_ | BackoffLimit of the submitter k8s job. |  |  |


## UpscalingMode

_Underlying type:_ _string_



_Validation:_
- Enum: [Default Aggressive Conservative]

_Appears in:_
- [AutoscalerOptions](#autoscaleroptions)



## WorkerGroupSpec



WorkerGroupSpec are the specs for the worker pods



_Appears in:_
- [RayClusterSpec](#rayclusterspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `suspend` _boolean_ | Suspend indicates whether a worker group should be suspended.<br />A suspended worker group will have all pods deleted.<br />This is not a user-facing API and is only used by RayJob DeletionStrategy. |  |  |
| `groupName` _string_ | we can have multiple worker groups, we distinguish them by name |  |  |
| `replicas` _integer_ | Replicas is the number of desired Pods for this worker group. See https://github.com/ray-project/kuberay/pull/1443 for more details about the reason for making this field optional. | 0 |  |
| `minReplicas` _integer_ | MinReplicas denotes the minimum number of desired Pods for this worker group. | 0 |  |
| `maxReplicas` _integer_ | MaxReplicas denotes the maximum number of desired Pods for this worker group, and the default value is maxInt32. | 2147483647 |  |
| `idleTimeoutSeconds` _integer_ | IdleTimeoutSeconds denotes the number of seconds to wait before the v2 autoscaler terminates an idle worker pod of this type.<br />This value is only used with the Ray Autoscaler enabled and defaults to the value set by the AutoscalingConfig if not specified for this worker group. |  |  |
| `resources` _object (keys:string, values:string)_ | Resources specifies the resource quantities for this worker group.<br />These values override the resources passed to `rayStartParams` for the group, but<br />have no effect on the resources set at the K8s Pod container level. |  |  |
| `labels` _object (keys:string, values:string)_ | Labels specifies the Ray node labels for this worker group.<br />These labels will also be added to the Pods of this worker group and override the `--labels`<br />argument passed to `rayStartParams`. |  |  |
| `rayStartParams` _object (keys:string, values:string)_ | RayStartParams are the params of the start command: address, object-store-memory, ... |  |  |
| `template` _[PodTemplateSpec](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#podtemplatespec-v1-core)_ | Template is a pod template for the worker |  |  |
| `scaleStrategy` _[ScaleStrategy](#scalestrategy)_ | ScaleStrategy defines which pods to remove |  |  |
| `numOfHosts` _integer_ | NumOfHosts denotes the number of hosts to create per replica. The default value is 1. | 1 |  |
