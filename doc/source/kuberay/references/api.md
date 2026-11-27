---
myst:
  html_meta:
    description: "Generated reference for the KubeRay ray.io/v1 custom resource definitions, covering every field of RayCluster, RayCronJob, RayJob, and RayService."
---

<!--
GENERATED FILE -- DO NOT EDIT.

Vendored from ray-project/kuberay:docs/reference/api.md at release tag v1.8.0.
The permalink pins the commit that last modified the artifact at that ref:
  https://github.com/ray-project/kuberay/blob/dd8a8b8c2a939b3877fc0f4d195cf12ee0c82970/docs/reference/api.md

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

The fields are those of KubeRay v1.8.0. Fields added to KubeRay after that release don't appear here.

## Resource Types
- [RayCluster](#raycluster)
- [RayCronJob](#raycronjob)
- [RayJob](#rayjob)
- [RayService](#rayservice)

## RayCluster

RayCluster is the Schema for the RayClusters API

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `ray.io/v1` | | |
| `kind` _string_ | `RayCluster` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[RayClusterSpec](#rayclusterspec)_ | Specification of the desired behavior of the RayCluster. |  |  |
| `status` _[RayClusterStatus](#rayclusterstatus)_ | Status is the most recently observed state of the RayCluster. |  |  |

## RayCronJob

RayCronJob is the Schema for the raycronjobs API

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `ray.io/v1` | | |
| `kind` _string_ | `RayCronJob` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[RayCronJobSpec](#raycronjobspec)_ |  |  |  |
| `status` _[RayCronJobStatus](#raycronjobstatus)_ | Status is the most recently observed state of the RayCronJob. |  |  |

## RayJob

RayJob is the Schema for the rayjobs API

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `ray.io/v1` | | |
| `kind` _string_ | `RayJob` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[RayJobSpec](#rayjobspec)_ |  |  |  |
| `status` _[RayJobStatus](#rayjobstatus)_ | Status is the most recently observed state of the RayJob. |  |  |

## RayService

RayService is the Schema for the rayservices API

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `ray.io/v1` | | |
| `kind` _string_ | `RayService` | | |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[RayServiceSpec](#rayservicespec)_ |  |  |  |
| `status` _[RayServiceStatuses](#rayservicestatuses)_ | Status is the most recently observed state of the RayService. |  |  |

## ActivePassiveHeadOptions

ActivePassiveHeadOptions configures active-passive head high availability for
the GCS via leader election.

_Appears in:_
- [GcsFaultToleranceOptions](#gcsfaulttoleranceoptions)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `enabled` _boolean_ | Enabled turns on active-passive head HA for the RayCluster. When true, KubeRay<br />provisions a standby head Pod. Defaults to false. |  |  |
| `leaseDurationSeconds` _integer_ | LeaseDurationSeconds is the duration that non-leader candidates wait before forcing leadership acquisition. | 15 | Minimum: 1 <br /> |
| `renewDeadlineSeconds` _integer_ | RenewDeadlineSeconds is the acting leader's bounded deadline for executing consecutive renewal sequences. | 10 | Minimum: 1 <br /> |
| `retryPeriodSeconds` _integer_ | RetryPeriodSeconds is the duration clients wait between sequential resource acquisition attempts. | 2 | Minimum: 1 <br /> |

## AppStatus

_Appears in:_
- [RayServiceStatus](#rayservicestatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `serveDeploymentStatuses` _object (keys:string, values:[ServeDeploymentStatus](#servedeploymentstatus))_ |  |  |  |
| `status` _string_ |  |  |  |
| `message` _string_ |  |  |  |

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
| `command` _string array_ | Optional list overwrite the default command of the autoscaler container. |  |  |
| `args` _string array_ | Optional to overwrite the default args of the autoscaler container. |  |  |

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

## ClusterState

_Underlying type:_ _string_

The overall state of the Ray cluster.

_Appears in:_
- [RayClusterStatus](#rayclusterstatus)

| Field | Description |
| --- | --- |
| `ready` |  |
| `failed` | Failed is deprecated, but we keep it to avoid compilation errors in projects that import the KubeRay Golang module.<br /> |
| `suspended` |  |

## ClusterUpgradeOptions

These options are currently only supported for the IncrementalUpgrade type.

_Appears in:_
- [RayServiceUpgradeStrategy](#rayserviceupgradestrategy)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `maxSurgePercent` _integer_ | The capacity of serve requests the upgraded cluster should scale to handle each interval.<br />Defaults to 100%. | 100 |  |
| `stepSizePercent` _integer_ | The percentage of traffic to switch to the upgraded RayCluster at a set interval after scaling by MaxSurgePercent.<br />StepSizePercent must be less than or equal to MaxSurgePercent. |  |  |
| `intervalSeconds` _integer_ | The interval in seconds between transferring StepSize traffic from the old to new RayCluster. |  |  |
| `gatewayClassName` _string_ | The name of the Gateway Class installed by the Kubernetes Cluster admin.<br />Exactly one of GatewayClassName or GatewayRef must be set. |  |  |
| `gatewayRef` _[GatewayReference](#gatewayreference)_ | GatewayRef attaches the HTTPRoute to an existing Gateway instead of creating one per RayService.<br />The referenced Gateway must allow HTTPRoutes from the RayService's namespace.<br />Exactly one of GatewayClassName or GatewayRef must be set. |  |  |

## CollectorOptions

CollectorOptions defines settings for the history server collector sidecar.

_Appears in:_
- [HistoryServerOptions](#historyserveroptions)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `image` _string_ | Image is the collector container image to be used (e.g. quay.io/kuberay/collector:v1.7.0). |  |  |
| `imagePullPolicy` _[PullPolicy](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#pullpolicy-v1-core)_ | ImagePullPolicy is the pull policy for the collector image. |  |  |
| `resources` _[ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#resourcerequirements-v1-core)_ | Resources specifies computing resource requirements. |  |  |
| `env` _[EnvVar](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#envvar-v1-core) array_ | Env allows injecting custom environment variables into the collector container. |  |  |
| `securityContext` _[SecurityContext](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#securitycontext-v1-core)_ | SecurityContext specifies the security context for the collector container.<br />If set, the fields of SecurityContext override the equivalent fields of PodSecurityContext.<br />More info: https://kubernetes.io/docs/tasks/configure-pod-container/security-context/ |  |  |

## DeletionCondition

DeletionCondition specifies the trigger conditions for a deletion action.
Exactly one of JobStatus or JobDeploymentStatus must be specified:
  - JobStatus (application-level): Match the Ray job execution status.
  - JobDeploymentStatus (infrastructure-level): Match the RayJob deployment lifecycle status. This is particularly useful for cleaning up resources when Ray jobs fail to be submitted.

_Appears in:_
- [DeletionRule](#deletionrule)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `jobStatus` _[JobStatus](#jobstatus)_ | JobStatus is the terminal status of the RayJob that triggers this condition.<br />For the initial implementation, only "SUCCEEDED" and "FAILED" are supported. |  | Enum: [SUCCEEDED FAILED] <br /> |
| `jobDeploymentStatus` _[JobDeploymentStatus](#jobdeploymentstatus)_ | JobDeploymentStatus is the terminal status of the RayJob deployment that triggers this condition.<br />For the initial implementation, only "Failed" is supported. |  | Enum: [Failed] <br /> |
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

## GCSStorageDeletionPolicy

_Underlying type:_ _string_

GCSStorageDeletionPolicy specifies what happens to the operator-managed GCS
storage PVC when the owning RayCluster is deleted.

_Validation:_
- Enum: [DeleteWithCluster Retain]

_Appears in:_
- [GcsEmbeddedStorage](#gcsembeddedstorage)

| Field | Description |
| --- | --- |
| `DeleteWithCluster` | DeleteWithClusterGCSStorageDeletionPolicy (the default) makes the<br />operator-managed PVC a child of the RayCluster via an ownerReference, so it<br />(and its RocksDB data) is garbage-collected together with the cluster.<br /> |
| `Retain` | RetainGCSStorageDeletionPolicy keeps the operator-managed PVC (and its data)<br />after the owning RayCluster is deleted: the operator omits the ownerReference<br />so the PVC outlives the cluster. Recover the GCS state by pointing a new<br />cluster's ClaimName at the retained PVC.<br /> |

## GatewayReference

GatewayReference identifies the existing Gateway and listener the RayService's
HTTPRoute attaches to. It becomes the HTTPRoute's parentRef. KubeRay never creates,
updates, or deletes the referenced Gateway; it must already exist and must allow
HTTPRoutes from the RayService's namespace.

_Appears in:_
- [ClusterUpgradeOptions](#clusterupgradeoptions)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `name` _string_ | Name of the existing Gateway. Must be a valid Gateway resource name<br />(an RFC 1123 subdomain). |  | MaxLength: 253 <br />MinLength: 1 <br />Pattern: `^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$` <br /> |
| `namespace` _string_ | Namespace of the existing Gateway. Defaults to the RayService's namespace when omitted.<br />Must be a valid namespace name (an RFC 1123 label). |  | MaxLength: 63 <br />Pattern: `^[a-z0-9]([-a-z0-9]*[a-z0-9])?$` <br /> |
| `sectionName` _string_ | SectionName is the name of a listener on the referenced Gateway to attach the<br />HTTPRoute to. When omitted, the HTTPRoute attaches to every listener on the<br />Gateway that accepts it. Useful for a shared Gateway with multiple listeners.<br />Must be a valid Gateway API SectionName (an RFC 1123 subdomain). |  | MaxLength: 253 <br />MinLength: 1 <br />Pattern: `^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$` <br /> |
| `port` _integer_ | Port is the network port of the referenced Gateway's listener to attach the<br />HTTPRoute to. When both SectionName and Port are set, the selected listener<br />must match both. When omitted, listener selection is not constrained by port. |  | Maximum: 65535 <br />Minimum: 1 <br /> |

## GcsEmbeddedStorage

GcsEmbeddedStorage configures the PVC backing the embedded RocksDB store.

RocksDB tolerates only a single writer at a time. The operator mounts the
volume on the head Pod but does not itself enforce mutual exclusion, so when a
volume can be attached to more than one Pod concurrently (see AccessModes) the
caller is responsible for ensuring only one Ray head writes to it at a time.

_Appears in:_
- [GcsFaultToleranceOptions](#gcsfaulttoleranceoptions)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `claimName` _string_ | ClaimName is the name of an existing, user-provided PersistentVolumeClaim to<br />use as the RocksDB store ("bring your own" PVC). When set, the operator<br />consumes that PVC as-is: it does not create, delete, resize, or set<br />ownerReferences on it -- the user owns its entire lifecycle. Mutually<br />exclusive with Size/StorageClassName/AccessModes (those configure an<br />operator-managed PVC, which is used instead when ClaimName is empty).<br />This is the supported path for persisting GCS state across a RayService<br />zero-downtime upgrade: point every RayService generation at the same claim.<br />(An operator-managed PVC is keyed by and owned by the RayCluster, so it is<br />not reused across upgrades.) Because the old and new head Pods overlap during<br />a zero-downtime upgrade, the claim must permit concurrent attach<br />(ReadWriteMany) with externally-coordinated single-writer semantics, or an<br />active-passive handoff where only one Pod attaches at once. |  |  |
| `size` _[Quantity](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#quantity-resource-api)_ | Size of the operator-managed PVC (e.g. "1Gi"). Ignored when ClaimName is set.<br />Defaults to 1Gi. The operator-managed PVC is created once and not<br />reconfigured in place; to change size/class/accessModes, delete the PVC (or<br />switch to ClaimName). A warning event is emitted if this diverges from the<br />live PVC. |  |  |
| `storageClassName` _string_ | StorageClassName for the operator-managed PVC. Uses the cluster default<br />StorageClass when omitted. Ignored when ClaimName is set. |  |  |
| `accessModes` _[PersistentVolumeAccessMode](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#persistentvolumeaccessmode-v1-core) array_ | AccessModes for the operator-managed PVC. Defaults to [ReadWriteOnce].<br />Ignored when ClaimName is set.<br />ReadWriteOnce is the sane default for a standalone RayCluster (one head Pod<br />attaches at a time). ReadWriteMany is a valid choice when you need the volume<br />attached to multiple Pods concurrently (e.g. to overlap the old and new head<br />during a RayService upgrade); RocksDB still requires that only one of them<br />writes at a time, which you must coordinate externally. |  |  |
| `subPath` _string_ | SubPath mounts a subdirectory of the volume instead of its root. |  |  |
| `deletionPolicy` _[GCSStorageDeletionPolicy](#gcsstoragedeletionpolicy)_ | DeletionPolicy controls the lifecycle of the operator-managed PVC relative to<br />the owning RayCluster. Defaults to DeleteWithCluster. Ignored when ClaimName<br />is set (the operator never owns a bring-your-own PVC, so it is never deleted<br />or retained by the operator).<br />Recovery after Retain: a PVC left behind by a Retain delete can be recovered<br />either by pointing a new cluster's ClaimName at it, or by recreating a<br />RayCluster with the same name on the operator-managed path -- the operator<br />adopts the existing \{cluster\}-gcs-pvc and reuses its RocksDB state. To start<br />from a fresh store instead, delete the leftover PVC first. |  | Enum: [DeleteWithCluster Retain] <br /> |

## GcsFaultToleranceBackend

_Underlying type:_ _string_

GcsFaultToleranceBackend selects the GCS fault tolerance persistence backend.

_Validation:_
- Enum: [redis rocksdb]

_Appears in:_
- [GcsFaultToleranceOptions](#gcsfaulttoleranceoptions)

| Field | Description |
| --- | --- |
| `redis` | GcsFTBackendRedis persists GCS metadata in an external Redis service.<br /> |
| `rocksdb` | GcsFTBackendRocksDB persists GCS metadata in an embedded RocksDB store on a<br />persistent volume mounted on the head Pod.<br /> |

## GcsFaultToleranceOptions

GcsFaultToleranceOptions contains configs for GCS FT

_Appears in:_
- [RayClusterSpec](#rayclusterspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `backend` _[GcsFaultToleranceBackend](#gcsfaulttolerancebackend)_ | Backend selects the GCS FT persistence backend. Defaults to "redis" for<br />backward compatibility. Immutable: the backend cannot be switched on an<br />existing RayCluster (doing so would swap the entire GCS store and head-Pod<br />wiring, losing fault-tolerance state). |  | Enum: [redis rocksdb] <br /> |
| `redisUsername` _[RedisCredential](#rediscredential)_ |  |  |  |
| `redisPassword` _[RedisCredential](#rediscredential)_ |  |  |  |
| `externalStorageNamespace` _string_ |  |  |  |
| `redisAddress` _string_ | RedisAddress is the address of the external Redis service. Required when<br />Backend is "redis"; must be empty for "rocksdb". |  |  |
| `storage` _[GcsEmbeddedStorage](#gcsembeddedstorage)_ | Storage configures the persistent volume backing the embedded RocksDB<br />store. Only used when Backend is "rocksdb". |  |  |
| `activePassiveHeadOptions` _[ActivePassiveHeadOptions](#activepassiveheadoptions)_ | ActivePassiveHeadOptions configures active-passive high availability for the GCS.<br />It is only supported with the "redis" backend, not with "rocksdb". |  |  |

## HeadGroupSpec

HeadGroupSpec are the spec for the head pod

_Appears in:_
- [RayClusterSpec](#rayclusterspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `template` _[PodTemplateSpec](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#podtemplatespec-v1-core)_ | Template is the exact pod template used in K8s deployments, statefulsets, etc. |  |  |
| `headService` _[Service](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#service-v1-core)_ | HeadService is the Kubernetes service of the head pod. |  |  |
| `enableIngress` _boolean_ | EnableIngress indicates whether operator should create ingress object for head service or not. |  |  |
| `ingressOptions` _[IngressOptions](#ingressoptions)_ | IngressOptions specifies optional ingress configuration for the head service. |  |  |
| `resources` _object (keys:string, values:string)_ | Resources specifies the resource quantities for the head group.<br />These values override the resources passed to `rayStartParams` for the group, but<br />have no effect on the resources set at the K8s Pod container level. |  |  |
| `labels` _object (keys:string, values:string)_ | Labels specifies the Ray node labels for the head group.<br />These labels will also be added to the Pods of this head group and override the `--labels`<br />argument passed to `rayStartParams`. |  |  |
| `rayStartParams` _object (keys:string, values:string)_ | RayStartParams are the params of the start command: node-manager-port, object-store-memory, ... |  |  |
| `serviceType` _[ServiceType](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#servicetype-v1-core)_ | ServiceType is Kubernetes service type of the head service. it will be used by the workers to connect to the head pod |  |  |

## HeadInfo

HeadInfo gives info about head

_Appears in:_
- [RayClusterStatus](#rayclusterstatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `podIP` _string_ |  |  |  |
| `serviceIP` _string_ |  |  |  |
| `podName` _string_ |  |  |  |
| `serviceName` _string_ |  |  |  |

## HistoryServerOptions

HistoryServerOptions used for history server related configuration

_Appears in:_
- [RayClusterSpec](#rayclusterspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `collectorOptions` _[CollectorOptions](#collectoroptions)_ | CollectorOptions used for collector sidecar configuration |  |  |

## IdleTerminationOptions

_Appears in:_
- [RayClusterSpec](#rayclusterspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `timeoutSeconds` _integer_ | TimeoutSeconds denotes the number of seconds to wait before the v2 autoscaler terminates an idle RayCluster. |  |  |
| `policy` _[IdleTerminationPolicy](#idleterminationpolicy)_ | Policy is the action taken once the RayCluster has been idle for TimeoutSeconds. | Suspend | Enum: [Delete Suspend] <br /> |

## IdleTerminationPolicy

_Underlying type:_ _string_

_Validation:_
- Enum: [Delete Suspend]

_Appears in:_
- [IdleTerminationOptions](#idleterminationoptions)

| Field | Description |
| --- | --- |
| `Suspend` |  |
| `Delete` |  |

## IngressOptions

IngressOptions defines the host, path, and TLS configuration for the ingress generated for the head group.

_Appears in:_
- [HeadGroupSpec](#headgroupspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `host` _string_ | Host is the fully-qualified domain name used to route external traffic to the<br />Ray head dashboard. When unset, the generated ingress rule matches any host. |  |  |
| `path` _string_ | Path is the HTTP path that routes to the Ray head dashboard.<br />When unset, the operator defaults it to "/", which routes all traffic on the<br />host to the dashboard. |  |  |
| `pathType` _[IngressPathType](#ingresspathtype)_ | PathType is the path matching mode applied to Path.<br />When unset, the operator defaults it to "Prefix", which works out of the box<br />without a rewrite-target annotation or controller-specific regex support. |  | Enum: [Exact Prefix ImplementationSpecific] <br /> |
| `tls` _[IngressTLS](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#ingresstls-v1-networking) array_ | TLS configures TLS termination for the generated ingress. |  |  |

## IngressPathType

_Underlying type:_ _string_

_Validation:_
- Enum: [Exact Prefix ImplementationSpecific]

_Appears in:_
- [IngressOptions](#ingressoptions)

| Field | Description |
| --- | --- |
| `Exact` |  |
| `Prefix` |  |
| `ImplementationSpecific` |  |

## JobDeploymentStatus

_Underlying type:_ _string_

JobDeploymentStatus indicates RayJob status including RayCluster lifecycle management and Job submission

_Appears in:_
- [DeletionCondition](#deletioncondition)
- [RayJobStatus](#rayjobstatus)

| Field | Description |
| --- | --- |
| `` |  |
| `Initializing` |  |
| `Running` |  |
| `Complete` |  |
| `Failed` |  |
| `ValidationFailed` |  |
| `Suspending` |  |
| `Suspended` |  |
| `Retrying` |  |
| `Waiting` |  |

## JobFailedReason

_Underlying type:_ _string_

JobFailedReason indicates the reason the RayJob changes its JobDeploymentStatus to 'Failed'

_Appears in:_
- [RayJobStatus](#rayjobstatus)

| Field | Description |
| --- | --- |
| `SubmissionFailed` |  |
| `DeadlineExceeded` |  |
| `PreRunningDeadlineExceeded` |  |
| `AppFailed` |  |
| `JobDeploymentStatusTransitionGracePeriodExceeded` |  |
| `JobStatusCheckTimeoutExceeded` |  |
| `ValidationFailed` |  |

## JobStatus

_Underlying type:_ _string_

JobStatus is the Ray Job Status.

_Appears in:_
- [DeletionCondition](#deletioncondition)
- [RayJobStatus](#rayjobstatus)

| Field | Description |
| --- | --- |
| `` |  |
| `PENDING` |  |
| `RUNNING` |  |
| `STOPPED` |  |
| `SUCCEEDED` |  |
| `FAILED` |  |

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

## LabelRef

LabelRef maps a node label to a Ray node label.

_Appears in:_
- [WorkerGroupSpec](#workergroupspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `name` _string_ | Name is the Ray label key the value is delivered under. If empty, defaults to the node label key in fieldPath.<br />Must not be a key in the same group's workerGroupSpecs[].labels. |  | MaxLength: 317 <br /> |
| `valueFrom` _[LabelRefSource](#labelrefsource)_ | ValueFrom selects the node value to deliver. |  |  |

## LabelRefSource

LabelRefSource selects where a LabelRef value comes from.

_Appears in:_
- [LabelRef](#labelref)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `nodeRef` _[NodeFieldRef](#nodefieldref)_ | NodeRef selects a field of the node the pod is bound to. |  |  |

## NetworkPolicyConfig

NetworkPolicyConfig defines network isolation settings for Ray cluster.
All modes permit intra-cluster pod-to-pod traffic.
DNS egress is not included automatically; see NetworkPolicyRules.EgressRules
for why it must be added under DenyAll/DenyAllEgress.

_Appears in:_
- [RayClusterSpec](#rayclusterspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `mode` _[NetworkPolicyMode](#networkpolicymode)_ | Mode controls the security level. All modes permit intra-cluster pod-to-pod<br />traffic (DNS egress excluded, see EgressRules).<br />- "DenyAll": Denies all Ingress and Egress.<br />- "DenyAllIngress": Denies all Ingress.<br />- "DenyAllEgress": Denies all Egress. | DenyAll | Enum: [DenyAll DenyAllIngress DenyAllEgress] <br /> |
| `head` _[NetworkPolicyRules](#networkpolicyrules)_ | Head specifies custom NetworkPolicy rules applied only to the head pod's policy.<br />The base head policy always allows intra-cluster traffic and (for K8sJobMode<br />RayJob-owned clusters) the submitter pod. Rules here are appended to those<br />base rules. Platforms that need operator dashboard access should add it here<br />(e.g. via a mutating webhook). |  |  |
| `worker` _[NetworkPolicyRules](#networkpolicyrules)_ | Worker specifies custom NetworkPolicy rules applied only to worker pods' policy.<br />The base worker policy always allows intra-cluster traffic.<br />Rules here are appended to that base rule.<br />Acts as the default for all worker groups; see WorkerGroups for per-group overrides. |  |  |
| `workerGroups` _[WorkerGroupNetworkPolicyRules](#workergroupnetworkpolicyrules) array_ | WorkerGroups specifies per-worker-group NetworkPolicy rules, keyed by group name.<br />If an entry exists for a worker group, it replaces (not merges with) Worker for<br />that group. Worker groups without an entry fall back to Worker. |  |  |

## NetworkPolicyMode

_Underlying type:_ _string_

NetworkPolicyMode is the type for network isolation mode constants.

_Validation:_
- Enum: [DenyAll DenyAllIngress DenyAllEgress]

_Appears in:_
- [NetworkPolicyConfig](#networkpolicyconfig)

| Field | Description |
| --- | --- |
| `DenyAll` | NetworkPolicyDenyAll denies all ingress and egress traffic.<br /> |
| `DenyAllIngress` | NetworkPolicyDenyAllIngress denies all ingress traffic.<br /> |
| `DenyAllEgress` | NetworkPolicyDenyAllEgress denies all egress traffic.<br /> |

## NetworkPolicyRules

NetworkPolicyRules defines custom ingress and egress rules for a NetworkPolicy.

_Appears in:_
- [NetworkPolicyConfig](#networkpolicyconfig)
- [WorkerGroupNetworkPolicyRules](#workergroupnetworkpolicyrules)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `ingressRules` _[NetworkPolicyIngressRule](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#networkpolicyingressrule-v1-networking) array_ | IngressRules specifies custom ingress rules appended to the base policy.<br />Only meaningful when the mode includes ingress denial (DenyAll or DenyAllIngress). |  |  |
| `egressRules` _[NetworkPolicyEgressRule](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#networkpolicyegressrule-v1-networking) array_ | EgressRules specifies custom egress rules appended to the base policy.<br />Only meaningful when the mode includes egress denial (DenyAll or DenyAllEgress).<br />DNS egress is NOT added automatically: under DenyAll/DenyAllEgress you MUST<br />add a DNS rule here (e.g. to kube-system pods labeled k8s-app=kube-dns on<br />port 53), because Ray workers reach the head via its service FQDN and cannot<br />resolve it without DNS. See the network-policy-deny-all sample. |  |  |

## NodeFieldRef

NodeFieldRef selects a field of the node in downward API syntax.

_Appears in:_
- [LabelRefSource](#labelrefsource)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `fieldPath` _string_ | FieldPath is the node field to read; only metadata.labels['<key>'] is supported in this version.<br />The key must be in the operator's allowedNodeLabels. |  | MinLength: 1 <br /> |

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
| `idleSuspend` _boolean_ | IdleSuspend indicates whether a RayCluster should be suspended due to idleness.<br />An idle-suspended RayCluster will have head pods and worker pods deleted. |  |  |
| `idleTerminationOptions` _[IdleTerminationOptions](#idleterminationoptions)_ | IdleTerminationOptions specifies optional configuration for terminating an idle RayCluster.<br />A RayCluster is considered idle when no Ray driver is connected. |  |  |
| `managedBy` _string_ | ManagedBy is an optional configuration for the controller or entity that manages a RayCluster.<br />The value must be either 'ray.io/kuberay-operator' or 'kueue.x-k8s.io/multikueue'.<br />The kuberay-operator reconciles a RayCluster which doesn't have this field at all or<br />the field value is the reserved string 'ray.io/kuberay-operator',<br />but delegates reconciling the RayCluster with 'kueue.x-k8s.io/multikueue' to the Kueue.<br />The field is immutable. |  |  |
| `autoscalerOptions` _[AutoscalerOptions](#autoscaleroptions)_ | AutoscalerOptions specifies optional configuration for the Ray autoscaler. |  |  |
| `headServiceAnnotations` _object (keys:string, values:string)_ |  |  |  |
| `enableInTreeAutoscaling` _boolean_ | EnableInTreeAutoscaling indicates whether operator should create in tree autoscaling configs |  |  |
| `gcsFaultToleranceOptions` _[GcsFaultToleranceOptions](#gcsfaulttoleranceoptions)_ | GcsFaultToleranceOptions for enabling GCS FT |  |  |
| `historyServerOptions` _[HistoryServerOptions](#historyserveroptions)_ | HistoryServerOptions used for history server related configuration |  |  |
| `networkPolicy` _[NetworkPolicyConfig](#networkpolicyconfig)_ | NetworkPolicy specifies optional configuration for network isolation.<br />When set, separate NetworkPolicies are created for head and worker pods.<br />The reconciler always permits intra-cluster pod-to-pod traffic.<br />Note: under DenyAll/DenyAllEgress, DNS egress is not added<br />automatically; since Ray pods reach the head via its service FQDN, you must<br />allow DNS egress via Head/Worker EgressRules or the cluster will fail to start. |  |  |
| `tlsOptions` _[TLSOptions](#tlsoptions)_ | TLSOptions specifies optional TLS encryption settings for the RayCluster.<br />If omitted or Enabled is false, TLS is disabled. When Enabled is true,<br />the operator enables mTLS using cert-manager to provision and manage certificates.<br />Requires the RayClusterMTLS feature gate on the operator. |  |  |
| `headGroupSpec` _[HeadGroupSpec](#headgroupspec)_ | HeadGroupSpec is the spec for the head pod |  |  |
| `rayVersion` _string_ | RayVersion is used to determine the command for the Kubernetes Job managed by RayJob |  |  |
| `workerGroupSpecs` _[WorkerGroupSpec](#workergroupspec) array_ | WorkerGroupSpecs are the specs for the worker pods |  |  |

## RayClusterStatus

RayClusterStatus defines the observed state of RayCluster

_Appears in:_
- [RayCluster](#raycluster)
- [RayJobStatus](#rayjobstatus)
- [RayServiceStatus](#rayservicestatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `state` _[ClusterState](#clusterstate)_ | INSERT ADDITIONAL STATUS FIELD - define observed state of cluster<br />Important: Run "make" to regenerate code after modifying this file<br />Status reflects the status of the cluster<br />Deprecated: the State field is replaced by the Conditions field. |  |  |
| `desiredCPU` _[Quantity](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#quantity-resource-api)_ | DesiredCPU indicates total desired CPUs for the cluster |  |  |
| `desiredMemory` _[Quantity](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#quantity-resource-api)_ | DesiredMemory indicates total desired memory for the cluster |  |  |
| `desiredGPU` _[Quantity](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#quantity-resource-api)_ | DesiredGPU indicates total desired GPUs for the cluster |  |  |
| `desiredTPU` _[Quantity](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#quantity-resource-api)_ | DesiredTPU indicates total desired TPUs for the cluster |  |  |
| `lastUpdateTime` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#time-v1-meta)_ | LastUpdateTime indicates last update timestamp for this cluster status. |  |  |
| `stateTransitionTimes` _object (keys:[ClusterState](#clusterstate), values:[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#time-v1-meta))_ | StateTransitionTimes indicates the time of the last state transition for each state. |  |  |
| `endpoints` _object (keys:string, values:string)_ | Service Endpoints |  |  |
| `head` _[HeadInfo](#headinfo)_ | Head info |  |  |
| `reason` _string_ | Reason provides more information about current State |  |  |
| `conditions` _[Condition](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#condition-v1-meta) array_ | Represents the latest available observations of a RayCluster's current state. |  |  |
| `readyWorkerReplicas` _integer_ | ReadyWorkerReplicas indicates the number of worker pods currently in the Ready state in the cluster.<br />It actually reflects the number of Ready pods, although it is named "replicas" to maintain backward compatibility. |  |  |
| `availableWorkerReplicas` _integer_ | AvailableWorkerReplicas indicates how many worker pods are currently available (i.e., running).<br />It is named "replicas" to maintain backward compatibility. |  |  |
| `desiredWorkerReplicas` _integer_ | DesiredWorkerReplicas indicates the desired total number of worker Pods at the cluster level,<br />calculated as the sum of `replicas * numOfHosts` for each worker group.<br />It is named "replicas" to maintain backward compatibility. |  |  |
| `minWorkerReplicas` _integer_ | MinWorkerReplicas indicates the minimum number of worker pods across all worker groups,<br />calculated as the sum of `minReplicas * numOfHosts` for each worker group.<br />It is named "replicas" to maintain backward compatibility. |  |  |
| `maxWorkerReplicas` _integer_ | MaxWorkerReplicas indicates the maximum number of worker pods across all worker groups,<br />calculated as the sum of `maxReplicas * numOfHosts` for each worker group.<br />It is named "replicas" to maintain backward compatibility. |  |  |
| `observedGeneration` _integer_ | observedGeneration is the most recent generation observed for this RayCluster. It corresponds to the<br />RayCluster's generation, which is updated on mutation by the API Server. |  |  |

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

## RayCronJobSpec

_Appears in:_
- [RayCronJob](#raycronjob)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `jobTemplate` _[RayJobTemplateSpec](#rayjobtemplatespec)_ | JobTemplate defines the RayJob that will be created by cron scheduling |  |  |
| `schedule` _string_ | Schedule is the cron schedule string |  |  |
| `timeZone` _string_ | TimeZone is the time zone name for the given schedule. If not specified, default to the local time zone of the<br />KubeRay operator. Empty string is not allowed.<br />The bundled version of the time zone database is used. |  | MinLength: 1 <br /> |
| `suspend` _boolean_ | Suspend tells the controller to suspend the scheduling, it does not apply to<br />scheduled RayJob. |  |  |

## RayCronJobStatus

RayCronJobStatus defines the observed state of RayCronJob

_Appears in:_
- [RayCronJob](#raycronjob)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `lastScheduleTime` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#time-v1-meta)_ |  |  |  |

## RayJobSpec

RayJobSpec defines the desired state of RayJob

_Appears in:_
- [RayJob](#rayjob)
- [RayJobTemplateSpec](#rayjobtemplatespec)

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

## RayJobStatus

RayJobStatus defines the observed state of RayJob

_Appears in:_
- [RayJob](#rayjob)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `rayJobInfo` _[RayJobStatusInfo](#rayjobstatusinfo)_ | INSERT ADDITIONAL STATUS FIELD - define observed state of cluster<br />Important: Run "make" to regenerate code after modifying this file<br />RayJobStatusInfo contains information about the Ray job retrieved from the Ray dashboard. |  |  |
| `jobId` _string_ |  |  |  |
| `rayClusterName` _string_ |  |  |  |
| `dashboardURL` _string_ |  |  |  |
| `jobStatus` _[JobStatus](#jobstatus)_ |  |  |  |
| `jobDeploymentStatus` _[JobDeploymentStatus](#jobdeploymentstatus)_ |  |  |  |
| `reason` _[JobFailedReason](#jobfailedreason)_ |  |  |  |
| `message` _string_ |  |  |  |
| `startTime` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#time-v1-meta)_ | StartTime is the time when JobDeploymentStatus transitioned from 'New' to 'Initializing'. |  |  |
| `endTime` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#time-v1-meta)_ | EndTime is the time when JobDeploymentStatus transitioned to 'Complete' status.<br />This occurs when the Ray job reaches a terminal state (SUCCEEDED, FAILED, STOPPED)<br />or the submitter Job has failed. |  |  |
| `succeeded` _integer_ | Succeeded is the number of times this job succeeded. | 0 |  |
| `failed` _integer_ | Failed is the number of times this job failed. | 0 |  |
| `rayClusterStatus` _[RayClusterStatus](#rayclusterstatus)_ | RayClusterStatus is the status of the RayCluster running the job. |  |  |
| `jobStatusCheckFailureStartTime` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#time-v1-meta)_ | JobStatusCheckFailureStartTime is set when job status checks via the Ray Dashboard started failing.<br />Reset on a successful check. JobDeploymentStatus transitions to 'Failed' after RAYJOB_STATUS_CHECK_TIMEOUT_SECONDS |  |  |
| `observedGeneration` _integer_ | observedGeneration is the most recent generation observed for this RayJob. It corresponds to the<br />RayJob's generation, which is updated on mutation by the API Server. |  |  |

## RayJobStatusInfo

`RayJobStatusInfo` is a subset of `RayJobInfo` from `dashboard_httpclient.py`.
This subset is used to store information in the CR status.

after careful consideration. In that case, we can remove `RayJobStatusInfo`.

_Appears in:_
- [RayJobStatus](#rayjobstatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `startTime` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#time-v1-meta)_ |  |  |  |
| `endTime` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#time-v1-meta)_ |  |  |  |

## RayJobTemplateSpec

RayJobTemplateSpec describes the data a RayJob should have when created from a template

_Appears in:_
- [RayCronJobSpec](#raycronjobspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[RayJobSpec](#rayjobspec)_ | Specification of the desired behavior of the RayJob |  |  |

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
| `suspend` _boolean_ | Suspend indicates whether the RayService should suspend its execution. When set to true,<br />all Kubernetes resources owned by the RayService controller will be deleted. Setting it<br />back to false will allow the RayService controller to recreate the resources. |  |  |

## RayServiceStatus

_Appears in:_
- [RayServiceStatuses](#rayservicestatuses)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `applicationStatuses` _object (keys:string, values:[AppStatus](#appstatus))_ | Important: Run "make" to regenerate code after modifying this file |  |  |
| `targetCapacity` _integer_ | TargetCapacity is the `target_capacity` percentage for all Serve replicas<br />across the cluster for this RayService. The `num_replicas`, `min_replicas`, `max_replicas`,<br />and `initial_replicas` for each deployment will be scaled by this percentage." |  |  |
| `trafficRoutedPercent` _integer_ | TrafficRoutedPercent is the percentage of traffic that is routed to the Serve service<br />for this RayService. TrafficRoutedPercent is updated to reflect the weight on the HTTPRoute<br />created for this RayService during incremental upgrades to a new cluster. |  |  |
| `lastTrafficMigratedTime` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#time-v1-meta)_ | LastTrafficMigratedTime is the last time that TrafficRoutedPercent was updated to a new value<br />for this RayService. |  |  |
| `rayClusterName` _string_ |  |  |  |
| `rayClusterStatus` _[RayClusterStatus](#rayclusterstatus)_ |  |  |  |

## RayServiceStatuses

RayServiceStatuses defines the observed state of RayService

_Appears in:_
- [RayService](#rayservice)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `conditions` _[Condition](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#condition-v1-meta) array_ | Represents the latest available observations of a RayService's current state. |  |  |
| `lastUpdateTime` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#time-v1-meta)_ | LastUpdateTime represents the timestamp when the RayService status was last updated. |  |  |
| `serviceStatus` _[ServiceStatus](#servicestatus)_ | Deprecated: `ServiceStatus` is deprecated - use `Conditions` instead. `Running` means the RayService is ready to<br />serve requests. An empty `ServiceStatus` means the RayService is not ready to serve requests. The definition of<br />`ServiceStatus` is equivalent to the `RayServiceReady` condition. |  |  |
| `activeServiceStatus` _[RayServiceStatus](#rayservicestatus)_ |  |  |  |
| `pendingServiceStatus` _[RayServiceStatus](#rayservicestatus)_ | Pending Service Status indicates a RayCluster will be created or is being created. |  |  |
| `numServeEndpoints` _integer_ | NumServeEndpoints indicates the number of Ray Pods that are actively serving or have been selected by the serve service.<br />Ray Pods without a proxy actor or those that are unhealthy will not be counted. |  |  |
| `observedGeneration` _integer_ | observedGeneration is the most recent generation observed for this RayService. It corresponds to the<br />RayService's generation, which is updated on mutation by the API Server. |  |  |

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

## ScaleGate

ScaleGate marks a worker group as not currently scalable. Type is the merge
key, so a gate is added and removed by exactly one controller. This API is
intended to be consistent with PodCondition:
https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.37/#podcondition-v1-core

_Appears in:_
- [ScaleStrategy](#scalestrategy)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `type` _string_ | Type uniquely identifies this gate and its owner, for example<br />"example.com/gate-name". |  |  |

## ScaleStrategy

ScaleStrategy controls scaling of a worker group.

_Appears in:_
- [WorkerGroupSpec](#workergroupspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `workersToDelete` _string array_ | WorkersToDelete workers to be deleted |  |  |
| `scaleGate` _[ScaleGate](#scalegate) array_ | ScaleGate is a signal written by an external controller to indicate that<br />this worker group cannot currently be scaled up. KubeRay preserves the<br />field across reconciles but never reads or writes it; the Ray Autoscaler<br />consumes it and falls back to another worker group while it is non-empty.<br />Each gate is keyed by its type. A writer must add or remove only its own<br />gates via Server-Side Apply under a distinct field manager; replacing the<br />list wholesale, or using read-modify-write Update, drops gates owned by<br />others. |  |  |

## ServeDeploymentStatus

ServeDeploymentStatus defines the current state of a Serve deployment

_Appears in:_
- [AppStatus](#appstatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `status` _string_ |  |  |  |
| `message` _string_ |  |  |  |

## ServiceStatus

_Underlying type:_ _string_

_Appears in:_
- [RayServiceStatuses](#rayservicestatuses)

| Field | Description |
| --- | --- |
| `Running` | `Running` means the RayService is ready to serve requests. `NotRunning` means it is not ready.<br />The naming is a bit confusing, but to maintain backward compatibility, we use `Running` instead of `Ready`.<br />Since KubeRay v1.3.0, `ServiceStatus` is equivalent to the `RayServiceReady` condition.<br />`ServiceStatus` is deprecated - please use conditions instead.<br /> |
| `` |  |

## SubmitterConfig

_Appears in:_
- [RayJobSpec](#rayjobspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `backoffLimit` _integer_ | BackoffLimit of the submitter. In K8sJobMode, this is the K8s Job backoffLimit.<br />In SidecarMode with SidecarSubmitterRestart enabled, this is the maximum container restart count. |  |  |

## TLSOptions

TLSOptions configures TLS encryption for the RayCluster.
When TLSOptions is nil or Enabled is nil/false, TLS is disabled.
When Enabled is true, the operator uses cert-manager to automatically
provision a full PKI (self-signed CA, head and worker leaf certificates)
and keeps certificates up to date as pod IPs change during autoscaling.

_Appears in:_
- [RayClusterSpec](#rayclusterspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `enabled` _boolean_ | Enabled controls whether mTLS is active for this RayCluster.<br />Defaults to false when omitted. Set to true to enable mTLS. |  |  |

## UpscalingMode

_Underlying type:_ _string_

_Validation:_
- Enum: [Default Aggressive Conservative]

_Appears in:_
- [AutoscalerOptions](#autoscaleroptions)

## WorkerGroupNetworkPolicyRules

WorkerGroupNetworkPolicyRules is NetworkPolicyRules bound to one worker group.

_Appears in:_
- [NetworkPolicyConfig](#networkpolicyconfig)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `groupName` _string_ | GroupName matches WorkerGroupSpec.GroupName. |  | Required: \{\} <br /> |
| `ingressRules` _[NetworkPolicyIngressRule](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#networkpolicyingressrule-v1-networking) array_ | IngressRules specifies custom ingress rules appended to the base policy.<br />Only meaningful when the mode includes ingress denial (DenyAll or DenyAllIngress). |  |  |
| `egressRules` _[NetworkPolicyEgressRule](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#networkpolicyegressrule-v1-networking) array_ | EgressRules specifies custom egress rules appended to the base policy.<br />Only meaningful when the mode includes egress denial (DenyAll or DenyAllEgress).<br />DNS egress is NOT added automatically: under DenyAll/DenyAllEgress you MUST<br />add a DNS rule here (e.g. to kube-system pods labeled k8s-app=kube-dns on<br />port 53), because Ray workers reach the head via its service FQDN and cannot<br />resolve it without DNS. See the network-policy-deny-all sample. |  |  |

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
| `priority` _integer_ | Priority influences which worker group the autoscaler prefers when multiple<br />groups can satisfy the same resource demand. Higher priority groups are<br />preferred for scale-up. Only honored by Ray Autoscaler v2 (Ray >= 2.56). | 0 |  |
| `resources` _object (keys:string, values:string)_ | Resources specifies the resource quantities for this worker group.<br />These values override the resources passed to `rayStartParams` for the group, but<br />have no effect on the resources set at the K8s Pod container level. |  |  |
| `labels` _object (keys:string, values:string)_ | Labels specifies the Ray node labels for this worker group.<br />These labels will also be added to the Pods of this worker group and override the `--labels`<br />argument passed to `rayStartParams`. |  |  |
| `rayStartParams` _object (keys:string, values:string)_ | RayStartParams are the params of the start command: address, object-store-memory, ... |  |  |
| `template` _[PodTemplateSpec](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#podtemplatespec-v1-core)_ | Template is a pod template for the worker |  |  |
| `scaleStrategy` _[ScaleStrategy](#scalestrategy)_ | ScaleStrategy controls scaling of this worker group: which pods to remove,<br />and whether the group can currently be scaled up. |  |  |
| `numOfHosts` _integer_ | NumOfHosts denotes the number of hosts to create per replica. The default value is 1. | 1 |  |
| `labelRefs` _[LabelRef](#labelref) array_ | LabelRefs delivers labels of the node each worker pod is bound to as Ray node labels.<br />This enables Ray scheduling based on node attributes, such as topology placement (e.g., rack, zone, or topology domain).<br />Only allowlisted node labels can be delivered. Every referenced label must exist on the node or pod startup fails.<br />Empty means no labels are delivered.<br />Requires the `NodeLabelDelivery` feature gate and `ENABLE_WEBHOOKS` enabled on the operator, and Ray 2.45.0 or later (`--labels-file`). |  |  |
