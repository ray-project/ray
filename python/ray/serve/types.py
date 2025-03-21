from enum import Enum

from ray.util.annotations import PublicAPI


# Keep in sync with ServeSystemActorStatus in
# python/ray/dashboard/client/src/type/serve.ts
@PublicAPI(stability="stable")
class ProxyStatus(str, Enum):
    """The current status of the proxy."""

    STARTING = "STARTING"
    HEALTHY = "HEALTHY"
    UNHEALTHY = "UNHEALTHY"
    DRAINING = "DRAINING"
    # The DRAINED status is a momentary state
    # just before the proxy is removed
    # so this status won't show up on the dashboard.
    DRAINED = "DRAINED"


@PublicAPI(stability="stable")
class ApplicationStatus(str, Enum):
    """The current status of an application.

    Values:
        NOT_STARTED: The application has not started yet.
        DEPLOYING: The application is currently being deployed.
        DEPLOY_FAILED: The application failed during deployment.
        RUNNING: The application is running successfully.
        UNHEALTHY: The application is running but in an unhealthy state.
        DELETING: The application is being deleted.
    """

    NOT_STARTED = "NOT_STARTED"
    DEPLOYING = "DEPLOYING"
    DEPLOY_FAILED = "DEPLOY_FAILED"
    RUNNING = "RUNNING"
    UNHEALTHY = "UNHEALTHY"
    DELETING = "DELETING"


# Keep in sync with ServeReplicaState in dashboard/client/src/type/serve.ts
class ReplicaState(str, Enum):
    STARTING = "STARTING"
    UPDATING = "UPDATING"
    RECOVERING = "RECOVERING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    PENDING_MIGRATION = "PENDING_MIGRATION"


ALL_REPLICA_STATES = list(ReplicaState)


class DeploymentStatus(str, Enum):
    """The current status of a deployment.

    Values:
        UPDATING: The deployment has been deployed / re-deployed.
        HEALTHY: The deployment is healthy and running at the target
            number of replicas.
        UNHEALTHY: The deployment is unhealthy, most likely because of
            replica failures. This can be a transient state.
        DEPLOY_FAILED: The new deployment version errored. This is a
            terminal state and requires re-deploy.
        UPSCALING: The deployment is upscaling the number of replicas.
        DOWNSCALING: The deployment is downscaling the number of replicas.
    """

    UPDATING = "UPDATING"
    HEALTHY = "HEALTHY"
    UNHEALTHY = "UNHEALTHY"
    DEPLOY_FAILED = "DEPLOY_FAILED"
    UPSCALING = "UPSCALING"
    DOWNSCALING = "DOWNSCALING"


class DeploymentStatusTrigger(str, Enum):
    UNSPECIFIED = "UNSPECIFIED"
    CONFIG_UPDATE_STARTED = "CONFIG_UPDATE_STARTED"
    CONFIG_UPDATE_COMPLETED = "CONFIG_UPDATE_COMPLETED"
    UPSCALE_COMPLETED = "UPSCALE_COMPLETED"
    DOWNSCALE_COMPLETED = "DOWNSCALE_COMPLETED"
    AUTOSCALING = "AUTOSCALING"
    REPLICA_STARTUP_FAILED = "REPLICA_STARTUP_FAILED"
    HEALTH_CHECK_FAILED = "HEALTH_CHECK_FAILED"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    DELETING = "DELETING"


@PublicAPI(stability="alpha")
class EncodingType(str, Enum):
    """Encoding type for the serve logs."""

    TEXT = "TEXT"
    JSON = "JSON"


@PublicAPI(stability="alpha")
class APIType(str, Enum):
    """Tracks the type of API that an application originates from."""

    UNKNOWN = "unknown"
    IMPERATIVE = "imperative"
    DECLARATIVE = "declarative"


class ServeDeployMode(str, Enum):
    MULTI_APP = "MULTI_APP"
