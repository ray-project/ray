import logging

from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.replica import UserCallableWrapper
from ray.serve.deployment import Deployment
from ray.serve._private.common import DeploymentID, RequestMetadata

logger = logging.getLogger(SERVE_LOGGER_NAME)


class LocalDeploymentHandleResponse:
    pass


class LocalDeploymentHandle:
    def __init__(self, deployment: Deployment, app_name: str):
        # TODO: validate deployment options and yell if they aren't respected?
        self._deployment = deployment
        self._deployment_id = DeploymentID(deployment.name, app_name=app_name)
        self._user_callable_wrapper = None

    def do_init(self):
        # TODO: timeout?
        logger.info(
            f"Initializing local replica for deployment '{self._deployment.name}'"
        )
        # TODO: has to be done after building app or else can't pickle.
        self._user_callable_wrapper = UserCallableWrapper(
            self._deployment.func_or_class,
            self._deployment.init_args,
            self._deployment.init_kwargs,
            deployment_id=self._deployment_id,
        )
        self._user_callable_wrapper.initialize_callable().result()

        # TODO: call the user health check.

    def __getattr__(self, name: str):
        raise NotImplementedError("Only __call__ implemented.")

    def remote(self, *args, **kwargs):
        f = self._user_callable_wrapper.call_user_method(
            RequestMetadata(request_id="test-request-id", internal_request_id="test-internal-request-id"),
            args,
            kwargs,
        )

        class Wrapper:
            def result(self):
                return f.result()

        return Wrapper()

    @classmethod
    def _deserialize(cls, kwargs):
        return cls(**kwargs)

    def __reduce__(self):
        return self.__class__._deserialize, ({"deployment": self._deployment},)

    def __repr__(self) -> str:
        return f"LocalDeploymentHandle({self._deployment_id})"
