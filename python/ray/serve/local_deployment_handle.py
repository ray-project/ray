import logging

from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve.deployment import Deployment

logger = logging.getLogger(SERVE_LOGGER_NAME)


class LocalDeploymentHandleResponse:
    pass


class LocalDeploymentHandle:
    def __init__(self, deployment: Deployment):
        # TODO: validate things and yell if options aren't respected.
        self._deployment = deployment
        self._user_callable = None
        self._method = "__call__"

    def do_init(self):
        logger.info(
            f"Initializing local replica for deployment '{self._deployment.name}'"
        )
        self._user_callable = self._deployment.func_or_class(
            *self._deployment.init_args,
            **self._deployment.init_kwargs,
        )

    def __getattr__(self, name: str):
        self._method = name
        return self

    def remote(self, *args, **kwargs):
        output = getattr(self._user_callable, self._method)(*args, **kwargs)

        class Wrapper:
            def result(self):
                return output

        return Wrapper()

    @classmethod
    def _deserialize(cls, kwargs):
        return cls(**kwargs)

    def __reduce__(self):
        return self.__class__._deserialize, ({"deployment": self._deployment},)
