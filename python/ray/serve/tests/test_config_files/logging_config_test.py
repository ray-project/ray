import logging

import ray
from ray import serve
from ray.exceptions import RayActorError
from ray.serve.context import _get_global_client

logger = logging.getLogger("ray.serve")


@serve.deployment
class Model:
    def __call__(self):
        logger.debug("this_is_debug_info")
        logger.info("this_is_access_log", extra={"serve_access_log": True})

        log_file = logger.handlers[1].baseFilename

        return {
            "log_file": log_file,
            "replica": serve.get_replica_context().replica_id.to_full_id_str(),
            "log_level": logger.level,
            "num_handlers": len(logger.handlers),
        }


@serve.deployment
class Router:
    def __init__(self, handle):
        self.handle = handle

    async def __call__(self):
        logger.debug("this_is_debug_info_from_router")
        log_info = await self.handle.remote()
        if len(logger.handlers) == 2:
            log_info["router_log_file"] = logger.handlers[1].baseFilename
        else:
            log_info["router_log_file"] = None
        log_info["router_log_level"] = logger.level

        try:
            # Add controller log file path
            client = _get_global_client()
            _, log_file_path = ray.get(client._controller._get_logging_config.remote())
        except RayActorError:
            log_file_path = None
        log_info["controller_log_file"] = log_file_path
        return log_info


model = Router.bind(Model.bind())


@serve.deployment(logging_config={"log_level": "DEBUG"})
class ModelWithConfig:
    def __call__(self):
        logger.debug("this_is_debug_info")
        log_file = logger.handlers[1].baseFilename
        return {"log_file": log_file}


model2 = ModelWithConfig.bind()
