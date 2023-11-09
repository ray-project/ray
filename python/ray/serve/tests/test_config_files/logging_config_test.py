import logging

from ray import serve

logger = logging.getLogger("ray.serve")


@serve.deployment
class Model:
    def __call__(self):
        logger.debug("this_is_debug_info")
        logger.info("this_is_access_log", extra={"serve_access_log": True})

        log_file = logger.handlers[1].baseFilename

        return {
            "log_file": log_file,
            "replica": serve.get_replica_context().replica_tag,
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
        return log_info


model = Router.bind(Model.bind())


@serve.deployment(logging_config={"log_level": "DEBUG"})
class ModelWithConfig:
    def __call__(self):
        logger.debug("this_is_debug_info")
        log_file = logger.handlers[1].baseFilename
        return {"log_file": log_file}


model2 = ModelWithConfig.bind()
