import logging

from ray import serve

logger = logging.getLogger("ray.serve")


@serve.deployment
class Model:
    def __call__(self):
        logger.debug("this_is_debug_info")

        handlers_state = None
        log_file = None
        if len(logger.handlers) == 2:
            handlers_state = "ALL"
            log_file = logger.handlers[1].baseFilename
        elif len(logger.handlers) == 1 and isinstance(
            logger.handlers[0], logging.handlers.RotatingFileHandler
        ):
            handlers_state = "FILE_ONLY"
            log_file = logger.handlers[0].baseFilename
        elif len(logger.handlers) == 1 and isinstance(
            logger.handlers[0], logging.StreamHandler
        ):
            handlers_state = "STREAM_ONLY"
        else:
            handlers_state = "DISABLE"

        return {
            "log_file": log_file,
            "replica": serve.get_replica_context().replica_tag,
            "log_level": logger.level,
            "handlers_state": handlers_state,
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
        print("router_log_level: ", logger.level)
        log_info["router_log_level"] = logger.level
        return log_info


model = Router.bind(Model.bind())
