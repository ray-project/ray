import logging
import os

import ray


def getLogger(name: str):
    """A utility function to help configure remote actor's logging level globally."""
    logger = logging.getLogger(name)
    try:
        worker_mode = ray._private.worker._mode()
        if worker_mode == ray._private.worker.WORKER_MODE:  # in remote actor
            logging_level = os.environ.get("EXE_REMOTE_ACTOR_LOGGING_LEVEL", None)
            if logging_level == "DEBUG":
                logger.setLevel(logging.DEBUG)
            elif logging_level == "INFO":
                logger.setLevel(logging.INFO)
    except Exception:
        pass

    return logger
