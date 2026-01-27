import logging

from ray.train.v2.api.exceptions import ControllerError, TrainingFailedError

logger = logging.getLogger(__name__)


class CallbackManager:
    def __init__(self, callbacks):
        self._callbacks = callbacks

    # change this return type later
    def invoke(self, hook_name: str, *args, **context) -> TrainingFailedError | None:
        for callback in self._callbacks:
            callback_name = type(callback).__name__
            method = getattr(callback, hook_name, None)
            if not method:
                continue
            try:
                method(*args, **context)
            except Exception as e:
                # TODO: Enable configuration to suppress exceptions.
                logger.exception(
                    f"Exception raised in callback hook '{hook_name}' from callback "
                    f"'{callback_name}'."
                )
                return ControllerError(e)

        return None
