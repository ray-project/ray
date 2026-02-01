import logging

from ray.train.v2.api.exceptions import ControllerError

logger = logging.getLogger(__name__)


class CallbackManager:
    def __init__(self, callbacks):
        self._callbacks = callbacks

    # change this return type later
    def invoke(self, hook_name: str, *args, **context) -> None:
        for callback in self._callbacks:
            callback_name = type(callback).__name__
            method = getattr(callback, hook_name, None)
            if method is None or not callable(method):
                raise ControllerError(
                    AttributeError(
                        f"Callback '{callback_name}' hook '{hook_name}' is missing "
                        "or not callable."
                    )
                )
            try:
                method(*args, **context)
            except Exception as e:
                # TODO: Enable configuration to suppress exceptions.
                logger.exception(
                    f"Exception raised in callback hook '{hook_name}' from callback "
                    f"'{callback_name}'."
                )
                raise ControllerError(e) from e
