import inspect
import logging

from ray.train.v2.api.exceptions import ControllerError

logger = logging.getLogger(__name__)


class CallbackManager:
    def __init__(self, callbacks):
        self._callbacks = callbacks

    def _get_method(self, callback, hook_name: str):
        """Look up a hook method on a callback, raising if missing."""
        callback_name = type(callback).__name__
        method = getattr(callback, hook_name, None)
        if method is None or not callable(method):
            raise ControllerError(
                AttributeError(
                    f"Callback '{callback_name}' hook '{hook_name}' is missing "
                    "or not callable."
                )
            )
        return method, callback_name

    def invoke(self, hook_name: str, *args, **context) -> None:
        for callback in self._callbacks:
            method, callback_name = self._get_method(callback, hook_name)
            try:
                method(*args, **context)
            except Exception as e:
                # TODO: Enable configuration to suppress exceptions.
                logger.exception(
                    f"Exception raised in callback hook '{hook_name}' from callback "
                    f"'{callback_name}'."
                )
                raise ControllerError(e) from e

    async def async_invoke(self, hook_name: str, *args, **context) -> None:
        """Invoke a hook that may be sync or async (e.g. before_controller_shutdown)."""
        for callback in self._callbacks:
            method, callback_name = self._get_method(callback, hook_name)
            try:
                result = method(*args, **context)
                if inspect.isawaitable(result):
                    await result
            except Exception as e:
                logger.exception(
                    f"Exception raised in callback hook '{hook_name}' from callback "
                    f"'{callback_name}'."
                )
                raise ControllerError(e) from e
