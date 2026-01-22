import logging

from ray.train.v2._internal.execution.callback import CallbackErrorAction
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
                exc_handler = getattr(callback, "on_callback_hook_exception", None)
                if exc_handler is None:
                    # This should never happen, log it for debugging purposes.
                    logger.debug(
                        f"Exception raised in callback hook '{hook_name}' from callback "
                        f"'{callback_name}', but no 'on_callback_hook_exception' "
                        f"handler is implemented."
                    )
                    continue
                try:
                    result = exc_handler(hook_name, e, **context)
                except Exception as handler_exc:
                    logger.exception(
                        f"Exception raised in callback hook 'on_callback_hook_exception' "
                        f"from callback '{callback_name}' while handling hook '{hook_name}'"
                    )
                    return ControllerError(handler_exc)

                if not self._validate_handler_result(result):
                    e = TypeError(
                        "`on_callback_hook_exception` must return "
                        "(CallbackErrorAction, TrainingFailedError), "
                        f"got {type(result)}"
                    )
                    return ControllerError(e)

                action, mapped_error = result

                match action:
                    case CallbackErrorAction.SUPPRESS:
                        logger.exception(
                            f"Exception raised in callback hook '{hook_name}' from "
                            f"callback '{callback_name}'. Error: {mapped_error}"
                        )
                        continue
                    case CallbackErrorAction.RAISE:
                        return mapped_error
                    case _:
                        e = ValueError(f"Unknown CallbackErrorAction: {action}")
                        return ControllerError(e)

        return None

    def _validate_handler_result(self, result: object) -> bool:
        if not (isinstance(result, tuple) and len(result) == 2):
            return False
        action, mapped_error = result
        if mapped_error is None or not isinstance(mapped_error, TrainingFailedError):
            return False
        if not isinstance(action, CallbackErrorAction):
            return False
        return True
