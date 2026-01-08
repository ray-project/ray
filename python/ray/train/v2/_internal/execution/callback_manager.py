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
                    continue
                try:
                    result = exc_handler(hook_name, e, **context)
                except Exception as handler_exc:
                    logger.exception(
                        f"Exception raised in callback hook 'on_callback_hook_exception' "
                        f"from callback '{callback_name}' while handling hook '{hook_name}'"
                    )
                    return ControllerError(handler_exc)

                if not self._is_callback_hook_exception_handler_result(result):
                    e = TypeError(
                        "`on_callback_hook_exception` must return "
                        "(CallbackErrorAction, Optional[TrainingFailedError]), "
                        f"got {type(result)}"
                    )
                    return ControllerError(e)

                action, mapped_error = result
                if not mapped_error:
                    continue

                match action:
                    case CallbackErrorAction.SUPPRESS:
                        if mapped_error is not None:
                            logger.exception(
                                f"Exception raised in callback hook '{hook_name}' from callback '{callback_name}'."
                            )
                        continue
                    case CallbackErrorAction.RAISE:
                        return mapped_error
                    case _:
                        e = ValueError(f"Unknown CallbackErrorAction: {action}")
                        return ControllerError(e)

        return None

    def _is_callback_hook_exception_handler_result(self, result: object) -> bool:
        if not (isinstance(result, tuple) and len(result) == 2):
            return False
        action, mapped_error = result
        if not isinstance(action, CallbackErrorAction):
            return False
        if mapped_error is not None and not isinstance(
            mapped_error, TrainingFailedError
        ):
            return False
        return True
