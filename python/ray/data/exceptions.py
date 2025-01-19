import logging
from typing import Callable

from ray.data._internal.logging import get_log_directory
from ray.data.context import DataContext
from ray.exceptions import UserCodeException
from ray.util import log_once
from ray.util.annotations import DeveloperAPI
from ray.util.rpdb import _is_ray_debugger_post_mortem_enabled

logger = logging.getLogger(__name__)


@DeveloperAPI
class RayDataUserCodeException(UserCodeException):
    """Represents an Exception originating from user code, e.g.
    user-specified UDF used in a Ray Data transformation.

    By default, the frames corresponding to Ray Data internal files are
    omitted from the stack trace logged to stdout, but will still be
    emitted to the Ray Data specific log file. To emit all stack frames to stdout,
    set `DataContext.log_internal_stack_trace_to_stdout` to True."""

    pass


@DeveloperAPI
class SystemException(Exception):
    """Represents an Exception originating from Ray Data internal code
    or Ray Core private code paths, as opposed to user code. When
    Exceptions of this form are raised, it likely indicates a bug
    in Ray Data or Ray Core."""

    pass


@DeveloperAPI
def omit_traceback_stdout(fn: Callable) -> Callable:
    """Decorator which runs the function, and if there is an exception raised,
    drops the stack trace before re-raising the exception. The original exception,
    including the full unmodified stack trace, is always written to the Ray Data
    log file at `data_exception_logger._log_path`.

    This is useful for stripping long stack traces of internal Ray Data code,
    which can otherwise obfuscate user code errors."""

    def handle_trace(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            # Only log the full internal stack trace to stdout when configured
            # via DataContext, or when the Ray Debugger is enabled.
            # The full stack trace will always be emitted to the Ray Data log file.
            log_to_stdout = DataContext.get_current().log_internal_stack_trace_to_stdout
            if _is_ray_debugger_post_mortem_enabled():
                logger.exception("Full stack trace:")
                raise e

            is_user_code_exception = isinstance(e, UserCodeException)
            if is_user_code_exception:
                # Exception has occurred in user code.
                if not log_to_stdout and log_once("ray_data_exception_internal_hidden"):
                    logger.error(
                        "Exception occurred in user code, with the abbreviated stack "
                        "trace below. By default, the Ray Data internal stack trace "
                        "is omitted from stdout, and only written to the Ray Data log "
                        f"files at {get_log_directory()}. To "
                        "output the full stack trace to stdout, set "
                        "`DataContext.log_internal_stack_trace_to_stdout` to True."
                    )
            else:
                # Exception has occurred in internal Ray Data / Ray Core code.
                logger.error(
                    "Exception occurred in Ray Data or Ray Core internal code. "
                    "If you continue to see this error, please open an issue on "
                    "the Ray project GitHub page with the full stack trace below: "
                    "https://github.com/ray-project/ray/issues/new/choose"
                )

            should_hide_traceback = is_user_code_exception and not log_to_stdout
            logger.exception(
                "Full stack trace:",
                exc_info=True,
                extra={"hide": should_hide_traceback},
            )
            if is_user_code_exception:
                raise e.with_traceback(None)
            else:
                raise e.with_traceback(None) from SystemException()

    return handle_trace
