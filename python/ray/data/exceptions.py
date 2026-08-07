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

    The frames corresponding to Ray Data internal files are always omitted
    from the stack trace shown on stdout. To also write the full stack trace
    (including those internal frames) to the Ray Data log file, set
    `DataContext.log_internal_stack_trace` to True."""

    pass


@DeveloperAPI
class SystemException(Exception):
    """Represents an Exception originating from Ray Data internal code
    or Ray Core private code paths, as opposed to user code. When
    Exceptions of this form are raised, it likely indicates a bug
    in Ray Data or Ray Core."""

    pass


@DeveloperAPI
class ExecutionTimeoutError(TimeoutError):
    """Represents an Exception raised when a Ray Data execution has made no
    progress for `DataContext.execution_no_progress_timeout_s` seconds while
    the consumer was waiting on the pipeline.

    The cause is often outside Ray Data: a UDF blocked on an external call, a
    cluster that can no longer schedule tasks after preemption, or a UDF that
    is simply slower than the timeout. It can also indicate a deadlock in Ray
    Data or Ray Core. Raise the timeout, or set it to a negative value to
    disable it, via `DataContext.execution_no_progress_timeout_s`."""


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
            # Whether to write the full internal Ray Data / Ray Core stack
            # frames to the Ray Data log file for user-code errors. When False
            # (the default), the log file gets only the cleaned worker-side
            # trace. Either way, the internal frames are never shown on stdout.
            log_internal_stack_trace = (
                DataContext.get_current().log_internal_stack_trace
            )
            if _is_ray_debugger_post_mortem_enabled():
                logger.exception("Full stack trace:")
                raise e

            is_user_code_exception = isinstance(e, UserCodeException)
            is_actionable_exception = is_user_code_exception or isinstance(
                e, ExecutionTimeoutError
            )
            if is_user_code_exception:
                # Exception has occurred in user code.
                if not log_internal_stack_trace and log_once(
                    "ray_data_exception_internal_hidden"
                ):
                    logger.error(
                        "Exception occurred in user code, with the abbreviated stack "
                        "trace below. The Ray Data internal stack frames are omitted "
                        "from stdout. To also write the full stack trace to the Ray "
                        f"Data log file at `{get_log_directory()}`, set "
                        "`DataContext.log_internal_stack_trace` to True."
                    )
            elif not is_actionable_exception:
                # Exception has occurred in internal Ray Data / Ray Core code.
                logger.error(
                    "Exception occurred in Ray Data or Ray Core internal code. "
                    "If you continue to see this error, please open an issue on "
                    "the Ray project GitHub page with the full stack trace below: "
                    "https://github.com/ray-project/ray/issues/new/choose"
                )

            if is_actionable_exception:
                # The driver-side propagation frames add nothing here: for a
                # user-code error the real failure is the worker traceback in
                # ``str(e)``, and for a timeout the message is self-contained.
                # Keep them off stdout always (``hide=True`` filters the console
                # handler only; the file handler still writes the record). The
                # flag controls only what reaches the log file.
                if log_internal_stack_trace:
                    logger.exception(
                        "Full stack trace:", exc_info=True, extra={"hide": True}
                    )
                else:
                    logger.error("Full stack trace:\n%s", e, extra={"hide": True})
            else:
                # System exception (likely a Ray bug): surface the full trace on
                # stdout (and the log file) so the user can report it.
                logger.exception("Full stack trace:", exc_info=True)
            if is_actionable_exception:
                raise e.with_traceback(None)
            else:
                raise e.with_traceback(None) from SystemException()

    return handle_trace
