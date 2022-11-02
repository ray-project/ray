import datetime
import functools
import inspect
import logging
import os
import pathlib
import re
import sys
from logging.config import dictConfig
from typing import Callable, Iterable, List, Optional, Union

# Set the number of columns to use for rich output when in jupyter
os.environ["JUPYTER_COLUMNS"] = "160"

try:
    import rich
    import rich.logging

    FormatTimeCallable = Callable[[datetime.datetime], rich.text.Text]

    class RayLogRender(rich._log_render.LogRender):
        """Render a log record (renderables) to a rich console.

        This class stores some console state; for example, the last logged time, which
        is used in formatting subsequent messages.

        This code was modified from rich._log_render.LogRender, so see that
        for more information.
        """

        def __call__(
            self,
            console: rich.console.Console,
            renderables: Iterable[rich.console.ConsoleRenderable],
            log_time: Optional[datetime.datetime] = None,
            time_format: Optional[Union[str, FormatTimeCallable]] = None,
            level: rich.text.TextType = "",
            path: Optional[str] = None,
            line_no: Optional[int] = None,
            link_path: Optional[str] = None,
            package: Optional[str] = None,
        ) -> rich.table.Table:
            """Render a log message to the rich console.

            console: Console to write the message to
            renderables: A list containing either 1 or 2 items: the log message and
                optionally a traceback

            Returns
            -------
                A table contianing the log message.
            """
            output = rich.table.Table.grid(padding=(0, 1), expand=True)

            # If this is run on CI, set a sensible table width
            if "BUILDKITE_JOB_ID" in os.environ or "PYTEST_CURRENT_TEST" in os.environ:
                output.min_width = 140

            if "RICH_CONSOLE_WIDTH" in os.environ:
                output.width = int(os.environ["RICH_CONSOLE_WIDTH"])

            if self.show_time:
                output.add_column(style="log.time")

            # Ray package column
            output.add_column(style="yellow")

            if self.show_level:
                output.add_column(style="log.level", width=self.level_width)

            output.add_column(ratio=1, style="log.message", overflow="fold")

            if self.show_path and path:
                output.add_column(style="log.path", justify="right")

            row: List[rich.console.RenderableType] = []
            if self.show_time:
                log_time = log_time or console.get_datetime()
                time_format = time_format or self.time_format
                if callable(time_format):
                    log_time_display = time_format(log_time)
                else:
                    log_time_display = rich.text.Text(log_time.strftime(time_format))

                if log_time_display == self._last_time and self.omit_repeated_times:
                    row.append(rich.text.Text(" " * len(log_time_display)))
                else:
                    row.append(log_time_display)
                    self._last_time = log_time_display

            if package:
                row.append(rich.text.Text(f"[{package}]"))
            else:
                row.append(rich.text.Text(""))

            if self.show_level:
                row.append(level)

            row.append(rich.containers.Renderables(renderables))
            if self.show_path and path:
                path_text = rich.text.Text()
                path_text.append(
                    path, style=f"link file://{link_path}" if link_path else ""
                )
                if line_no:
                    path_text.append(":")
                    path_text.append(
                        f"{line_no}",
                        style=f"link file://{link_path}#{line_no}" if link_path else "",
                    )
                row.append(path_text)

            output.add_row(*row)
            return output

    class RichRayHandler(rich.logging.RichHandler):
        """Rich log handler, used to handle logs if rich is installed."""

        def __init__(
            self,
            *args,
            show_time: bool = True,
            omit_repeated_times: bool = True,
            show_level: bool = True,
            show_path: bool = True,
            highlighter: rich.highlighter.Highlighter = None,
            log_time_format: Union[str, FormatTimeCallable] = "[%x %X]",
            **kwargs,
        ):
            # Rich highlighter conflicts with colorama; disable here
            if not highlighter:
                highlighter = rich.highlighter.NullHighlighter()

            rich.logging.RichHandler.__init__(
                self, *args, highlighter=highlighter, rich_tracebacks=False, **kwargs
            )

            self._log_render = RayLogRender(
                show_time=show_time,
                show_level=show_level,
                show_path=show_path,
                time_format=log_time_format,
                omit_repeated_times=omit_repeated_times,
                level_width=None,
            )

            # Needed because the existing logging system causes workers to write to
            # a file that is read by the driver. This will need to be removed when
            # a proper logging system is set up in a way that lets workers pass
            # LogRecords to the driver.
            self.plain_handler = logging.StreamHandler()
            self.plain_handler.level = self.level
            self.plain_handler.formatter = logging.Formatter(fmt="%(message)s")

        def emit(self, record: logging.LogRecord):
            """Emit the log message.

            If this is a worker, dump the record to stdout as usual, where
            it will get routed into a file to be read by the driver later.
            If this is the driver, emit the message using the rich handler.

            Args:
                record: Log record to be emitted
            """
            import ray

            if (
                hasattr(ray, "_private")
                and ray._private.worker.global_worker.mode
                == ray._private.worker.WORKER_MODE
            ):
                self.plain_handler.emit(record)
            else:
                rich.logging.RichHandler.emit(self, record)

        def render(
            self,
            *,
            record: logging.LogRecord,
            traceback: Optional[rich.traceback.Traceback],
            message_renderable: rich.console.ConsoleRenderable,
        ) -> rich.console.ConsoleRenderable:
            """Render log for display.

            Args:
                record: logging Record.
                traceback: Traceback instance or None for no Traceback.
                message_renderable: Renderable (typically Text) containing log message
                    contents.

            Returns:
                Renderable to display log.
            """
            path = pathlib.Path(record.pathname).name
            level = self.get_level_text(record)
            time_format = None if self.formatter is None else self.formatter.datefmt
            log_time = datetime.datetime.fromtimestamp(record.created)

            log_renderable = self._log_render(
                self.console,
                [message_renderable]
                if not traceback
                else [message_renderable, traceback],
                log_time=log_time,
                time_format=time_format,
                level=level,
                path=path,
                line_no=record.lineno,
                link_path=record.pathname if self.enable_link_path else None,
                package=record.package,
            )
            return log_renderable

except ImportError:
    rich = None


class ContextFilter(logging.Filter):
    """A filter that adds ray context info to log records.

    This filter adds a package name to append to the message as well as information
    about what worker emitted the message, if applicable.
    """

    logger_regex = re.compile(r"ray(\.(?P<subpackage>\w+))?(\..*)?")
    package_message_names = {
        "air": "AIR",
        "data": "Data",
        "rllib": "RLlib",
        "serve": "Serve",
        "train": "Train",
        "tune": "Tune",
        "workflow": "Workflow",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.global_worker_mode = None

    def filter(self, record: logging.LogRecord) -> bool:
        """Add context information to the log record.

        This filter adds a package name from where the message was generated as
        well as the worker IP address, if applicable.

        Args:
            record: Record to be filtered

        Returns:
            True if the record is to be logged, False otherwise. (This filter only
            adds context, so records are always logged.)
        """
        match = self.logger_regex.search(record.name)
        if match:
            record.package = (
                f"Ray {self.package_message_names.get(match['subpackage'], 'Core')}"
            )
        else:
            record.package = "Ray Core"

        # Lazily load ray when this is called to avoid circular import
        import ray

        if hasattr(ray, "_private"):
            if self.global_worker_mode is None:
                self.global_worker_mode = ray._private.worker.global_worker.mode

            if (
                self.global_worker_mode
                and self.global_worker_mode == ray._private.worker.WORKER_MODE
            ):
                record.node_ip_address = (
                    ray.runtime_context.get_runtime_context().worker.node_ip_address
                )

        return True


class AnsiStripFormatter(logging.Formatter):
    """Formatter which strips ANSI escape codes from log messages.

    These escape codes produce noise in output files and conflict with rich logging,
    and so are stripped here.
    """

    strip_ansi_regex = re.compile(r"\x1b\[[0-9;]*m")

    def format(self, record):
        return re.sub(self.strip_ansi_regex, "", super().format(record))


class PlainRayHandler(logging.StreamHandler):
    """Plain log handler, used as fallback if rich is not installed."""

    def __init__(self):
        logging.StreamHandler.__init__(self, stream=sys.stdout)

        # Flag to know if a warning about rich not being installed be emitted
        self.rich_warning = True

        # Needed because the existing logging system causes workers to write to
        # a file that is read by the driver. This will need to be removed when
        # a proper logging system is set up in a way that lets workers pass
        # LogRecords to the driver.
        self.plain_handler = logging.StreamHandler()
        self.plain_handler.level = self.level
        self.plain_handler.formatter = logging.Formatter(fmt="%(message)s")

    def emit(self, record: logging.LogRecord):
        """Emit the log message.

        If this is a worker, serialize the record and send it to the driver via TCP.
        If this is the driver, emit the message using the appropriate console handler.

        Also emits a warning to the user if `rich` is not installed if this is being
        run on the driver; this warning needs to be here to avoid circular import
        issues.

        Args:
            record: Log record to be emitted
        """
        import ray

        if (
            hasattr(ray, "_private")
            and ray._private.worker.global_worker.mode
            == ray._private.worker.WORKER_MODE
        ):
            self.plain_handler.emit(record)
        else:
            logging.StreamHandler.emit(self, record)

            if self.rich_warning:
                self.rich_warning = False
                logger = logging.getLogger(__name__)
                logger.warn(
                    "rich is not installed. Run `pip install rich` for"
                    " improved logging, progress, and tracebacks."
                )


def call_once(func: Callable[[None], None]):
    """Decorator for a function that should only run once.

    Args:
        func: Function to wrap.

    Returns:
        A function that only runs once; other calls generate debug messages and do
        not run.
    """

    @functools.wraps(func)
    def wrapped(force=False):
        caller = inspect.stack()[-1]

        if not (wrapped.last_caller or force):
            wrapped.last_caller = caller
            func()
        else:
            logging.getLogger(__name__).debug(
                "Ray logging has already been configured at "
                f"{wrapped.last_caller.filename}::{wrapped.last_caller.lineno}."
                " Skipping reconfiguration requested at "
                f"{caller.filename}::{caller.lineno}."
            )

    wrapped.last_caller = None
    return wrapped


@call_once
def generate_logging_config():
    """Generate the default Ray logging configuration.

    If rich is installed, a logger which generates output using the rich console is
    used. Otherwise, a simple fallback logging format is used, and a warning message
    is emitted asking the user to install rich for better formatting.

    In either case, if the handler is run on a worker, it serializes the LogRecord
    and sends it to the driver via TCP. If the handler is run on the driver, it is
    emitted as usual.
    """
    formatters = {
        "rich": {
            "()": AnsiStripFormatter,
            "datefmt": "[%Y-%m-%d %H:%M:%S]",
            "format": "%(message)s",
        },
        "plain": {
            "datefmt": "[%Y-%m-%d %H:%M:%S]",
            "format": "%(asctime)s [%(package)s] %(levelname)s %(name)s::%(message)s",
        },
    }
    filters = {"context_filter": {"()": ContextFilter}}
    handlers = {
        "null": {"class": "logging.NullHandler"},
    }

    force_plain = os.environ.get("FORCE_PLAIN_LOGGING", False)
    if isinstance(force_plain, str) and force_plain.isdecimal():
        force_plain = int(force_plain)

    if rich and not force_plain:
        handlers["default"] = {
            "()": RichRayHandler,
            "formatter": "rich",
            "filters": ["context_filter"],
        }
    else:
        handlers["default"] = {
            "()": PlainRayHandler,
            "formatter": "plain",
            "filters": ["context_filter"],
        }

    loggers = {
        # Default ray logger; any log message that gets propagated here will be logged
        # to the console
        "ray": {
            "level": "INFO",
            "handlers": ["default"],
        },
        # Special handling for ray.rllib: only warning-level messages passed through
        # See https://github.com/ray-project/ray/pull/31858 for related PR
        "ray.rllib": {
            "level": "WARN",
        },
    }

    dictConfig(
        {
            "version": 1,
            "formatters": formatters,
            "filters": filters,
            "handlers": handlers,
            "loggers": loggers,
        }
    )
