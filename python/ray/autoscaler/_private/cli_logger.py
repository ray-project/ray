"""Logger implementing the Command Line Interface.

A replacement for the standard Python `logging` API
designed for implementing a better CLI UX for the cluster launcher.

Supports color, bold text, italics, underlines, etc.
(depending on TTY features)
as well as indentation and other structured output.
"""
import inspect
import logging
import os
import sys
import time
from contextlib import contextmanager
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Tuple

import click
import colorama

# Import ray first to use the bundled colorama
import ray  # noqa: F401

if sys.platform == "win32":
    import msvcrt
else:
    import select


class _ColorfulMock:
    def __init__(self):
        # do not do any color work
        self.identity = lambda x: x

        self.colorful = self
        self.colormode = None

        self.NO_COLORS = None
        self.ANSI_8_COLORS = None

    def disable(self):
        pass

    @contextmanager
    def with_style(self, x):
        class IdentityClass:
            def __getattr__(self, name):
                return lambda y: y

        yield IdentityClass()

    def __getattr__(self, name):
        if name == "with_style":
            return self.with_style

        return self.identity


try:
    import colorful as _cf
    from colorful.core import ColorfulString

    _cf.use_8_ansi_colors()
except ModuleNotFoundError:
    # We mock Colorful to restrict the colors used for consistency
    # anyway, so we also allow for not having colorful at all.
    # If the Ray Core dependency on colorful is ever removed,
    # the CliLogger code will still work.
    class ColorfulString:
        pass

    _cf = _ColorfulMock()


# We want to only allow specific formatting
# to prevent people from accidentally making bad looking color schemes.
#
# This is especially important since most will look bad on either light
# or dark themes.
class _ColorfulProxy:
    _proxy_allowlist = [
        "disable",
        "reset",
        "bold",
        "italic",
        "underlined",
        # used instead of `gray` as `dimmed` adapts to
        # both light and dark themes
        "dimmed",
        "dodgerBlue",  # group
        "limeGreen",  # success
        "red",  # error
        "orange",  # warning
        "skyBlue",  # label
        "magenta",  # syntax highlighting key words and symbols
        "yellow",  # syntax highlighting strings
    ]

    def __getattr__(self, name):
        res = getattr(_cf, name)
        if callable(res) and name not in _ColorfulProxy._proxy_allowlist:
            raise ValueError(
                "Usage of the colorful method '" + name + "' is forbidden "
                "by the proxy to keep a consistent color scheme. "
                "Check `cli_logger.py` for allowed methods"
            )
        return res


cf = _ColorfulProxy()

colorama.init(strip=False)


def _patched_makeRecord(
    self, name, level, fn, lno, msg, args, exc_info, func=None, extra=None, sinfo=None
):
    """Monkey-patched version of logging.Logger.makeRecord
    We have to patch default loggers so they use the proper frame for
    line numbers and function names (otherwise everything shows up as
    e.g. cli_logger:info() instead of as where it was called from).

    In Python 3.8 we could just use stacklevel=2, but we have to support
    Python 3.6 and 3.7 as well.

    The solution is this Python magic superhack.

    The default makeRecord will deliberately check that we don't override
    any existing property on the LogRecord using `extra`,
    so we remove that check.

    This patched version is otherwise identical to the one in the standard
    library.

    TODO: Remove this magic superhack. Find a more responsible workaround.
    """
    rv = logging._logRecordFactory(
        name, level, fn, lno, msg, args, exc_info, func, sinfo
    )
    if extra is not None:
        rv.__dict__.update(extra)
    return rv


logging.Logger.makeRecord = _patched_makeRecord


def _external_caller_info():
    """Get the info from the caller frame.

    Used to override the logging function and line number with the correct
    ones. See the comment on _patched_makeRecord for more info.
    """

    frame = inspect.currentframe()
    caller = frame
    levels = 0
    while caller.f_code.co_filename == __file__:
        caller = caller.f_back
        levels += 1
    return {
        "lineno": caller.f_lineno,
        "filename": os.path.basename(caller.f_code.co_filename),
    }


def _format_msg(
    msg: str,
    *args: Any,
    no_format: bool = None,
    _tags: Dict[str, Any] = None,
    _numbered: Tuple[str, int, int] = None,
    **kwargs: Any,
):
    """Formats a message for printing.

    Renders `msg` using the built-in `str.format` and the passed-in
    `*args` and `**kwargs`.

    Args:
        *args (Any): `.format` arguments for `msg`.
        no_format (bool):
            If `no_format` is `True`,
            `.format` will not be called on the message.

            Useful if the output is user-provided or may otherwise
            contain an unexpected formatting string (e.g. "{}").
        _tags (Dict[str, Any]):
            key-value pairs to display at the end of
            the message in square brackets.

            If a tag is set to `True`, it is printed without the value,
            the presence of the tag treated as a "flag".

            E.g. `_format_msg("hello", _tags=dict(from=mom, signed=True))`
                 `hello [from=Mom, signed]`
        _numbered (Tuple[str, int, int]):
            `(brackets, i, n)`

            The `brackets` string is composed of two "bracket" characters,
            `i` is the index, `n` is the total.

            The string `{i}/{n}` surrounded by the "brackets" is
            prepended to the message.

            This is used to number steps in a procedure, with different
            brackets specifying different major tasks.

            E.g. `_format_msg("hello", _numbered=("[]", 0, 5))`
                 `[0/5] hello`

    Returns:
        The formatted message.
    """

    if isinstance(msg, str) or isinstance(msg, ColorfulString):
        tags_str = ""
        if _tags is not None:
            tags_list = []
            for k, v in _tags.items():
                if v is True:
                    tags_list += [k]
                    continue
                if v is False:
                    continue

                tags_list += [k + "=" + v]
            if tags_list:
                tags_str = cf.reset(cf.dimmed(" [{}]".format(", ".join(tags_list))))

        numbering_str = ""
        if _numbered is not None:
            chars, i, n = _numbered
            numbering_str = cf.dimmed(chars[0] + str(i) + "/" + str(n) + chars[1]) + " "

        if no_format:
            # todo: throw if given args/kwargs?
            return numbering_str + msg + tags_str
        return numbering_str + msg.format(*args, **kwargs) + tags_str

    if kwargs:
        raise ValueError("We do not support printing kwargs yet.")

    res = [msg, *args]
    res = [str(x) for x in res]
    return ", ".join(res)


# TODO: come up with a plan to unify logging.
# formatter = logging.Formatter(
#     # TODO(maximsmol): figure out the required log level padding
#     #                  width automatically
#     fmt="[{asctime}] {levelname:6} {message}",
#     datefmt="%x %X",
#     # We want alignment on our level names
#     style="{")


def _isatty():
    """More robust check for interactive terminal/tty."""
    try:
        # https://stackoverflow.com/questions/6108330/
        # checking-for-interactive-shell-in-a-python-script
        return sys.__stdin__.isatty()
    except Exception:
        # sometimes this can fail due to closed output
        # either way, no-tty is generally safe fallback.
        return False


class _CliLogger:
    """Singleton class for CLI logging.

    Without calling 'cli_logger.configure', the CLILogger will default
    to 'record' style logging.

    Attributes:
        color_mode (str):
            Can be "true", "false", or "auto".

            Enables or disables `colorful`.

            If `color_mode` is "auto", is set to `not stdout.isatty()`
        indent_level (int):
            The current indentation level.

            All messages will be indented by prepending `"  " * indent_level`
        vebosity (int):
            Output verbosity.

            Low verbosity will disable `verbose` and `very_verbose` messages.
    """

    color_mode: str
    # color_mode: Union[Literal["auto"], Literal["false"], Literal["true"]]
    indent_level: int
    interactive: bool
    VALID_LOG_STYLES = ("auto", "record", "pretty")

    _autodetected_cf_colormode: int

    def __init__(self):
        self.indent_level = 0

        self._verbosity = 0
        self._verbosity_overriden = False
        self._color_mode = "auto"
        self._log_style = "record"
        self.pretty = False
        self.interactive = False

        # store whatever colorful has detected for future use if
        # the color ouput is toggled (colorful detects # of supported colors,
        # so it has some non-trivial logic to determine this)
        self._autodetected_cf_colormode = cf.colorful.colormode
        self.set_format()

    def set_format(self, format_tmpl=None):
        if not format_tmpl:
            from ray.autoscaler._private.constants import LOGGER_FORMAT

            format_tmpl = LOGGER_FORMAT
        self._formatter = logging.Formatter(format_tmpl)

    def configure(self, log_style=None, color_mode=None, verbosity=None):
        """Configures the logger according to values."""
        if log_style is not None:
            self._set_log_style(log_style)

        if color_mode is not None:
            self._set_color_mode(color_mode)

        if verbosity is not None:
            self._set_verbosity(verbosity)

        self.detect_colors()

    @property
    def log_style(self):
        return self._log_style

    def _set_log_style(self, x):
        """Configures interactivity and formatting."""
        self._log_style = x.lower()
        self.interactive = _isatty()

        if self._log_style == "auto":
            self.pretty = _isatty()
        elif self._log_style == "record":
            self.pretty = False
            self._set_color_mode("false")
        elif self._log_style == "pretty":
            self.pretty = True

    @property
    def color_mode(self):
        return self._color_mode

    def _set_color_mode(self, x):
        self._color_mode = x.lower()
        self.detect_colors()

    @property
    def verbosity(self):
        if self._verbosity_overriden:
            return self._verbosity
        elif not self.pretty:
            return 999
        return self._verbosity

    def _set_verbosity(self, x):
        self._verbosity = x
        self._verbosity_overriden = True

    def detect_colors(self):
        """Update color output settings.

        Parse the `color_mode` string and optionally disable or force-enable
        color output
        (8-color ANSI if no terminal detected to be safe) in colorful.
        """
        if self.color_mode == "true":
            if self._autodetected_cf_colormode != cf.NO_COLORS:
                cf.colormode = self._autodetected_cf_colormode
            else:
                cf.colormode = cf.ANSI_8_COLORS
            return
        if self.color_mode == "false":
            cf.disable()
            return
        if self.color_mode == "auto":
            # colorful autodetects tty settings
            return

        raise ValueError("Invalid log color setting: " + self.color_mode)

    def newline(self):
        """Print a line feed."""
        self.print("")

    def _print(
        self,
        msg: str,
        _level_str: str = "INFO",
        _linefeed: bool = True,
        end: str = None,
    ):
        """Proxy for printing messages.

        Args:
            msg: Message to print.
            linefeed (bool):
                If `linefeed` is `False` no linefeed is printed at the
                end of the message.
        """
        if self.pretty:
            rendered_message = "  " * self.indent_level + msg
        else:
            if msg.strip() == "":
                return
            caller_info = _external_caller_info()
            record = logging.LogRecord(
                name="cli",
                # We override the level name later
                # TODO(maximsmol): give approximate level #s to our log levels
                level=0,
                # The user-facing logs do not need this information anyway
                # and it would be very tedious to extract since _print
                # can be at varying depths in the call stack
                # TODO(maximsmol): do it anyway to be extra
                pathname=caller_info["filename"],
                lineno=caller_info["lineno"],
                msg=msg,
                args={},
                # No exception
                exc_info=None,
            )
            record.levelname = _level_str
            rendered_message = self._formatter.format(record)

        # We aren't using standard python logging convention, so we hardcode
        # the log levels for now.
        if _level_str in ["WARNING", "ERROR", "PANIC"]:
            stream = sys.stderr
        else:
            stream = sys.stdout

        if not _linefeed:
            stream.write(rendered_message)
            stream.flush()
            return

        kwargs = {"end": end}
        print(rendered_message, file=stream, **kwargs)

    def indented(self):
        """Context manager that starts an indented block of output."""
        cli_logger = self

        class IndentedContextManager:
            def __enter__(self):
                cli_logger.indent_level += 1

            def __exit__(self, type, value, tb):
                cli_logger.indent_level -= 1

        return IndentedContextManager()

    def group(self, msg: str, *args: Any, **kwargs: Any):
        """Print a group title in a special color and start an indented block.

        For arguments, see `_format_msg`.
        """
        self.print(cf.dodgerBlue(msg), *args, **kwargs)

        return self.indented()

    def verbatim_error_ctx(self, msg: str, *args: Any, **kwargs: Any):
        """Context manager for printing multi-line error messages.

        Displays a start sequence "!!! {optional message}"
        and a matching end sequence "!!!".

        The string "!!!" can be used as a "tombstone" for searching.

        For arguments, see `_format_msg`.
        """
        cli_logger = self

        class VerbatimErorContextManager:
            def __enter__(self):
                cli_logger.error(cf.bold("!!! ") + "{}", msg, *args, **kwargs)

            def __exit__(self, type, value, tb):
                cli_logger.error(cf.bold("!!!"))

        return VerbatimErorContextManager()

    def labeled_value(self, key: str, msg: str, *args: Any, **kwargs: Any):
        """Displays a key-value pair with special formatting.

        Args:
            key: Label that is prepended to the message.

        For other arguments, see `_format_msg`.
        """
        self._print(cf.skyBlue(key) + ": " + _format_msg(cf.bold(msg), *args, **kwargs))

    def verbose(self, msg: str, *args: Any, **kwargs: Any):
        """Prints a message if verbosity is not 0.

        For arguments, see `_format_msg`.
        """
        if self.verbosity > 0:
            self.print(msg, *args, _level_str="VINFO", **kwargs)

    def verbose_warning(self, msg, *args, **kwargs):
        """Prints a formatted warning if verbosity is not 0.

        For arguments, see `_format_msg`.
        """
        if self.verbosity > 0:
            self._warning(msg, *args, _level_str="VWARN", **kwargs)

    def verbose_error(self, msg: str, *args: Any, **kwargs: Any):
        """Logs an error if verbosity is not 0.

        For arguments, see `_format_msg`.
        """
        if self.verbosity > 0:
            self._error(msg, *args, _level_str="VERR", **kwargs)

    def very_verbose(self, msg: str, *args: Any, **kwargs: Any):
        """Prints if verbosity is > 1.

        For arguments, see `_format_msg`.
        """
        if self.verbosity > 1:
            self.print(msg, *args, _level_str="VVINFO", **kwargs)

    def success(self, msg: str, *args: Any, **kwargs: Any):
        """Prints a formatted success message.

        For arguments, see `_format_msg`.
        """
        self.print(cf.limeGreen(msg), *args, _level_str="SUCC", **kwargs)

    def _warning(self, msg: str, *args: Any, _level_str: str = None, **kwargs: Any):
        """Prints a formatted warning message.

        For arguments, see `_format_msg`.
        """
        if _level_str is None:
            raise ValueError("Log level not set.")
        self.print(cf.orange(msg), *args, _level_str=_level_str, **kwargs)

    def warning(self, *args, **kwargs):
        self._warning(*args, _level_str="WARN", **kwargs)

    def _error(self, msg: str, *args: Any, _level_str: str = None, **kwargs: Any):
        """Prints a formatted error message.

        For arguments, see `_format_msg`.
        """
        if _level_str is None:
            raise ValueError("Log level not set.")
        self.print(cf.red(msg), *args, _level_str=_level_str, **kwargs)

    def error(self, *args, **kwargs):
        self._error(*args, _level_str="ERR", **kwargs)

    def panic(self, *args, **kwargs):
        self._error(*args, _level_str="PANIC", **kwargs)

    # Fine to expose _level_str here, since this is a general log function.
    def print(
        self,
        msg: str,
        *args: Any,
        _level_str: str = "INFO",
        end: str = None,
        **kwargs: Any,
    ):
        """Prints a message.

        For arguments, see `_format_msg`.
        """
        self._print(_format_msg(msg, *args, **kwargs), _level_str=_level_str, end=end)

    def info(self, msg: str, no_format=True, *args, **kwargs):
        self.print(msg, no_format=no_format, *args, **kwargs)

    def abort(
        self, msg: Optional[str] = None, *args: Any, exc: Any = None, **kwargs: Any
    ):
        """Prints an error and aborts execution.

        Print an error and throw an exception to terminate the program
        (the exception will not print a message).
        """
        if msg is not None:
            self._error(msg, *args, _level_str="PANIC", **kwargs)

        if exc is not None:
            raise exc

        exc_cls = click.ClickException
        if self.pretty:
            exc_cls = SilentClickException

        if msg is None:
            msg = "Exiting due to cli_logger.abort()"
        raise exc_cls(msg)

    def doassert(self, val: bool, msg: str, *args: Any, **kwargs: Any):
        """Handle assertion without throwing a scary exception.

        Args:
            val: Value to check.

        For other arguments, see `_format_msg`.
        """
        if not val:
            exc = None
            if not self.pretty:
                exc = AssertionError()

            # TODO(maximsmol): rework asserts so that we get the expression
            #                  that triggered the assert
            #                  to do this, install a global try-catch
            #                  for AssertionError and raise them normally
            self.abort(msg, *args, exc=exc, **kwargs)

    def render_list(self, xs: List[str], separator: str = cf.reset(", ")):
        """Render a list of bolded values using a non-bolded separator."""
        return separator.join([str(cf.bold(x)) for x in xs])

    def confirm(
        self,
        yes: bool,
        msg: str,
        *args: Any,
        _abort: bool = False,
        _default: bool = False,
        _timeout_s: Optional[float] = None,
        **kwargs: Any,
    ):
        """Display a confirmation dialog.

        Valid answers are "y/yes/true/1" and "n/no/false/0".

        Args:
            yes: If `yes` is `True` the dialog will default to "yes"
                        and continue without waiting for user input.
            _abort (bool):
                If `_abort` is `True`,
                "no" means aborting the program.
            _default (bool):
                The default action to take if the user just presses enter
                with no input.
            _timeout_s (float):
                If user has no input within _timeout_s seconds, the default
                action is taken. None means no timeout.
        """
        should_abort = _abort
        default = _default

        if not self.interactive and not yes:
            # no formatting around --yes here since this is non-interactive
            self.error(
                "This command requires user confirmation. "
                "When running non-interactively, supply --yes to skip."
            )
            raise ValueError("Non-interactive confirm without --yes.")

        if default:
            yn_str = "Y/n"
        else:
            yn_str = "y/N"

        confirm_str = cf.underlined("Confirm [" + yn_str + "]:") + " "

        rendered_message = _format_msg(msg, *args, **kwargs)
        # the rendered message ends with ascii coding
        if rendered_message and not msg.endswith("\n"):
            rendered_message += " "

        msg_len = len(rendered_message.split("\n")[-1])
        complete_str = rendered_message + confirm_str

        if yes:
            self._print(complete_str + "y " + cf.dimmed("[automatic, due to --yes]"))
            return True

        self._print(complete_str, _linefeed=False)

        res = None
        yes_answers = ["y", "yes", "true", "1"]
        no_answers = ["n", "no", "false", "0"]
        try:
            while True:
                if _timeout_s is None:
                    ans = sys.stdin.readline()
                elif sys.platform == "win32":
                    # Windows doesn't support select
                    start_time = time.time()
                    ans = ""
                    while True:
                        if (time.time() - start_time) >= _timeout_s:
                            self.newline()
                            ans = "\n"
                            break
                        elif msvcrt.kbhit():
                            ch = msvcrt.getwch()
                            if ch in ("\n", "\r"):
                                self.newline()
                                ans = ans + "\n"
                                break
                            elif ch == "\b":
                                if ans:
                                    ans = ans[:-1]
                                    # Emulate backspace erasing
                                    print("\b \b", end="", flush=True)
                            else:
                                ans = ans + ch
                                print(ch, end="", flush=True)
                        else:
                            time.sleep(0.1)
                else:
                    ready, _, _ = select.select([sys.stdin], [], [], _timeout_s)
                    if not ready:
                        self.newline()
                        ans = "\n"
                    else:
                        ans = sys.stdin.readline()

                ans = ans.lower()

                if ans == "\n":
                    res = default
                    break

                ans = ans.strip()
                if ans in yes_answers:
                    res = True
                    break
                if ans in no_answers:
                    res = False
                    break

                indent = " " * msg_len
                self.error(
                    "{}Invalid answer: {}. Expected {} or {}",
                    indent,
                    cf.bold(ans.strip()),
                    self.render_list(yes_answers, "/"),
                    self.render_list(no_answers, "/"),
                )
                self._print(indent + confirm_str, _linefeed=False)
        except KeyboardInterrupt:
            self.newline()
            res = default

        if not res and should_abort:
            # todo: make sure we tell the user if they
            # need to do cleanup
            self._print("Exiting...")
            raise SilentClickException(
                "Exiting due to the response to confirm(should_abort=True)."
            )

        return res

    def prompt(self, msg: str, *args, **kwargs):
        """Prompt the user for some text input.

        Args:
            msg: The mesage to display to the user before the prompt.

        Returns:
            The string entered by the user.
        """
        complete_str = cf.underlined(msg)
        rendered_message = _format_msg(complete_str, *args, **kwargs)
        # the rendered message ends with ascii coding
        if rendered_message and not msg.endswith("\n"):
            rendered_message += " "
        self._print(rendered_message, linefeed=False)

        res = ""
        try:
            ans = sys.stdin.readline()
            ans = ans.lower()
            res = ans.strip()
        except KeyboardInterrupt:
            self.newline()

        return res

    def flush(self):
        sys.stdout.flush()
        sys.stderr.flush()


class SilentClickException(click.ClickException):
    """`ClickException` that does not print a message.

    Some of our tooling relies on catching ClickException in particular.

    However the default prints a message, which is undesirable since we expect
    our code to log errors manually using `cli_logger.error()` to allow for
    colors and other formatting.
    """

    def __init__(self, message: str):
        super(SilentClickException, self).__init__(message)

    def show(self, file=None):
        pass


cli_logger = _CliLogger()

CLICK_LOGGING_OPTIONS = [
    click.option(
        "--log-style",
        required=False,
        type=click.Choice(cli_logger.VALID_LOG_STYLES, case_sensitive=False),
        default="auto",
        help=(
            "If 'pretty', outputs with formatting and color. If 'record', "
            "outputs record-style without formatting. "
            "'auto' defaults to 'pretty', and disables pretty logging "
            "if stdin is *not* a TTY."
        ),
    ),
    click.option(
        "--log-color",
        required=False,
        type=click.Choice(["auto", "false", "true"], case_sensitive=False),
        default="auto",
        help=("Use color logging. Auto enables color logging if stdout is a TTY."),
    ),
    click.option("-v", "--verbose", default=None, count=True),
]


def add_click_logging_options(f: Callable) -> Callable:
    for option in reversed(CLICK_LOGGING_OPTIONS):
        f = option(f)

    @wraps(f)
    def wrapper(*args, log_style=None, log_color=None, verbose=None, **kwargs):
        cli_logger.configure(log_style, log_color, verbose)
        return f(*args, **kwargs)

    return wrapper
