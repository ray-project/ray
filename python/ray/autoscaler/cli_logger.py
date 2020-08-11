"""Logger implementing the Command Line Interface.

A replacement for the standard Python `logging` API
designed for implementing a better CLI UX for the cluster launcher.

Supports color, bold text, italics, underlines, etc.
(depending on TTY features)
as well as indentation and other structured output.
"""

import sys
import logging
import inspect
import os

from typing import Any, Dict, Tuple, Optional, List

import click

import colorama
from colorful.core import ColorfulString
import colorful as cf
colorama.init()


def _patched_makeRecord(self,
                        name,
                        level,
                        fn,
                        lno,
                        msg,
                        args,
                        exc_info,
                        func=None,
                        extra=None,
                        sinfo=None):
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
    """
    rv = logging.LogRecord(name, level, fn, lno, msg, args, exc_info, func,
                           sinfo)
    if extra is not None:
        rv.__dict__.update(extra)
    return rv


logging.Logger.makeRecord = _patched_makeRecord


def _parent_frame_info():
    """Get the info from the caller frame.

    Used to override the logging function and line number with the correct
    ones. See the comment on _patched_makeRecord for more info.
    """

    frame = inspect.currentframe()
    # we are also in a function, so must go 2 levels up
    caller = frame.f_back.f_back
    return {
        "lineno": caller.f_lineno,
        "filename": os.path.basename(caller.f_code.co_filename),
    }


def _format_msg(msg: str,
                *args: Any,
                _tags: Dict[str, Any] = None,
                _numbered: Tuple[str, int, int] = None,
                _no_format: bool = None,
                **kwargs: Any):
    """Formats a message for printing.

    Renders `msg` using the built-in `str.format` and the passed-in
    `*args` and `**kwargs`.

    Args:
        *args (Any): `.format` arguments for `msg`.
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
        _no_format (bool):
            If `_no_format` is `True`,
            `.format` will not be called on the message.

            Useful if the output is user-provided or may otherwise
            contain an unexpected formatting string (e.g. "{}").

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
                tags_str = cf.reset(
                    cf.gray(" [{}]".format(", ".join(tags_list))))

        numbering_str = ""
        if _numbered is not None:
            chars, i, n = _numbered
            numbering_str = cf.gray(chars[0] + str(i) + "/" + str(n) +
                                    chars[1]) + " "

        if _no_format:
            # todo: throw if given args/kwargs?
            return numbering_str + msg + tags_str
        return numbering_str + cf.format(msg, *args, **kwargs) + tags_str

    if kwargs:
        raise ValueError("We do not support printing kwargs yet.")

    res = [msg, *args]
    res = [str(x) for x in res]
    return ", ".join(res)


class _CliLogger():
    """Singleton class for CLI logging.

    Attributes:
        strip (bool):
            If `strip` is `True`, all TTY control sequences will be
            removed from the output.
        old_style (bool):
            If `old_style` is `True`, the old logging calls are used instead
            of the new CLI UX. This is enabled by default and remains for
            backwards compatibility.
        color_mode (str):
            Can be "true", "false", or "auto".

            Determines the value of `strip` using a human-readable string
            that can be set from command line arguments.

            Also affects the `colorful` settings.

            If `color_mode` is "auto", `strip` is set to `not stdout.isatty()`
        indent_level (int):
            The current indentation level.

            All messages will be indented by prepending `"  " * indent_level`
        vebosity (int):
            Output verbosity.

            Low verbosity will disable `verbose` and `very_verbose` messages.
        dump_command_output (bool):
            Determines whether the old behavior of dumping command output
            to console will be used, or the new behavior of redirecting to
            a file.

            ! Currently unused.
    """
    strip: bool
    old_style: bool
    color_mode: str
    # color_mode: Union[Literal["auto"], Literal["false"], Literal["true"]]
    indent_level: int
    verbosity: int

    dump_command_output: bool
    _autodetected_cf_colormode: int

    def __init__(self):
        self.strip = False
        self.old_style = True
        self.color_mode = "auto"
        self.indent_level = 0
        self.verbosity = 0

        self.dump_command_output = False

        # store whatever colorful has detected for future use if
        # the color ouput is toggled (colorful detects # of supported colors,
        # so it has some non-trivial logic to determine this)
        self._autodetected_cf_colormode = cf.colormode

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
            self.strip = not sys.stdout.isatty()
            # colorful autodetects tty settings
            return

        raise ValueError("Invalid log color setting: " + self.color_mode)

    def newline(self):
        """Print a line feed.
        """
        self.print("")

    def _print(self, msg: str, linefeed: bool = True):
        """Proxy for printing messages.

        Args:
            msg (str): Message to print.
            linefeed (bool):
                If `linefeed` is `False` no linefeed is printed at the
                end of the message.
        """

        if self.old_style:
            return

        rendered_message = "  " * self.indent_level + msg

        if not linefeed:
            sys.stdout.write(rendered_message)
            sys.stdout.flush()
            return

        print(rendered_message)

    def indented(self):
        """Context manager that starts an indented block of output.
        """
        cli_logger = self

        class IndentedContextManager():
            def __enter__(self):
                cli_logger.indent_level += 1

            def __exit__(self, type, value, tb):
                cli_logger.indent_level -= 1

        return IndentedContextManager()

    def timed(self, msg: str, *args: Any, **kwargs: Any):
        """
        TODO: Unimplemented special type of output grouping that displays
              a timer for its execution. The method was not removed so we
              can mark places where this might be useful in case we ever
              implement this.

        For arguments, see `_format_msg`.
        """
        return self.group(msg, *args, **kwargs)

    def group(self, msg: str, *args: Any, **kwargs: Any):
        """Print a group title in a special color and start an indented block.

        For arguments, see `_format_msg`.
        """
        self.print(cf.cornflowerBlue(msg), *args, **kwargs)

        return self.indented()

    def verbatim_error_ctx(self, msg: str, *args: Any, **kwargs: Any):
        """Context manager for printing multi-line error messages.

        Displays a start sequence "!!! {optional message}"
        and a matching end sequence "!!!".

        The string "!!!" can be used as a "tombstone" for searching.

        For arguments, see `_format_msg`.
        """
        cli_logger = self

        class VerbatimErorContextManager():
            def __enter__(self):
                cli_logger.error(cf.bold("!!! ") + "{}", msg, *args, **kwargs)

            def __exit__(self, type, value, tb):
                cli_logger.error(cf.bold("!!!"))

        return VerbatimErorContextManager()

    def labeled_value(self, key: str, msg: str, *args: Any, **kwargs: Any):
        """Displays a key-value pair with special formatting.

        Args:
            key (str): Label that is prepended to the message.

        For other arguments, see `_format_msg`.
        """
        if self.old_style:
            return

        self._print(
            cf.cyan(key) + ": " + _format_msg(cf.bold(msg), *args, **kwargs))

    def verbose(self, msg: str, *args: Any, **kwargs: Any):
        """Prints a message if verbosity is not 0.

        For arguments, see `_format_msg`.
        """
        if self.verbosity > 0:
            self.print(msg, *args, **kwargs)

    def verbose_warning(self, msg, *args, **kwargs):
        """Prints a formatted warning if verbosity is not 0.

        For arguments, see `_format_msg`.
        """
        if self.verbosity > 0:
            self.warning(msg, *args, **kwargs)

    def verbose_error(self, msg: str, *args: Any, **kwargs: Any):
        """Logs an error if verbosity is not 0.

        For arguments, see `_format_msg`.
        """
        if self.verbosity > 0:
            self.error(msg, *args, **kwargs)

    def very_verbose(self, msg: str, *args: Any, **kwargs: Any):
        """Prints if verbosity is > 1.

        For arguments, see `_format_msg`.
        """
        if self.verbosity > 1:
            self.print(msg, *args, **kwargs)

    def success(self, msg: str, *args: Any, **kwargs: Any):
        """Prints a formatted success message.

        For arguments, see `_format_msg`.
        """
        self.print(cf.green(msg), *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any):
        """Prints a formatted warning message.

        For arguments, see `_format_msg`.
        """
        self.print(cf.yellow(msg), *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any):
        """Prints a formatted error message.

        For arguments, see `_format_msg`.
        """
        self.print(cf.red(msg), *args, **kwargs)

    def print(self, msg: str, *args: Any, **kwargs: Any):
        """Prints a message.

        For arguments, see `_format_msg`.
        """
        if self.old_style:
            return

        self._print(_format_msg(msg, *args, **kwargs))

    def abort(self, msg: Optional[str] = None, *args: Any, **kwargs: Any):
        """Prints an error and aborts execution.

        Print an error and throw an exception to terminate the program
        (the exception will not print a message).
        """
        if self.old_style:
            return

        if msg is not None:
            self.error(msg, *args, **kwargs)

        raise SilentClickException("Exiting due to cli_logger.abort()")

    def doassert(self, val: bool, msg: str, *args: Any, **kwargs: Any):
        """Handle assertion without throwing a scary exception.

        Args:
            val (bool): Value to check.

        For other arguments, see `_format_msg`.
        """
        if self.old_style:
            return

        if not val:
            self.abort(msg, *args, **kwargs)

    def old_debug(self, logger: logging.Logger, msg: str, *args: Any,
                  **kwargs: Any):
        """Old debug logging proxy.

        Pass along an old debug log iff new logging is disabled.
        Supports the new formatting features.

        Args:
            logger (logging.Logger):
                Logger to use if old logging behavior is selected.

        For other arguments, see `_format_msg`.
        """
        if self.old_style:
            logger.debug(
                _format_msg(msg, *args, **kwargs), extra=_parent_frame_info())
            return

    def old_info(self, logger: logging.Logger, msg: str, *args: Any,
                 **kwargs: Any):
        """Old info logging proxy.

        Pass along an old info log iff new logging is disabled.
        Supports the new formatting features.

        Args:
            logger (logging.Logger):
                Logger to use if old logging behavior is selected.

        For other arguments, see `_format_msg`.
        """
        if self.old_style:
            logger.info(
                _format_msg(msg, *args, **kwargs), extra=_parent_frame_info())
            return

    def old_warning(self, logger: logging.Logger, msg: str, *args: Any,
                    **kwargs: Any):
        """Old warning logging proxy.

        Pass along an old warning log iff new logging is disabled.
        Supports the new formatting features.

        Args:
            logger (logging.Logger):
                Logger to use if old logging behavior is selected.

        For other arguments, see `_format_msg`.
        """
        if self.old_style:
            logger.warning(
                _format_msg(msg, *args, **kwargs), extra=_parent_frame_info())
            return

    def old_error(self, logger: logging.Logger, msg: str, *args: Any,
                  **kwargs: Any):
        """Old error logging proxy.

        Pass along an old error log iff new logging is disabled.
        Supports the new formatting features.

        Args:
            logger (logging.Logger):
                Logger to use if old logging behavior is selected.

        For other arguments, see `_format_msg`.
        """
        if self.old_style:
            logger.error(
                _format_msg(msg, *args, **kwargs), extra=_parent_frame_info())
            return

    def old_exception(self, logger: logging.Logger, msg: str, *args: Any,
                      **kwargs: Any):
        """Old exception logging proxy.

        Pass along an old exception log iff new logging is disabled.
        Supports the new formatting features.

        Args:
            logger (logging.Logger):
                Logger to use if old logging behavior is selected.

        For other arguments, see `_format_msg`.
        """
        if self.old_style:
            logger.exception(
                _format_msg(msg, *args, **kwargs), extra=_parent_frame_info())
            return

    def render_list(self, xs: List[str], separator: str = cf.reset(", ")):
        """Render a list of bolded values using a non-bolded separator.
        """
        return separator.join([str(cf.bold(x)) for x in xs])

    def confirm(self,
                yes: bool,
                msg: str,
                *args: Any,
                _abort: bool = False,
                _default: bool = False,
                **kwargs: Any):
        """Display a confirmation dialog.

        Valid answers are "y/yes/true/1" and "n/no/false/0".

        Args:
            yes (bool): If `yes` is `True` the dialog will default to "yes"
                        and continue without waiting for user input.
            _abort (bool):
                If `_abort` is `True`,
                "no" means aborting the program.
            _default (bool):
                The default action to take if the user just presses enter
                with no input.
        """
        if self.old_style:
            return

        should_abort = _abort
        default = _default

        if default:
            yn_str = cf.green("Y") + "/" + cf.red("n")
        else:
            yn_str = cf.green("y") + "/" + cf.red("N")

        confirm_str = cf.underlined("Confirm [" + yn_str + "]:") + " "

        rendered_message = _format_msg(msg, *args, **kwargs)
        if rendered_message and rendered_message[-1] != "\n":
            rendered_message += " "

        msg_len = len(rendered_message.split("\n")[-1])
        complete_str = rendered_message + confirm_str

        if yes:
            self._print(complete_str + "y " +
                        cf.gray("[automatic, due to --yes]"))
            return True

        self._print(complete_str, linefeed=False)

        res = None
        yes_answers = ["y", "yes", "true", "1"]
        no_answers = ["n", "no", "false", "0"]
        try:
            while True:
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
                self.error("{}Invalid answer: {}. "
                           "Expected {} or {}", indent, cf.bold(ans.strip()),
                           self.render_list(yes_answers, "/"),
                           self.render_list(no_answers, "/"))
                self._print(indent + confirm_str, linefeed=False)
        except KeyboardInterrupt:
            self.newline()
            res = default

        if not res and should_abort:
            # todo: make sure we tell the user if they
            # need to do cleanup
            self._print("Exiting...")
            raise SilentClickException(
                "Exiting due to the response to confirm(should_abort=True).")

        return res

    def old_confirm(self, msg: str, yes: bool):
        """Old confirm dialog proxy.

        Let `click` display a confirm dialog iff new logging is disabled.
        """
        if not self.old_style:
            return

        return None if yes else click.confirm(msg, abort=True)


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
