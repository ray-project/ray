"""Logger implementing the Command Line Interface.

A replacement for the standard Python `logging` API
designed for implementing a better CLI UX for the cluster launcher.

Supports color, bold text, italics, underlines, etc.
(depending on TTY features)
as well as indentation and other structured output.
"""

import time
import sys
import logging
import inspect
import os

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


def _format_msg(msg,
                *args,
                _tags=None,
                _numbered=None,
                _no_format=None,
                **kwargs):
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

            i = str(i)
            n = str(n)

            numbering_str = cf.gray(chars[0] + i + "/" + n + chars[1]) + " "

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

    def __init__(self):
        self._non_interactive = False
        self.color_mode = "auto"
        self.indent_level = 0

        self._verbosity = 0

        self.dump_command_output = False

        # store whatever colorful has detected for future use if
        # the color ouput is toggled (colorful detects # of supported colors,
        # so it has some non-trivial logic to determine this)
        self._autodetected_cf_colormode = cf.colormode

        self.non_interactive = True

    @property
    def non_interactive(self):
        return self._non_interactive

    @non_interactive.setter
    def non_interactive(self, x):
        if x:
            self.color_mode = "false"
            self.detect_colors()
        self._non_interactive = x

    @property
    def verbosity(self):
        if self.non_interactive:
            return 999
        return self._verbosity

    @verbosity.setter
    def verbosity(self, x):
        self._verbosity = x

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

    def _print(self, msg, _level_str="INFO", _linefeed=True):
        """Proxy for printing messages.

        Args:
            msg (str): Message to print.
            linefeed (bool):
                If `linefeed` is `False` no linefeed is printed at the
                end of the message.
        """
        if self.non_interactive:
            if msg.strip() == "":
                return

            timestamp = time.strftime("[%x %X] ")
            # TODO(maximsmol): figure out the required level string
            #                  width automatically
            rendered_message = "{}{:6} {}".format(timestamp, _level_str, msg)
        else:
            rendered_message = "  " * self.indent_level + msg

        if not _linefeed:
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

    def timed(self, msg, *args, **kwargs):
        """
        TODO: Unimplemented special type of output grouping that displays
              a timer for its execution. The method was not removed so we
              can mark places where this might be useful in case we ever
              implement this.

        For arguments, see `_format_msg`.
        """
        return self.group(msg, *args, **kwargs)

    def group(self, msg, *args, **kwargs):
        """Print a group title in a special color and start an indented block.

        For arguments, see `_format_msg`.
        """
        self.print(cf.cornflowerBlue(msg), *args, **kwargs)

        return self.indented()

    def verbatim_error_ctx(self, msg, *args, **kwargs):
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

    def labeled_value(self, key, msg, *args, **kwargs):
        """Displays a key-value pair with special formatting.

        Args:
            key (str): Label that is prepended to the message.

        For other arguments, see `_format_msg`.
        """
        self._print(
            cf.cyan(key) + ": " + _format_msg(cf.bold(msg), *args, **kwargs))

    def verbose(self, msg, *args, **kwargs):
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

    def verbose_error(self, msg, *args, **kwargs):
        """Logs an error if verbosity is not 0.

        For arguments, see `_format_msg`.
        """
        if self.verbosity > 0:
            self._error(msg, *args, _level_str="VERR", **kwargs)

    def very_verbose(self, msg, *args, **kwargs):
        """Prints if verbosity is > 1.

        For arguments, see `_format_msg`.
        """
        if self.verbosity > 1:
            self.print(msg, *args, _level_str="VVINFO", **kwargs)

    def success(self, msg, *args, **kwargs):
        """Prints a formatted success message.

        For arguments, see `_format_msg`.
        """
        self.print(cf.green(msg), *args, _level_str="SUCC", **kwargs)

    def _warning(self, msg, *args, _level_str=None, **kwargs):
        """Prints a formatted warning message.

        For arguments, see `_format_msg`.
        """
        if _level_str is None:
            raise ValueError("Log level not set.")
        self.print(cf.yellow(msg), *args, _level_str=_level_str, **kwargs)

    def warning(self, *args, **kwargs):
        self._warning(*args, _level_str="WARN", **kwargs)

    def _error(self, msg, *args, _level_str=None, **kwargs):
        """Prints a formatted error message.

        For arguments, see `_format_msg`.
        """
        if _level_str is None:
            raise ValueError("Log level not set.")
        self.print(cf.red(msg), *args, _level_str=_level_str, **kwargs)

    def error(self, *args, **kwargs):
        self._error(*args, _level_str="ERR", **kwargs)

    # Fine to expose _level_str here, since this is a general log function.
    def print(self, msg, *args, _level_str="INFO", _linefeed=True, **kwargs):
        """Prints a message.

        For arguments, see `_format_msg`.
        """

        self._print(
            _format_msg(msg, *args, **kwargs),
            _level_str=_level_str,
            _linefeed=_linefeed)

    def abort(self, msg=None, exc=None, *args, **kwargs):
        """Prints an error and aborts execution.

        Print an error and throw an exception to terminate the program
        (the exception will not print a message).
        """

        if msg is not None:
            self._error(msg, *args, _level_str="PANIC", **kwargs)

        if exc is not None:
            raise exc

        exc_cls = SilentClickException
        if self.non_interactive:
            exc_cls = click.ClickException
        raise exc_cls("Exiting due to cli_logger.abort()")

    def doassert(self, val, msg, *args, **kwargs):
        """Handle assertion without throwing a scary exception.

        Args:
            val (bool): Value to check.

        For other arguments, see `_format_msg`.
        """
        if not val:
            exc = None
            if self.non_interactive:
                exc = AssertionError()

            # TODO(maximsmol): rework asserts so that we get the expression
            #                  that triggered the assert
            #                  to do this, install a global try-catch
            #                  for AssertionError and raise them normally
            self.abort(msg, *args, exc=exc, **kwargs)

    def old_debug(self, logger, msg, *args, **kwargs):
        return

    def old_info(self, logger, msg, *args, **kwargs):
        return

    def old_warning(self, logger, msg, *args, **kwargs):
        return

    def old_error(self, logger, msg, *args, **kwargs):
        return

    def old_exception(self, logger, msg, *args, **kwargs):
        return

    def render_list(self, xs, separator=cf.reset(", ")):
        """Render a list of bolded values using a non-bolded separator.
        """
        return separator.join([str(cf.bold(x)) for x in xs])

    def confirm(self, yes, msg, *args, _abort=False, _default=False, **kwargs):
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
        should_abort = _abort
        default = _default

        if self.non_interactive and not yes:
            # no formatting around --yes here since this is non-interactive
            self.error("This command requires user confirmation. "
                       "When running non-interactively, supply --yes to skip.")
            raise ValueError("Non-interactive confirm without --yes.")

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

        self._print(complete_str, _linefeed=False)

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
                self._print(indent + confirm_str, _linefeed=False)
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

    def old_confirm(self, msg, yes):
        return


class SilentClickException(click.ClickException):
    """`ClickException` that does not print a message.

    Some of our tooling relies on catching ClickException in particular.

    However the default prints a message, which is undesirable since we expect
    our code to log errors manually using `cli_logger.error()` to allow for
    colors and other formatting.
    """

    def __init__(self, message):
        super(SilentClickException, self).__init__(message)

    def show(self, file=None):
        pass


cli_logger = _CliLogger()
