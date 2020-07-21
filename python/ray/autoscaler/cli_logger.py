import sys

import click

import colorama
from colorful.core import ColorfulString
import colorful as cf
colorama.init()


def _strip_codes(msg):
    return msg  # todo


# we could bold "{}" strings automatically but do we want that?
# todo:
def _format_msg(msg,
                *args,
                _tags=None,
                _numbered=None,
                _no_format=None,
                **kwargs):
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
        return numbering_str + msg.format(*args, **kwargs) + tags_str

    if kwargs:
        raise ValueError("We do not support printing kwargs yet.")

    res = [msg, *args]
    res = [str(x) for x in res]
    return ", ".join(res)


class _CliLogger():
    def __init__(self):
        self.strip = False
        self.old_style = True
        self.color_mode = "auto"
        self.indent_level = 0
        self.verbosity = 0

        self.dump_command_output = False

        self.info = {}

    def detect_colors(self):
        if self.color_mode == "true":
            self.strip = False
            return
        if self.color_mode == "false":
            self.strip = True
            return
        if self.color_mode == "auto":
            self.strip = sys.stdout.isatty()
            return

        raise ValueError("Invalid log color setting: " + self.color_mode)

    def newline(self):
        self._print("")

    def _print(self, msg, linefeed=True):
        if self.old_style:
            return

        if self.strip:
            msg = _strip_codes(msg)

        rendered_message = "  " * self.indent_level + msg

        if not linefeed:
            sys.stdout.write(rendered_message)
            sys.stdout.flush()
            return

        print(rendered_message)

    def indented(self, cls=False):
        cli_logger = self

        class IndentedContextManager():
            def __enter__(self):
                cli_logger.indent_level += 1

            def __exit__(self, type, value, tb):
                cli_logger.indent_level -= 1

        if cls:
            # fixme: this does not work :()
            return IndentedContextManager
        return IndentedContextManager()

    def timed(self, msg, *args, **kwargs):
        return self.group(msg, *args, **kwargs)

    def group(self, msg, *args, **kwargs):
        self._print(_format_msg(cf.cornflowerBlue(msg), *args, **kwargs))

        return self.indented()

    def verbatim_error_ctx(self, msg, *args, **kwargs):
        cli_logger = self

        class VerbatimErorContextManager():
            def __enter__(self):
                cli_logger.error(cf.bold("!!! ") + msg, *args, **kwargs)

            def __exit__(self, type, value, tb):
                cli_logger.error(cf.bold("!!!"))

        return VerbatimErorContextManager()

    def labeled_value(self, key, msg, *args, **kwargs):
        self._print(
            cf.cyan(key) + ": " + _format_msg(cf.bold(msg), *args, **kwargs))

    def verbose(self, msg, *args, **kwargs):
        if self.verbosity > 0:
            self.print(msg, *args, **kwargs)

    def verbose_error(self, msg, *args, **kwargs):
        if self.verbosity > 0:
            self.error(msg, *args, **kwargs)

    def very_verbose(self, msg, *args, **kwargs):
        if self.verbosity > 1:
            self.print(msg, *args, **kwargs)

    def success(self, msg, *args, **kwargs):
        self._print(_format_msg(cf.green(msg), *args, **kwargs))

    def warning(self, msg, *args, **kwargs):
        self._print(_format_msg(cf.yellow(msg), *args, **kwargs))

    def error(self, msg, *args, **kwargs):
        self._print(_format_msg(cf.red(msg), *args, **kwargs))

    def print(self, msg, *args, **kwargs):
        self._print(_format_msg(msg, *args, **kwargs))

    def abort(self, msg=None, *args, **kwargs):
        if msg is not None:
            self.error(msg, *args, **kwargs)

        raise SilentClickException("Exiting due to cli_logger.abort()")

    def doassert(self, val, msg, *args, **kwargs):
        if self.old_style:
            return

        if not val:
            self.abort(msg, *args, **kwargs)

    def old_debug(self, logger, msg, *args, **kwargs):
        if self.old_style:
            logger.debug(_format_msg(msg, *args, **kwargs))
            return

    def old_info(self, logger, msg, *args, **kwargs):
        if self.old_style:
            logger.info(_format_msg(msg, *args, **kwargs))
            return

    def old_warning(self, logger, msg, *args, **kwargs):
        if self.old_style:
            logger.warning(_format_msg(msg, *args, **kwargs))
            return

    def old_error(self, logger, msg, *args, **kwargs):
        if self.old_style:
            logger.error(_format_msg(msg, *args, **kwargs))
            return

    def old_exception(self, logger, msg, *args, **kwargs):
        if self.old_style:
            logger.exception(_format_msg(msg, *args, **kwargs))
            return

    def render_list(self, xs, separator=cf.reset(", ")):
        return separator.join([str(cf.bold(x)) for x in xs])

    def confirm(self, yes, msg, *args, _abort=False, _default=False, **kwargs):
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

    def old_confirm(self, msg, yes):
        if not self.old_style:
            return

        return None if yes else click.confirm(msg, abort=True)


class SilentClickException(click.ClickException):
    """
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
