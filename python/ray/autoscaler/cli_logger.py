import sys

import click

import colorama
from colorful.core import ColorfulString
import colorful as cf
colorama.init()

def _tostr(x):
    if not isinstance(x, str):
        return str(x)
    return x

def _strip_codes(msg):
    return msg # todo

# we could bold "{}" strings automatically but do we want that?
# todo:
def _format_msg(msg, *args, **kwargs):
    if isinstance(msg, str) or isinstance(msg, ColorfulString):
        tags_str = ""
        if "_tags" in kwargs:
            tags_list = []
            for k, v in kwargs["_tags"].items():
                if v is True:
                    tags_list += [k]
                    continue
                if v is False:
                    continue

                tags_list += [k+"="+v]
            if tags_list:
                tags_str = cf.reset(
                    cf.gray(" [{}]".format(", ".join(tags_list))))

            del kwargs["_tags"]

        numbering_str = ""
        if "_numbered" in kwargs:
            chars, i, n = kwargs["_numbered"]

            i = _tostr(i)
            n = _tostr(n)

            numbering_str = cf.gray(
                chars[0] + i + "/" + n + chars[1]) + " "

            del kwargs["_numbered"]

        no_format = False
        if "_no_format" in kwargs:
            no_format = kwargs["_no_format"]
            del kwargs["_no_format"]

        if no_format:
            # todo: throw if given args/kwargs?
            return numbering_str + msg + tags_str
        return numbering_str + msg.format(*args, **kwargs) + tags_str

    if kwargs:
        raise ValueError("We do not support printing kwargs yet.")

    l = [msg, *args]
    l = [_tostr(x) for x in l]
    return ', '.join(l)

class _CliLogger():
    def __init__(self):
        self.strip = False
        self.old_style = False
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

        # todo: actually detect ttys here
        self.strip = False

    def newline(self):
        self._print('')

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
        self.print(msg, *args, **kwargs)

        # todo: implement timer
        cli_logger = self
        class TimedContextManager():
            def __enter__(self):
                cli_logger.indent_level += 1

            def __exit__(self, type, value, tb):
                cli_logger.indent_level -= 1

        return TimedContextManager()

    def group(self, msg, *args, **kwargs):
        self._print(_format_msg(cf.cornflowerBlue(msg), *args, **kwargs))

        return self.indented()

    def verbatim_error_ctx(self, msg, *args, **kwargs):
        cli_logger = self
        class VerbatimErorContextManager():
            def __enter__(self):
                cli_logger.error(cf.bold("!!! ")+msg, *args, **kwargs)

            def __exit__(self, type, value, tb):
                cli_logger.error(cf.bold("!!!"))

        return VerbatimErorContextManager()

    def keyvalue(self, key, msg, *args, **kwargs):
        self._print(
            cf.cyan(key)+": "+_format_msg(cf.bold(msg), *args, **kwargs))

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

        sys.exit(1)

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

    def add_log_info(self, **kwargs):
        for k, v in kwargs.items():
            self.info[k] = v

    def arn_to_name(self, arn):
        return arn.split(":")[-1].split("/")[-1]

    def render_list(self, xs, separator=cf.reset(", ")):
        return separator.join([str(cf.bold(x)) for x in xs])

    def confirm(self, yes, msg, *args, **kwargs):
        if self.old_style:
            return

        should_abort = False
        if "_abort" in kwargs:
            should_abort = kwargs["_abort"]
            del kwargs["_abort"]

        default = False
        if "_default" in kwargs:
            default = kwargs["_default"]
            del kwargs["_default"]

        if default:
            yn_str = cf.green("Y") + "/" + cf.red("n")
        else:
            yn_str = cf.green("y") + "/" + cf.red("N")

        confirm_str = cf.underlined("Confirm [" + yn_str + "]:") + " "

        rendered_message = _format_msg(msg, *args, **kwargs)
        if rendered_message and rendered_message[-1] != "\n":
            rendered_message += " "

        l = len(rendered_message.split("\n")[-1])
        complete_str = rendered_message + confirm_str

        if yes:
            self._print(
                complete_str + "y " + cf.gray("[automatic, due to --yes]"))
            return True

        self._print(complete_str, linefeed=False)

        res = None
        yes_answers = ["y", "yes", "true", "1"]
        no_answers = ["n", "no", "false", "0"]
        try:
            while True:
                ans = sys.stdin.readline()
                ans = ans.strip()
                ans = ans.lower()

                if ans == "\n":
                    res = default
                    break
                elif ans in yes_answers:
                    res = True
                    break
                elif ans in no_answers:
                    res = False
                    break
                else:
                    indent = " " * l
                    self.error(
                        "{}Invalid answer: {}. "
                        "Expected {} or {}",
                        indent, cf.bold(ans.strip()),
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
            sys.exit(2)

        return res

    def old_confirm(self, msg, yes):
        if not self.old_style:
            return

        return None if yes else click.confirm(msg, abort=True)

    def boto_exception_handler(self, msg, *args, **kwargs):
        # todo: implement timer
        cli_logger = self
        class ExceptionHandlerContextManager():
            def __enter__(self):
                pass

            def __exit__(self, type, value, tb):
                import botocore

                if type is botocore.exceptions.ClientError:
                    cli_logger.handle_boto_error(value, msg, *args, **kwargs)

        return ExceptionHandlerContextManager()

    def handle_boto_error(self, exc, msg, *args, **kwargs):
        if self.old_style:
            # old-style logging doesn't do anything here
            return

        error_code = None
        error_info = None
        # todo: not sure if these exceptions always have response
        if hasattr(exc, "response"):
            error_info = exc.response.get("Error", None)
        if error_info is not None:
            error_code = error_info.get("Code", None)

        generic_message_args = [
            "{}\n"
            "Error code: {}",
            msg.format(*args, **kwargs),
            cf.bold(error_code)
        ]

        # apparently
        # ExpiredTokenException
        # ExpiredToken
        # RequestExpired
        # are all the same pretty much
        credentials_expiration_codes = [
            "ExpiredTokenException",
            "ExpiredToken",
            "RequestExpired"
        ]

        if error_code in credentials_expiration_codes:
            # "An error occurred (ExpiredToken) when calling the
            # GetInstanceProfile operation: The security token
            # included in the request is expired"

            # "An error occurred (RequestExpired) when calling the
            # DescribeKeyPairs operation: Request has expired."

            token_command = (
                "aws sts get-session-token "
                "--serial-number arn:aws:iam::"+
                cf.underlined("ROOT_ACCOUNT_ID")+":mfa/"+
                cf.underlined("AWS_USERNAME")+
                " --token-code "+
                cf.underlined("TWO_FACTOR_AUTH_CODE"))

            secret_key_var = (
                "export AWS_SECRET_ACCESS_KEY = "+
                cf.underlined("REPLACE_ME")+
                " # found at Credentials.SecretAccessKey")
            session_token_var = (
                "export AWS_SESSION_TOKEN = "+
                cf.underlined("REPLACE_ME")+
                " # found at Credentials.SessionToken")
            access_key_id_var = (
                "export AWS_ACCESS_KEY_ID = "+
                cf.underlined("REPLACE_ME")+
                " # found at Credentials.AccessKeyId")

            # fixme: replace with a Github URL that points
            # to our repo
            aws_session_script_url = (
                "https://gist.github.com/maximsmol/"
                "a0284e1d97b25d417bd9ae02e5f450cf")

            self.verbose_error(*generic_message_args)
            self.verbose(vars(exc))

            self.abort(
                "Your AWS session has expired.\n\n"
                "You can request a new one using\n{}\n"
                "then expose it to Ray by setting\n{}\n{}\n{}\n\n"
                "You can find a script that automates this at:\n{}",
                cf.bold(token_command),

                cf.bold(secret_key_var),
                cf.bold(session_token_var),
                cf.bold(access_key_id_var),

                cf.underlined(aws_session_script_url))

        # todo: any other errors that we should catch separately?

        self.error(*generic_message_args)
        self.newline()
        with self.verbatim_error_ctx("Boto3 error:"):
            self.verbose(vars(exc))
            self.error(exc)
        self.abort()

class CLILoggerError(Exception):
    def __init__(self, msg, type, data):
        self.type = type
        self.data = data

        super(CLILoggerError, self).__init__(msg)

cli_logger = _CliLogger()
