from collections import defaultdict

from ray.autoscaler.cli_logger import cli_logger
import colorful as cf


class LazyDefaultDict(defaultdict):
    """
    LazyDefaultDict(default_factory[, ...]) --> dict with default factory

    The default factory is call with the key argument to produce
    a new value when a key is not present, in __getitem__ only.
    A LazyDefaultDict compares equal to a dict with the same items.
    All remaining arguments are treated the same as if they were
    passed to the dict constructor, including keyword arguments.
    """

    def __missing__(self, key):
        """
        __missing__(key) # Called by __getitem__ for missing key; pseudo-code:
          if self.default_factory is None: raise KeyError((key,))
          self[key] = value = self.default_factory(key)
          return value
        """
        self[key] = self.default_factory(key)
        return self[key]


def handle_boto_error(exc, msg, *args, **kwargs):
    if cli_logger.old_style:
        # old-style logging doesn't do anything here
        # so we exit early
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
        "ExpiredTokenException", "ExpiredToken", "RequestExpired"
    ]

    if error_code in credentials_expiration_codes:
        # "An error occurred (ExpiredToken) when calling the
        # GetInstanceProfile operation: The security token
        # included in the request is expired"

        # "An error occurred (RequestExpired) when calling the
        # DescribeKeyPairs operation: Request has expired."

        token_command = (
            "aws sts get-session-token "
            "--serial-number arn:aws:iam::" + cf.underlined("ROOT_ACCOUNT_ID")
            + ":mfa/" + cf.underlined("AWS_USERNAME") + " --token-code " +
            cf.underlined("TWO_FACTOR_AUTH_CODE"))

        secret_key_var = (
            "export AWS_SECRET_ACCESS_KEY = " + cf.underlined("REPLACE_ME") +
            " # found at Credentials.SecretAccessKey")
        session_token_var = (
            "export AWS_SESSION_TOKEN = " + cf.underlined("REPLACE_ME") +
            " # found at Credentials.SessionToken")
        access_key_id_var = (
            "export AWS_ACCESS_KEY_ID = " + cf.underlined("REPLACE_ME") +
            " # found at Credentials.AccessKeyId")

        # fixme: replace with a Github URL that points
        # to our repo
        aws_session_script_url = ("https://gist.github.com/maximsmol/"
                                  "a0284e1d97b25d417bd9ae02e5f450cf")

        cli_logger.verbose_error(*generic_message_args)
        cli_logger.verbose(vars(exc))

        cli_logger.abort(
            "Your AWS session has expired.\n\n"
            "You can request a new one using\n{}\n"
            "then expose it to Ray by setting\n{}\n{}\n{}\n\n"
            "You can find a script that automates this at:\n{}",
            cf.bold(token_command), cf.bold(secret_key_var),
            cf.bold(session_token_var), cf.bold(access_key_id_var),
            cf.underlined(aws_session_script_url))

    # todo: any other errors that we should catch separately?

    cli_logger.error(*generic_message_args)
    cli_logger.newline()
    with cli_logger.verbatim_error_ctx("Boto3 error:"):
        cli_logger.verbose(vars(exc))
        cli_logger.error(exc)
    cli_logger.abort()


def boto_exception_handler(msg, *args, **kwargs):
    # todo: implement timer
    class ExceptionHandlerContextManager():
        def __enter__(self):
            pass

        def __exit__(self, type, value, tb):
            import botocore

            if type is botocore.exceptions.ClientError:
                handle_boto_error(value, msg, *args, **kwargs)

    return ExceptionHandlerContextManager()
