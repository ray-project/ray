from collections import defaultdict
from functools import lru_cache

from boto3.exceptions import ResourceNotExistsError
from botocore.config import Config
import boto3

from ray.autoscaler._private.cli_logger import cli_logger, cf
from ray.autoscaler._private.constants import BOTO_MAX_RETRIES


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
    error_code = None
    error_info = None
    # todo: not sure if these exceptions always have response
    if hasattr(exc, "response"):
        error_info = exc.response.get("Error", None)
    if error_info is not None:
        error_code = error_info.get("Code", None)

    generic_message_args = [
        "{}\n" "Error code: {}",
        msg.format(*args, **kwargs),
        cf.bold(error_code),
    ]

    # apparently
    # ExpiredTokenException
    # ExpiredToken
    # RequestExpired
    # are all the same pretty much
    credentials_expiration_codes = [
        "ExpiredTokenException",
        "ExpiredToken",
        "RequestExpired",
    ]

    if error_code in credentials_expiration_codes:
        # "An error occurred (ExpiredToken) when calling the
        # GetInstanceProfile operation: The security token
        # included in the request is expired"

        # "An error occurred (RequestExpired) when calling the
        # DescribeKeyPairs operation: Request has expired."

        token_command = (
            "aws sts get-session-token "
            "--serial-number arn:aws:iam::"
            + cf.underlined("ROOT_ACCOUNT_ID")
            + ":mfa/"
            + cf.underlined("AWS_USERNAME")
            + " --token-code "
            + cf.underlined("TWO_FACTOR_AUTH_CODE")
        )

        secret_key_var = (
            "export AWS_SECRET_ACCESS_KEY = "
            + cf.underlined("REPLACE_ME")
            + " # found at Credentials.SecretAccessKey"
        )
        session_token_var = (
            "export AWS_SESSION_TOKEN = "
            + cf.underlined("REPLACE_ME")
            + " # found at Credentials.SessionToken"
        )
        access_key_id_var = (
            "export AWS_ACCESS_KEY_ID = "
            + cf.underlined("REPLACE_ME")
            + " # found at Credentials.AccessKeyId"
        )

        # fixme: replace with a Github URL that points
        # to our repo
        aws_session_script_url = (
            "https://gist.github.com/maximsmol/" "a0284e1d97b25d417bd9ae02e5f450cf"
        )

        cli_logger.verbose_error(*generic_message_args)
        cli_logger.verbose(vars(exc))

        cli_logger.panic("Your AWS session has expired.")
        cli_logger.newline()
        cli_logger.panic("You can request a new one using")
        cli_logger.panic(cf.bold(token_command))
        cli_logger.panic("then expose it to Ray by setting")
        cli_logger.panic(cf.bold(secret_key_var))
        cli_logger.panic(cf.bold(session_token_var))
        cli_logger.panic(cf.bold(access_key_id_var))
        cli_logger.newline()
        cli_logger.panic("You can find a script that automates this at:")
        cli_logger.panic(cf.underlined(aws_session_script_url))
        # Do not re-raise the exception here because it looks awful
        # and we already print all the info in verbose
        cli_logger.abort()

    # todo: any other errors that we should catch separately?

    cli_logger.panic(*generic_message_args)
    cli_logger.newline()
    with cli_logger.verbatim_error_ctx("Boto3 error:"):
        cli_logger.verbose("{}", str(vars(exc)))
        cli_logger.panic("{}", str(exc))
    cli_logger.abort()


def boto_exception_handler(msg, *args, **kwargs):
    # todo: implement timer
    class ExceptionHandlerContextManager:
        def __enter__(self):
            pass

        def __exit__(self, type, value, tb):
            import botocore

            if type is botocore.exceptions.ClientError:
                handle_boto_error(value, msg, *args, **kwargs)

    return ExceptionHandlerContextManager()


@lru_cache()
def resource_cache(name, region, max_retries=BOTO_MAX_RETRIES, **kwargs):
    cli_logger.verbose(
        "Creating AWS resource `{}` in `{}`", cf.bold(name), cf.bold(region)
    )
    kwargs.setdefault(
        "config",
        Config(retries={"max_attempts": max_retries}),
    )
    return boto3.resource(
        name,
        region,
        **kwargs,
    )


@lru_cache()
def client_cache(name, region, max_retries=BOTO_MAX_RETRIES, **kwargs):
    try:
        # try to re-use a client from the resource cache first
        return resource_cache(name, region, max_retries, **kwargs).meta.client
    except ResourceNotExistsError:
        # fall back for clients without an associated resource
        cli_logger.verbose(
            "Creating AWS client `{}` in `{}`", cf.bold(name), cf.bold(region)
        )
        kwargs.setdefault(
            "config",
            Config(retries={"max_attempts": max_retries}),
        )
        return boto3.client(
            name,
            region,
            **kwargs,
        )
