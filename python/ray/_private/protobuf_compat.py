from google.protobuf.json_format import MessageToDict
from packaging import version
import google.protobuf

"""
This module provides a compatibility layer for different versions of the protobuf library.
"""

pb_version = version.parse(google.protobuf.__version__)


def rename_always_print_fields_with_no_presence(kwargs):
    """
    Version 5.26.0rc2 renamed argument for `MessageToDict`:
    `including_default_value_fields` -> `always_print_fields_with_no_presence`.
    See https://github.com/protocolbuffers/protobuf/commit/06e7caba58ede0220b110b89d08f329e5f8a7537#diff-8de817c14d6a087981503c9aea38730b1b3e98f4e306db5ff9d525c7c304f234L75

    We choose to always use the new argument name, and if protobuf is old we map it back to the old name.
    """
    if pb_version < version.parse("5.26.0rc2"):
        if "always_print_fields_with_no_presence" in kwargs:
            kwargs["including_default_value_fields"] = kwargs.pop(
                "always_print_fields_with_no_presence"
            )
    return kwargs


def message_to_dict(*args, **kwargs):
    kwargs = rename_always_print_fields_with_no_presence(kwargs)
    return MessageToDict(*args, **kwargs)
