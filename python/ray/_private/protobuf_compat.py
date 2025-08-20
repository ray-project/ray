import inspect

from google.protobuf.json_format import MessageToDict, MessageToJson

"""
This module provides a compatibility layer for different versions of the protobuf
library.
"""

_protobuf_has_old_arg_name_cached = None


def _protobuf_has_old_arg_name():
    """Cache the inspect result to avoid doing it for every single message."""
    global _protobuf_has_old_arg_name_cached
    if _protobuf_has_old_arg_name_cached is None:
        params = inspect.signature(MessageToDict).parameters
        _protobuf_has_old_arg_name_cached = "including_default_value_fields" in params
    return _protobuf_has_old_arg_name_cached


def rename_always_print_fields_with_no_presence(kwargs):
    """
    Protobuf version 5.26.0rc2 renamed argument for `MessageToDict` and `MessageToJson`:
    `including_default_value_fields` -> `always_print_fields_with_no_presence`.
    See https://github.com/protocolbuffers/protobuf/commit/06e7caba58ede0220b110b89d08f329e5f8a7537#diff-8de817c14d6a087981503c9aea38730b1b3e98f4e306db5ff9d525c7c304f234L129  # noqa: E501

    We choose to always use the new argument name. If user used the old arg, we raise an
    error.

    If protobuf does not have the new arg name but have the old arg name, we rename our
    arg to the old one.
    """
    old_arg_name = "including_default_value_fields"
    new_arg_name = "always_print_fields_with_no_presence"
    if old_arg_name in kwargs:
        raise ValueError(f"{old_arg_name} is deprecated, please use {new_arg_name}")

    if new_arg_name in kwargs and _protobuf_has_old_arg_name():
        kwargs[old_arg_name] = kwargs.pop(new_arg_name)

    return kwargs


def message_to_dict(*args, **kwargs):
    kwargs = rename_always_print_fields_with_no_presence(kwargs)
    return MessageToDict(*args, **kwargs)


def message_to_json(*args, **kwargs):
    kwargs = rename_always_print_fields_with_no_presence(kwargs)
    return MessageToJson(*args, **kwargs)
