from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import namedtuple
import funcsigs
from funcsigs import Parameter
import logging

from ray.utils import is_cython

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)

DUMMY_TYPE = "__RAY_ARG_DUMMY__"

FunctionSignature = namedtuple("FunctionSignature", [
    "arg_names", "arg_defaults", "arg_is_positionals", "keyword_names",
    "function_name"
])
"""This class is used to represent a function signature.

Attributes:
    arg_names: A list containing the name of all arguments.
    arg_defaults: A dictionary mapping from argument name to argument default
        value. If the argument is not a keyword argument, the default value
        will be funcsigs._empty.
    arg_is_positionals: A dictionary mapping from argument name to a bool. The
        bool will be true if the argument is a *args argument. Otherwise it
        will be false.
    keyword_names: A set containing the names of the keyword arguments.
        Note most arguments in Python can be called as positional or keyword
        arguments, so this overlaps (sometimes completely) with arg_names.
    function_name: The name of the function whose signature is being
        inspected. This is used for printing better error messages.
"""


def _get_signature(func):
    """Get signature parameters

    Support Cython functions by grabbing relevant attributes from the Cython
    function and attaching to a no-op function. This is somewhat brittle, since
    funcsigs may change, but given that funcsigs is written to a PEP, we hope
    it is relatively stable. Future versions of Python may allow overloading
    the inspect 'isfunction' and 'ismethod' functions / create ABC for Python
    functions. Until then, it appears that Cython won't do anything about
    compatability with the inspect module.

    Args:
        func: The function whose signature should be checked.

    Raises:
        TypeError: A type error if the signature is not supported
    """
    # The first condition for Cython functions, the latter for Cython instance
    # methods
    if is_cython(func):
        attrs = [
            "__code__", "__annotations__", "__defaults__", "__kwdefaults__"
        ]

        if all(hasattr(func, attr) for attr in attrs):
            original_func = func

            def func():
                return

            for attr in attrs:
                setattr(func, attr, getattr(original_func, attr))
        else:
            raise TypeError("{!r} is not a Python function we can process"
                            .format(func))

    # return list(funcsigs.signature(func).parameters.items())
    return funcsigs.signature(func)


# def check_signature_supported(func, warn=False):
#     """Check if we support the signature of this function.

#     We currently do not allow remote functions to have **kwargs. We also do not
#     support keyword arguments in conjunction with a *args argument.

#     Args:
#         func: The function whose signature should be checked.
#         warn: If this is true, a warning will be printed if the signature is
#             not supported. If it is false, an exception will be raised if the
#             signature is not supported.

#     Raises:
#         Exception: An exception is raised if the signature is not supported.
#     """
#     function_name = func.__name__
#     sig_params = get_signature_params(func)

#     has_kwargs_param = False
#     has_kwonly_param = False
#     for keyword_name, parameter in sig_params:
#         if parameter.kind == Parameter.VAR_KEYWORD:
#             has_kwargs_param = True
#         if parameter.kind == Parameter.KEYWORD_ONLY:
#             has_kwonly_param = True

#     if has_kwargs_param:
#         message = ("The function {} has a **kwargs argument, which is "
#                    "currently not supported.".format(function_name))
#         if warn:
#             logger.debug(message)
#         else:
#             raise Exception(message)

#     if has_kwonly_param:
#         message = ("The function {} has a keyword only argument "
#                    "(defined after * or *args), which is currently "
#                    "not supported.".format(function_name))
#         if warn:
#             logger.debug(message)
#         else:
#             raise Exception(message)


def extract_signature(func, ignore_first=False):
    """Extract the function signature from the function.

    Args:
        func: The function whose signature should be extracted.
        ignore_first: True if the first argument should be ignored. This should
            be used when func is a method of a class.

    Returns:
        A function signature object, which includes the names of the keyword
            arguments as well as their default values.
    """
    function_signature = _get_signature(func)

    if ignore_first:
        if not function_signature.parameters.items():
            raise TypeError("Methods must take a 'self' argument, but the "
                            "method '{}' does not have one.".format(
                                func.__name__))
    #     sig_params = sig_params[1:]

    # # Construct the argument default values and other argument information.
    # arg_names = []
    # arg_defaults = []
    # arg_is_positionals = []
    # keyword_names = set()
    # for arg_name, parameter in sig_params:
    #     arg_names.append(arg_name)
    #     arg_defaults.append(parameter.default)
    #     arg_is_positionals.append(parameter.kind == parameter.VAR_POSITIONAL)
    #     if parameter.kind == Parameter.POSITIONAL_OR_KEYWORD:
    #         # Note KEYWORD_ONLY arguments currently unsupported.
    #         keyword_names.add(arg_name)

    # return FunctionSignature(arg_names, arg_defaults, arg_is_positionals,
    #                          keyword_names, func.__name__)
    return function_signature


def flatten_args(args, kwargs):
    """Generates a serializable format for the arguments.

    Args:
        args: The non-keyword arguments passed into the function.
        kwargs: The keyword arguments passed into the function.

    Returns:
        List of args and kwargs. Non-keyword arguments are prefixed
            by internal enum DUMMY_TYPE.

    Example:
        >>> flatten_args([1, 2, 3], {"a": 4})
        [None, 1, None, 2, None, 3, "a", 4]
    """
    list_args = []
    for arg in args:
        list_args += [DUMMY_TYPE, arg]

    for keyword, arg in kwargs.items():
        list_args += [keyword, arg]
    return list_args


def recover_args(flattened_args):
    """Recreates `args` and `kwargs` from the flattened arg list.

    Args:
        flattened_args: List of args and kwargs. This should be the output of
            flatten_args.

    Returns:
        args: The non-keyword arguments passed into the function.
        kwargs: The keyword arguments passed into the function.
    """
    assert len(flattened_args) % 2 == 0, "Unexpected argument formatting."
    args = []
    kwargs = {}
    for name_index in range(0, len(flattened_args), 2):
        name, arg = flattened_args[name_index], flattened_args[name_index + 1]
        if name == DUMMY_TYPE:
            args.append(arg)
        else:
            kwargs[name] = arg

    return args, kwargs


def validate_args(funcsigs_signature, args, kwargs, actor_call=False):
    """Validates that this function can be called with given arguments.

    Wrapper around funcsigs.Signature.bind.

    Raises:
        TypeError if provided argument is incorrect.

    """
    try:
        if actor_call:
            args = [None] + list(args)  # use dummy for 'self'
        funcsigs_signature.bind(*args, **kwargs)
    except TypeError as exc:
        print(args, kwargs)
        raise TypeError(str(exc))


# def extend_args(function_signature, args, kwargs):
#     """Extend the arguments that were passed into a function.

#     This extends the arguments that were passed into a function with the
#     default arguments provided in the function definition.

#     Args:
#         function_signature: The function signature of the function being
#             called.
#         args: The non-keyword arguments passed into the function.
#         kwargs: The keyword arguments passed into the function.

#     Returns:
#         An extended list of arguments to pass into the function.

#     Raises:
#         Exception: An exception may be raised if the function cannot be called
#             with these arguments.
#     """
#     # raise DeprecationWarning
#     arg_names = function_signature.arg_names
#     arg_defaults = function_signature.arg_defaults
#     arg_is_positionals = function_signature.arg_is_positionals
#     keyword_names = function_signature.keyword_names
#     function_name = function_signature.function_name

#     args = list(args)

#     for keyword_name in kwargs:
#         if keyword_name not in keyword_names:
#             pass
#             # raise Exception("The name '{}' is not a valid keyword argument "
#             #                 "for the function '{}'.".format(
#             #                     keyword_name, function_name))

#     # Fill in the remaining arguments.
#     for skipped_name in arg_names[0:len(args)]:
#         if skipped_name in kwargs:
#             pass
#             # raise Exception("Positional and keyword value provided for the "
#             #                 "argument '{}' for the function '{}'".format(
#             #                     keyword_name, function_name))

#     zipped_info = zip(arg_names, arg_defaults, arg_is_positionals)
#     zipped_info = list(zipped_info)[len(args):]
#     for keyword_name, default_value, is_positional in zipped_info:
#         if keyword_name in kwargs:
#             args.append(kwargs[keyword_name])
#         else:
#             if default_value != funcsigs._empty:
#                 args.append(default_value)
#             else:
#                 # This means that there is a missing argument. Unless this is
#                 # the last argument and it is a *args argument in which case it
#                 # can be omitted.
#                 if not is_positional:
#                     pass
#                     # raise Exception("No value was provided for the argument "
#                     #                 "'{}' for the function '{}'.".format(
#                     #                     keyword_name, function_name))

#     no_positionals = len(arg_is_positionals) == 0 or not arg_is_positionals[-1]
#     too_many_arguments = len(args) > len(arg_names) and no_positionals
#     if too_many_arguments:
#         pass
#         # raise Exception("Too many arguments were passed to the function '{}'"
#         #                 .format(function_name))
#     return args
