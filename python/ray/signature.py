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


def get_signature_params(func):
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

    return list(funcsigs.signature(func).parameters.items())


def check_signature_supported(func, warn=False):
    """Check if we support the signature of this function.

    We currently do not allow remote functions to have **kwargs. We also do not
    support keyword arguments in conjunction with a *args argument.

    Args:
        func: The function whose signature should be checked.
        warn: If this is true, a warning will be printed if the signature is
            not supported. If it is false, an exception will be raised if the
            signature is not supported.

    Raises:
        Exception: An exception is raised if the signature is not supported.
    """
    function_name = func.__name__
    sig_params = get_signature_params(func)

    has_kwargs_param = False
    has_kwonly_param = False
    for keyword_name, parameter in sig_params:
        if parameter.kind == Parameter.VAR_KEYWORD:
            has_kwargs_param = True
        if parameter.kind == Parameter.KEYWORD_ONLY:
            has_kwonly_param = True

    if has_kwargs_param:
        message = ("The function {} has a **kwargs argument, which is "
                   "currently not supported.".format(function_name))
        if warn:
            logger.warning(message)
        else:
            raise Exception(message)

    if has_kwonly_param:
        message = ("The function {} has a keyword only argument "
                   "(defined after * or *args), which is currently "
                   "not supported.".format(function_name))
        if warn:
            logger.warning(message)
        else:
            raise Exception(message)


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
    sig_params = get_signature_params(func)

    if ignore_first:
        if len(sig_params) == 0:
            raise Exception("Methods must take a 'self' argument, but the "
                            "method '{}' does not have one.".format(
                                func.__name__))
        sig_params = sig_params[1:]

    # Construct the argument default values and other argument information.
    arg_names = []
    arg_defaults = []
    arg_is_positionals = []
    keyword_names = set()
    for arg_name, parameter in sig_params:
        arg_names.append(arg_name)
        arg_defaults.append(parameter.default)
        arg_is_positionals.append(parameter.kind == parameter.VAR_POSITIONAL)
        if parameter.kind == Parameter.POSITIONAL_OR_KEYWORD:
            # Note KEYWORD_ONLY arguments currently unsupported.
            keyword_names.add(arg_name)

    return FunctionSignature(arg_names, arg_defaults, arg_is_positionals,
                             keyword_names, func.__name__)
