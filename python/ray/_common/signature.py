import inspect
import logging
from inspect import Parameter
from typing import Any, Dict, List, Tuple

from ray._private.inspect_util import is_cython

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)

# This dummy type is also defined in ArgumentsBuilder.java. Please keep it
# synced.
DUMMY_TYPE = b"__RAY_DUMMY__"


def get_signature(func: Any) -> inspect.Signature:
    """Get signature parameters.

    Support Cython functions by grabbing relevant attributes from the Cython
    function and attaching to a no-op function. This is somewhat brittle, since
    inspect may change, but given that inspect is written to a PEP, we hope
    it is relatively stable. Future versions of Python may allow overloading
    the inspect 'isfunction' and 'ismethod' functions / create ABC for Python
    functions. Until then, it appears that Cython won't do anything about
    compatability with the inspect module.

    Args:
        func: The function whose signature should be checked.

    Returns:
        A function signature object, which includes the names of the keyword
            arguments as well as their default values.

    Raises:
        TypeError: A type error if the signature is not supported
    """
    # The first condition for Cython functions, the latter for Cython instance
    # methods
    if is_cython(func):
        attrs = ["__code__", "__annotations__", "__defaults__", "__kwdefaults__"]

        if all(hasattr(func, attr) for attr in attrs):
            original_func = func

            def func():
                return

            for attr in attrs:
                setattr(func, attr, getattr(original_func, attr))
        else:
            raise TypeError(f"{func!r} is not a Python function we can process")

    return inspect.signature(func)


def extract_signature(func: Any, ignore_first: bool = False) -> List[Parameter]:
    """Extract the function signature from the function.

    Args:
        func: The function whose signature should be extracted.
        ignore_first: True if the first argument should be ignored. This should
            be used when func is a method of a class.

    Returns:
        List of Parameter objects representing the function signature.
    """
    signature_parameters = list(get_signature(func).parameters.values())

    if ignore_first:
        if len(signature_parameters) == 0:
            raise ValueError(
                "Methods must take a 'self' argument, but the "
                f"method '{func.__name__}' does not have one."
            )
        signature_parameters = signature_parameters[1:]

    return signature_parameters


def validate_args(
    signature_parameters: List[Parameter], args: Tuple[Any, ...], kwargs: Dict[str, Any]
) -> None:
    """Validates the arguments against the signature.

    Args:
        signature_parameters: The list of Parameter objects
            representing the function signature, obtained from
            `extract_signature`.
        args: The positional arguments passed into the function.
        kwargs: The keyword arguments passed into the function.

    Raises:
        TypeError: Raised if arguments do not fit in the function signature.
    """
    reconstructed_signature = inspect.Signature(parameters=signature_parameters)
    try:
        reconstructed_signature.bind(*args, **kwargs)
    except TypeError as exc:  # capture a friendlier stacktrace
        raise TypeError(str(exc)) from None


def flatten_args(
    signature_parameters: List[Parameter], args: Tuple[Any, ...], kwargs: Dict[str, Any]
) -> List[Any]:
    """Validates the arguments against the signature and flattens them.

    The flat list representation is a serializable format for arguments.
    Since the flatbuffer representation of function arguments is a list, we
    combine both keyword arguments and positional arguments. We represent
    this with two entries per argument value - [DUMMY_TYPE, x] for positional
    arguments and [KEY, VALUE] for keyword arguments. See the below example.
    See `recover_args` for logic restoring the flat list back to args/kwargs.

    Args:
        signature_parameters: The list of Parameter objects
            representing the function signature, obtained from
            `extract_signature`.
        args: The positional arguments passed into the function.
        kwargs: The keyword arguments passed into the function.

    Returns:
        List of args and kwargs. Non-keyword arguments are prefixed
            by internal enum DUMMY_TYPE.

    Raises:
        TypeError: Raised if arguments do not fit in the function signature.
    """
    validate_args(signature_parameters, args, kwargs)
    list_args = []
    for arg in args:
        list_args += [DUMMY_TYPE, arg]

    for keyword, arg in kwargs.items():
        list_args += [keyword, arg]
    return list_args


def recover_args(flattened_args: List[Any]) -> Tuple[List[Any], Dict[str, Any]]:
    """Recreates `args` and `kwargs` from the flattened arg list.

    Args:
        flattened_args: List of args and kwargs. This should be the output of
            `flatten_args`.

    Returns:
        args: The non-keyword arguments passed into the function.
        kwargs: The keyword arguments passed into the function.
    """
    assert (
        len(flattened_args) % 2 == 0
    ), "Flattened arguments need to be even-numbered. See `flatten_args`."
    args = []
    kwargs = {}
    for name_index in range(0, len(flattened_args), 2):
        name, arg = flattened_args[name_index], flattened_args[name_index + 1]
        if name == DUMMY_TYPE:
            args.append(arg)
        else:
            kwargs[name] = arg

    return args, kwargs
