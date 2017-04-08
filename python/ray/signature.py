from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import namedtuple
import funcsigs

FunctionSignature = namedtuple("FunctionSignature", ["sig_params",
                                                     "keyword_names",
                                                     "function_name"])


def check_signature_supported(func):
  """Check if we support the signature of this function.

  We currently do not allow remote functions to have **kwargs. We also do not
  support keyword arguments in conjunction with a *args argument.

  Args:
    func: The function whose signature should be checked.

  Raises:
    Exception: An exception is raised if the signature is not supported.
  """
  function_name = func.__name__
  sig_params = [(k, v) for k, v
                in funcsigs.signature(func).parameters.items()]

  has_vararg_param = False
  has_kwargs_param = False
  has_keyword_arg = False
  for keyword_name, parameter in funcsigs.signature(func).parameters.items():
    if parameter.kind == parameter.VAR_KEYWORD:
      has_kwargs_param = True
    if parameter.kind == parameter.VAR_POSITIONAL:
      has_vararg_param = True
    if parameter.default != funcsigs._empty:
      has_keyword_arg = True

  if has_kwargs_param:
    raise Exception("The function {} has a **kwargs argument, which is "
                    "currently not supported.".format(function_name))
  # Check if the user specified a variable number of arguments and any keyword
  # arguments.
  if has_vararg_param and has_keyword_arg:
    raise Exception("Function {} has a *args argument as well as a keyword "
                    "argument, which is currently not supported."
                    .format(function_name))


def extract_signature(func, ignore_first=False):
  """Extract the function signature from the function.

  Args:
    func: The function whose signature should be extracted.
    ignore_first: True if the first argument should be ignored. This should be
      used when func is a method of a class.

  Returns:
    A function signature object, which includes the names of the keyword
      arguments as well as their default values.
  """
  sig_params = [(k, v) for k, v
                in funcsigs.signature(func).parameters.items()]

  if ignore_first:
    if len(sig_params) == 0:
      raise Exception("Methods must take a 'self' argument, but the method "
                      "'{}' does not have one.".format(func.__name__))
    sig_params = sig_params[1:]

  # Extract the names of the keyword arguments.
  keyword_names = set()
  for keyword_name, parameter in funcsigs.signature(func).parameters.items():
    if parameter.default != funcsigs._empty:
      keyword_names.add(keyword_name)

  return FunctionSignature(sig_params, keyword_names, func.__name__)


def extend_args(function_signature, args, kwargs):
  """Extend the arguments that were passed into a function.

  This extends the arguments that were passed into a function with the default
  arguments provided in the function definition.

  Args:
    function_signature: The function signature of the function being called.
    args: The non-keyword arguments passed into the function.
    kwargs: The keyword arguments passed into the function.

  Returns:
    An extended list of arguments to pass into the function.

  Raises:
    Exception: An exception may be raised if the function cannot be called with
      these arguments.
  """
  sig_params = function_signature.sig_params
  keyword_names = function_signature.keyword_names
  function_name = function_signature.function_name

  args = list(args)

  for keyword_name in kwargs:
    if keyword_name not in keyword_names:
      raise Exception("The name '{}' is not a valid keyword argument for the "
                      "function '{}'.".format(keyword_name, function_name))

  # Fill in the remaining arguments.
  for keyword_name, param in sig_params[len(args):]:
    if keyword_name in kwargs:
      args.append(kwargs[keyword_name])
    else:
      default_value = param.default
      if default_value != funcsigs._empty:
        args.append(default_value)
      else:
        # This means that there is a missing argument. Unless this is the last
        # argument and it is a *args argument in which case it can be omitted.
        if param.kind != param.VAR_POSITIONAL:
          raise Exception("No value was provided for the argument '{}' for "
                          "the function '{}'.".format(keyword_name,
                                                      function_name))
  return args
