from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import namedtuple
import funcsigs

FunctionSignature = namedtuple("FunctionSignature", ["arg_names",
                                                     "arg_defaults",
                                                     "arg_is_positionals",
                                                     "keyword_names",
                                                     "function_name"])
"""This class is used to represent a function signature.

Attributes:
  keyword_names: The names of the functions keyword arguments. This is used to
    test if an incorrect keyword argument has been passed to the function.
  arg_defaults: A dictionary mapping from argument name to argument default
    value. If the argument is not a keyword argument, the default value will be
    funcsigs._empty.
  arg_is_positionals: A dictionary mapping from argument name to a bool. The
    bool will be true if the argument is a *args argument. Otherwise it will be
    false.
  function_name: The name of the function whose signature is being inspected.
    This is used for printing better error messages.
"""


def check_signature_supported(func, warn=False):
  """Check if we support the signature of this function.

  We currently do not allow remote functions to have **kwargs. We also do not
  support keyword arguments in conjunction with a *args argument.

  Args:
    func: The function whose signature should be checked.
    warn: If this is true, a warning will be printed if the signature is not
      supported. If it is false, an exception will be raised if the signature
      is not supported.

  Raises:
    Exception: An exception is raised if the signature is not supported.
  """
  function_name = func.__name__
  sig_params = [(k, v) for k, v
                in funcsigs.signature(func).parameters.items()]

  has_vararg_param = False
  has_kwargs_param = False
  has_keyword_arg = False
  for keyword_name, parameter in sig_params:
    if parameter.kind == parameter.VAR_KEYWORD:
      has_kwargs_param = True
    if parameter.kind == parameter.VAR_POSITIONAL:
      has_vararg_param = True
    if parameter.default != funcsigs._empty:
      has_keyword_arg = True

  if has_kwargs_param:
    message = ("The function {} has a **kwargs argument, which is "
               "currently not supported.".format(function_name))
    if warn:
      print(message)
    else:
      raise Exception(message)
  # Check if the user specified a variable number of arguments and any keyword
  # arguments.
  if has_vararg_param and has_keyword_arg:
    message = ("Function {} has a *args argument as well as a keyword "
               "argument, which is currently not supported."
               .format(function_name))
    if warn:
      print(message)
    else:
      raise Exception(message)


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
  for keyword_name, parameter in sig_params:
    if parameter.default != funcsigs._empty:
      keyword_names.add(keyword_name)

  # Construct the argument default values and other argument information.
  arg_names = []
  arg_defaults = []
  arg_is_positionals = []
  for keyword_name, parameter in sig_params:
    arg_names.append(keyword_name)
    arg_defaults.append(parameter.default)
    arg_is_positionals.append(parameter.kind == parameter.VAR_POSITIONAL)

  return FunctionSignature(arg_names, arg_defaults, arg_is_positionals,
                           keyword_names, func.__name__)


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
  arg_names = function_signature.arg_names
  arg_defaults = function_signature.arg_defaults
  arg_is_positionals = function_signature.arg_is_positionals
  keyword_names = function_signature.keyword_names
  function_name = function_signature.function_name

  args = list(args)

  for keyword_name in kwargs:
    if keyword_name not in keyword_names:
      raise Exception("The name '{}' is not a valid keyword argument for the "
                      "function '{}'.".format(keyword_name, function_name))

  # Fill in the remaining arguments.
  zipped_info = list(zip(arg_names, arg_defaults,
                         arg_is_positionals))[len(args):]
  for keyword_name, default_value, is_positional in zipped_info:
    if keyword_name in kwargs:
      args.append(kwargs[keyword_name])
    else:
      if default_value != funcsigs._empty:
        args.append(default_value)
      else:
        # This means that there is a missing argument. Unless this is the last
        # argument and it is a *args argument in which case it can be omitted.
        if not is_positional:
          raise Exception("No value was provided for the argument '{}' for "
                          "the function '{}'.".format(keyword_name,
                                                      function_name))

  too_many_arguments = (len(args) > len(arg_names) and
                        (len(arg_is_positionals) == 0 or
                         not arg_is_positionals[-1]))
  if too_many_arguments:
    raise Exception("Too many arguments were passed to the function '{}'"
                    .format(function_name))
  return args
