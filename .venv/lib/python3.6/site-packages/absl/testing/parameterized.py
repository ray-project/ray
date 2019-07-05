# Copyright 2017 The Abseil Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Adds support for parameterized tests to Python's unittest TestCase class.

A parameterized test is a method in a test case that is invoked with different
argument tuples.

A simple example:

  class AdditionExample(parameterized.TestCase):
    @parameterized.parameters(
       (1, 2, 3),
       (4, 5, 9),
       (1, 1, 3))
    def testAddition(self, op1, op2, result):
      self.assertEqual(result, op1 + op2)


Each invocation is a separate test case and properly isolated just
like a normal test method, with its own setUp/tearDown cycle. In the
example above, there are three separate testcases, one of which will
fail due to an assertion error (1 + 1 != 3).

Parameters for invididual test cases can be tuples (with positional parameters)
or dictionaries (with named parameters):

  class AdditionExample(parameterized.TestCase):
    @parameterized.parameters(
       {'op1': 1, 'op2': 2, 'result': 3},
       {'op1': 4, 'op2': 5, 'result': 9},
    )
    def testAddition(self, op1, op2, result):
      self.assertEqual(result, op1 + op2)

If a parameterized test fails, the error message will show the
original test name (which is modified internally) and the arguments
for the specific invocation, which are part of the string returned by
the shortDescription() method on test cases.

The id method of the test, used internally by the unittest framework, is also
modified to show the arguments (but note that the name reported by `id()`
doesn't match the actual test name, see below). To make sure that test names
stay the same across several invocations, object representations like

  >>> class Foo(object):
  ...  pass
  >>> repr(Foo())
  '<__main__.Foo object at 0x23d8610>'

are turned into '<__main__.Foo>'. When selecting a subset of test cases to run
on the command-line, the test cases contain an index suffix for each argument
in the order they were passed to `parameters()` (eg. testAddition0,
testAddition1, etc.) This naming scheme is subject to change; for more reliable
and stable names, especially in test logs, use `named_parameters()` instead.

Tests using `named_parameters()` are similar to `parameters()`, except only
tuples or dicts of args are supported. For tuples, the first parameter arg
has to be a string (or an object that returns an apt name when converted via
str()). For dicts, a value for the key 'testcase_name' must be present and must
be a string (or an object that returns an apt name when converted via str()):

  class NamedExample(parameterized.TestCase):
    @parameterized.named_parameters(
       ('Normal', 'aa', 'aaa', True),
       ('EmptyPrefix', '', 'abc', True),
       ('BothEmpty', '', '', True))
    def testStartsWith(self, prefix, string, result):
      self.assertEqual(result, string.startswith(prefix))

  class NamedExample(parameterized.TestCase):
    @parameterized.named_parameters(
       {'testcase_name': 'Normal',
        'result': True, 'string': 'aaa', 'prefix': 'aa'},
       {'testcase_name': 'EmptyPrefix',
        'result': True, 'string': 'abc', 'prefix': ''},
       {'testcase_name': 'BothEmpty',
        'result': True, 'string': '', 'prefix': ''})
    def testStartsWith(self, prefix, string, result):
      self.assertEqual(result, string.startswith(prefix))

Named tests also have the benefit that they can be run individually
from the command line:

  $ testmodule.py NamedExample.testStartsWithNormal
  .
  --------------------------------------------------------------------
  Ran 1 test in 0.000s

  OK

Parameterized Classes
=====================
If invocation arguments are shared across test methods in a single
TestCase class, instead of decorating all test methods
individually, the class itself can be decorated:

  @parameterized.parameters(
    (1, 2, 3),
    (4, 5, 9))
  class ArithmeticTest(parameterized.TestCase):
    def testAdd(self, arg1, arg2, result):
      self.assertEqual(arg1 + arg2, result)

    def testSubtract(self, arg1, arg2, result):
      self.assertEqual(result - arg1, arg2)

Inputs from Iterables
=====================
If parameters should be shared across several test cases, or are dynamically
created from other sources, a single non-tuple iterable can be passed into
the decorator. This iterable will be used to obtain the test cases:

  class AdditionExample(parameterized.TestCase):
    @parameterized.parameters(
      c.op1, c.op2, c.result for c in testcases
    )
    def testAddition(self, op1, op2, result):
      self.assertEqual(result, op1 + op2)


Single-Argument Test Methods
============================
If a test method takes only one argument, the single arguments must not be
wrapped into a tuple:

  class NegativeNumberExample(parameterized.TestCase):
    @parameterized.parameters(
       -1, -3, -4, -5
    )
    def testIsNegative(self, arg):
      self.assertTrue(IsNegative(arg))


List/tuple as a Single Argument
===============================
If a test method takes a single argument of a list/tuple, it must be wrapped
inside a tuple:

  class ZeroSumExample(parameterized.TestCase):
    @parameterized.parameters(
      ([-1, 0, 1], ),
      ([-2, 0, 2], ),
    )
    def testSumIsZero(self, arg):
      self.assertEqual(0, sum(arg))
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import functools
import re
import types
import unittest

from absl.testing import absltest
import six

_ADDR_RE = re.compile(r'\<([a-zA-Z0-9_\-\.]+) object at 0x[a-fA-F0-9]+\>')
_NAMED = object()
_ARGUMENT_REPR = object()
_NAMED_DICT_KEY = 'testcase_name'


class NoTestsError(Exception):
  """Raised when parameterized decorators do not generate any tests."""


class DuplicateTestNameError(Exception):
  """Raised when a parameterized test has the same test name multiple times."""

  def __init__(self, test_class_name, new_test_name, original_test_name):
    super(DuplicateTestNameError, self).__init__(
        'Duplicate parameterized test name in {}: generated test name {!r} '
        '(generated from {!r}) already exists. Consider using '
        'named_parameters() to give your tests unique names and/or renaming '
        'the conflicting test method.'.format(
            test_class_name, new_test_name, original_test_name))


def _clean_repr(obj):
  return _ADDR_RE.sub(r'<\1>', repr(obj))


def _non_string_or_bytes_iterable(obj):
  return (isinstance(obj, collections.Iterable) and
          not isinstance(obj, six.text_type) and
          not isinstance(obj, six.binary_type))


def _format_parameter_list(testcase_params):
  if isinstance(testcase_params, collections.Mapping):
    return ', '.join('%s=%s' % (argname, _clean_repr(value))
                     for argname, value in six.iteritems(testcase_params))
  elif _non_string_or_bytes_iterable(testcase_params):
    return ', '.join(map(_clean_repr, testcase_params))
  else:
    return _format_parameter_list((testcase_params,))


class _ParameterizedTestIter(object):
  """Callable and iterable class for producing new test cases."""

  def __init__(self, test_method, testcases, naming_type, original_name=None):
    """Returns concrete test functions for a test and a list of parameters.

    The naming_type is used to determine the name of the concrete
    functions as reported by the unittest framework. If naming_type is
    _FIRST_ARG, the testcases must be tuples, and the first element must
    have a string representation that is a valid Python identifier.

    Args:
      test_method: The decorated test method.
      testcases: (list of tuple/dict) A list of parameter tuples/dicts for
          individual test invocations.
      naming_type: The test naming type, either _NAMED or _ARGUMENT_REPR.
      original_name: The original test method name. When decorated on a test
          method, None is passed to __init__ and test_method.__name__ is used.
          Note test_method.__name__ might be different than the original defined
          test method because of the use of other decorators. A more accurate
          value is set by TestGeneratorMetaclass.__new__ later.
    """
    self._test_method = test_method
    self.testcases = testcases
    self._naming_type = naming_type
    if original_name is None:
      original_name = test_method.__name__
    self._original_name = original_name
    self.__name__ = _ParameterizedTestIter.__name__

  def __call__(self, *args, **kwargs):
    raise RuntimeError('You appear to be running a parameterized test case '
                       'without having inherited from parameterized.'
                       'TestCase. This is bad because none of '
                       'your test cases are actually being run. You may also '
                       'be using a mock annotation before the parameterized '
                       'one, in which case you should reverse the order.')

  def __iter__(self):
    test_method = self._test_method
    naming_type = self._naming_type
    extra_ids = collections.defaultdict(int)

    def make_bound_param_test(testcase_params):
      @functools.wraps(test_method)
      def bound_param_test(self):
        if isinstance(testcase_params, collections.Mapping):
          test_method(self, **testcase_params)
        elif _non_string_or_bytes_iterable(testcase_params):
          test_method(self, *testcase_params)
        else:
          test_method(self, testcase_params)

      if naming_type is _NAMED:
        # Signal the metaclass that the name of the test function is unique
        # and descriptive.
        bound_param_test.__x_use_name__ = True

        testcase_name = None
        if isinstance(testcase_params, collections.Mapping):
          if _NAMED_DICT_KEY not in testcase_params:
            raise RuntimeError(
                'Dict for named tests must contain key "%s"' % _NAMED_DICT_KEY)
          # Create a new dict to avoid modifying the supplied testcase_params.
          testcase_name = testcase_params[_NAMED_DICT_KEY]
          testcase_params = {k: v for k, v in six.iteritems(testcase_params)
                             if k != _NAMED_DICT_KEY}
        elif _non_string_or_bytes_iterable(testcase_params):
          testcase_name = testcase_params[0]
          testcase_params = testcase_params[1:]
        else:
          raise RuntimeError(
              'Named tests must be passed a dict or non-string iterable.')

        test_method_name = self._original_name
        # Support PEP-8 underscore style for test naming if used.
        if (test_method_name.startswith('test_')
            and testcase_name
            and not testcase_name.startswith('_')):
          test_method_name += '_'

        bound_param_test.__name__ = test_method_name + str(testcase_name)
      elif naming_type is _ARGUMENT_REPR:
        # If it's a generator, convert it to a tuple and treat them as
        # parameters.
        if isinstance(testcase_params, types.GeneratorType):
          testcase_params = tuple(testcase_params)
        # The metaclass creates a unique, but non-descriptive method name for
        # _ARGUMENT_REPR tests using an indexed suffix.
        # To keep test names descriptive, only the original method name is used.
        # To make sure test names are unique, we add a unique descriptive suffix
        # __x_extra_id__ for every test.
        extra_id = '(%s)' % (_format_parameter_list(testcase_params),)
        extra_ids[extra_id] += 1
        while extra_ids[extra_id] > 1:
          extra_id = '%s (%d)' % (extra_id, extra_ids[extra_id])
          extra_ids[extra_id] += 1
        bound_param_test.__x_extra_id__ = extra_id
      else:
        raise RuntimeError('%s is not a valid naming type.' % (naming_type,))

      bound_param_test.__doc__ = '%s(%s)' % (
          bound_param_test.__name__, _format_parameter_list(testcase_params))
      if test_method.__doc__:
        bound_param_test.__doc__ += '\n%s' % (test_method.__doc__,)
      return bound_param_test

    return (make_bound_param_test(c) for c in self.testcases)


def _modify_class(class_object, testcases, naming_type):
  assert not getattr(class_object, '_test_method_ids', None), (
      'Cannot add parameters to %s. Either it already has parameterized '
      'methods, or its super class is also a parameterized class.' % (
          class_object,))
  class_object._test_method_ids = test_method_ids = {}
  for name, obj in six.iteritems(class_object.__dict__.copy()):
    if (name.startswith(unittest.TestLoader.testMethodPrefix)
        and isinstance(obj, types.FunctionType)):
      delattr(class_object, name)
      methods = {}
      _update_class_dict_for_param_test_case(
          class_object.__name__, methods, test_method_ids, name,
          _ParameterizedTestIter(obj, testcases, naming_type, name))
      for name, meth in six.iteritems(methods):
        setattr(class_object, name, meth)


def _parameter_decorator(naming_type, testcases):
  """Implementation of the parameterization decorators.

  Args:
    naming_type: The naming type.
    testcases: Testcase parameters.

  Raises:
    NoTestsError: Raised when the decorator generates no tests.

  Returns:
    A function for modifying the decorated object.
  """
  def _apply(obj):
    if isinstance(obj, type):
      _modify_class(obj, testcases, naming_type)
      return obj
    else:
      return _ParameterizedTestIter(obj, testcases, naming_type)

  if (len(testcases) == 1 and
      not isinstance(testcases[0], tuple) and
      not (naming_type == _NAMED and
           isinstance(testcases[0], collections.Mapping))):
    # Support using a single non-tuple parameter as a list of test cases.
    # Note in named parameters case, the single non-tuple parameter can't be
    # Mapping either, which means a single named parameter case.
    assert _non_string_or_bytes_iterable(testcases[0]), (
        'Single parameter argument must be a non-string iterable')
    testcases = testcases[0]

  if not isinstance(testcases, collections.Sequence):
    testcases = list(testcases)
  if not testcases:
    raise NoTestsError(
        'parameterized test decorators did not generate any tests. '
        'Make sure you specify non-empty parameters, '
        'and do not reuse generators more than once.')

  return _apply


def parameters(*testcases):
  """A decorator for creating parameterized tests.

  See the module docstring for a usage example.

  Args:
    *testcases: Parameters for the decorated method, either a single
        iterable, or a list of tuples/dicts/objects (for tests with only one
        argument).

  Raises:
    NoTestsError: Raised when the decorator generates no tests.

  Returns:
     A test generator to be handled by TestGeneratorMetaclass.
  """
  return _parameter_decorator(_ARGUMENT_REPR, testcases)


def named_parameters(*testcases):
  """A decorator for creating parameterized tests.

  See the module docstring for a usage example. For every parameter tuple
  passed, the first element of the tuple should be a string and will be appended
  to the name of the test method. Each parameter dict passed must have a value
  for the key "testcase_name", the string representation of that value will be
  appended to the name of the test method.

  Args:
    *testcases: Parameters for the decorated method, either a single iterable,
        or a list of tuples or dicts.

  Raises:
    NoTestsError: Raised when the decorator generates no tests.

  Returns:
     A test generator to be handled by TestGeneratorMetaclass.
  """
  return _parameter_decorator(_NAMED, testcases)


class TestGeneratorMetaclass(type):
  """Metaclass for test cases with test generators.

  A test generator is an iterable in a testcase that produces callables. These
  callables must be single-argument methods. These methods are injected into
  the class namespace and the original iterable is removed. If the name of the
  iterable conforms to the test pattern, the injected methods will be picked
  up as tests by the unittest framework.

  In general, it is supposed to be used in conjuction with the
  parameters decorator.
  """

  def __new__(mcs, class_name, bases, dct):
    test_method_ids = dct.setdefault('_test_method_ids', {})
    for name, obj in six.iteritems(dct.copy()):
      if (name.startswith(unittest.TestLoader.testMethodPrefix) and
          _non_string_or_bytes_iterable(obj)):
        if isinstance(obj, _ParameterizedTestIter):
          # Update the original test method name so it's more accurate.
          # The mismatch might happen when another decorator is used inside
          # the parameterized decrators, and the inner decorator doesn't
          # preserve its __name__.
          # `obj` might be a generator, not _ParameterizedTestIter.
          obj._original_name = name
        iterator = iter(obj)
        dct.pop(name)
        _update_class_dict_for_param_test_case(
            class_name, dct, test_method_ids, name, iterator)
    # If the base class is a subclass of parameterized.TestCase, inherit its
    # _test_method_ids too.
    for base in bases:
      # Check if the base has _test_method_ids first, then check if it's a
      # subclass of parameterized.TestCase. Otherwise when this is called for
      # the parameterized.TestCase definition itself, this raises because
      # itself is not defined yet. This works as long as absltest.TestCase does
      # not define _test_method_ids.
      if getattr(base, '_test_method_ids', None) and issubclass(base, TestCase):
        for test_method, test_method_id in six.iteritems(base._test_method_ids):
          # test_method may both exists in base and this class.
          # This class's method overrides base class's.
          # That's why it should only inherit it if it does not exist.
          test_method_ids.setdefault(test_method, test_method_id)

    return type.__new__(mcs, class_name, bases, dct)


def _update_class_dict_for_param_test_case(
    test_class_name, dct, test_method_ids, name, iterator):
  """Adds individual test cases to a dictionary.

  Args:
    test_class_name: The name of the class tests are added to.
    dct: The target dictionary.
    test_method_ids: The dictionary for mapping names to test IDs.
    name: The original name of the test case.
    iterator: The iterator generating the individual test cases.

  Raises:
    DuplicateTestNameError: Raised when a test name occurs multiple times.
  """
  for idx, func in enumerate(iterator):
    assert callable(func), 'Test generators must yield callables, got %r' % (
        func,)
    if getattr(func, '__x_use_name__', False):
      original_name = func.__name__
      new_name = original_name
    else:
      original_name = name
      new_name = '%s%d' % (original_name, idx)

    if new_name in dct:
      raise DuplicateTestNameError(test_class_name, new_name, original_name)

    dct[new_name] = func
    test_method_id = original_name + getattr(func, '__x_extra_id__', '')
    assert test_method_id not in test_method_ids.values(), (
        'Id of parameterized test case "%s" not unique' % (test_method_id,))
    test_method_ids[new_name] = test_method_id


class TestCase(six.with_metaclass(TestGeneratorMetaclass, absltest.TestCase)):
  """Base class for test cases using the parameters decorator."""

  def __str__(self):
    return '%s (%s)' % (
        self._test_method_ids.get(self._testMethodName, self._testMethodName),
        unittest.util.strclass(self.__class__))

  def id(self):
    """Returns the descriptive ID of the test.

    This is used internally by the unittesting framework to get a name
    for the test to be used in reports.

    Returns:
      The test id.
    """
    return '%s.%s' % (
        unittest.util.strclass(self.__class__),
        # When a test method is NOT decorated, it doesn't exist in
        # _test_method_ids. Use the _testMethodName directly.
        self._test_method_ids.get(self._testMethodName, self._testMethodName))


# This function is kept CamelCase because it's used as a class's base class.
def CoopTestCase(other_base_class):  # pylint: disable=invalid-name
  """Returns a new base class with a cooperative metaclass base.

  This enables the TestCase to be used in combination
  with other base classes that have custom metaclasses, such as
  mox.MoxTestBase.

  Only works with metaclasses that do not override type.__new__.

  Example:

    from absl.testing import parameterized

    class ExampleTest(parameterized.CoopTestCase(OtherTestCase)):
      ...

  Args:
    other_base_class: (class) A test case base class.

  Returns:
    A new class object.
  """
  metaclass = type(
      'CoopMetaclass',
      (other_base_class.__metaclass__,
       TestGeneratorMetaclass), {})
  return metaclass(
      'CoopTestCase',
      (other_base_class, TestCase), {})
