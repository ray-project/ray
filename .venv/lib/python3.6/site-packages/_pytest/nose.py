""" run test suites written for nose. """
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys

import six

import pytest
from _pytest import python
from _pytest import runner
from _pytest import unittest
from _pytest.config import hookimpl


def get_skip_exceptions():
    skip_classes = set()
    for module_name in ("unittest", "unittest2", "nose"):
        mod = sys.modules.get(module_name)
        if hasattr(mod, "SkipTest"):
            skip_classes.add(mod.SkipTest)
    return tuple(skip_classes)


def pytest_runtest_makereport(item, call):
    if call.excinfo and call.excinfo.errisinstance(get_skip_exceptions()):
        # let's substitute the excinfo with a pytest.skip one
        call2 = runner.CallInfo.from_call(
            lambda: pytest.skip(six.text_type(call.excinfo.value)), call.when
        )
        call.excinfo = call2.excinfo


@hookimpl(trylast=True)
def pytest_runtest_setup(item):
    if is_potential_nosetest(item):
        if not call_optional(item.obj, "setup"):
            # call module level setup if there is no object level one
            call_optional(item.parent.obj, "setup")
        # XXX this implies we only call teardown when setup worked
        item.session._setupstate.addfinalizer((lambda: teardown_nose(item)), item)


def teardown_nose(item):
    if is_potential_nosetest(item):
        if not call_optional(item.obj, "teardown"):
            call_optional(item.parent.obj, "teardown")
        # if hasattr(item.parent, '_nosegensetup'):
        #    #call_optional(item._nosegensetup, 'teardown')
        #    del item.parent._nosegensetup


def is_potential_nosetest(item):
    # extra check needed since we do not do nose style setup/teardown
    # on direct unittest style classes
    return isinstance(item, python.Function) and not isinstance(
        item, unittest.TestCaseFunction
    )


def call_optional(obj, name):
    method = getattr(obj, name, None)
    isfixture = hasattr(method, "_pytestfixturefunction")
    if method is not None and not isfixture and callable(method):
        # If there's any problems allow the exception to raise rather than
        # silently ignoring them
        method()
        return True
