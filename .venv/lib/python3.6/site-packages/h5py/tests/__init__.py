# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

from __future__ import print_function

import sys
from .common import ut

from . import old, hl

MODULES = old.MODULES + hl.MODULES


def mname(obj):
    """ Get the full dotted name of the test method """
    mod_name = obj.__class__.__module__.replace('h5py.tests.','')
    return "%s.%s.%s" % (mod_name, obj.__class__.__name__, obj._testMethodName)


def run_tests(verbose=False):
    """ Run tests with TextTestRunner.  Returns a TestResult instance.

    """
    suite = ut.TestSuite()
    for m in MODULES:
        suite.addTests(ut.defaultTestLoader.loadTestsFromModule(m))
    result = ut.TextTestRunner(verbosity=1).run(suite)

    if verbose:
        for (case, reason) in result.skipped:
            print("S  %s (%s)" % (mname(case), reason), file=sys.stderr)
        for (case, reason) in result.expectedFailures:
            print("X  %s" % mname(case), file=sys.stderr)

    return result
