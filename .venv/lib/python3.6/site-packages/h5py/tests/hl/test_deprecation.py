# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2018 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

"""
Tests that deprecations work correctly
"""
from __future__ import absolute_import

import h5py

from ..common import ut, TestCase


class TestDeprecations(TestCase):
    def test_highlevel_access(self):
        warning_message = (
            "The h5py.highlevel module is deprecated, code should import "
            "directly from h5py, e.g. 'from h5py import File'."
        )
        with self.assertWarnsRegex(H5pyDeprecationWarning, warning_message) as warning:
            hl = h5py.highlevel
