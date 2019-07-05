# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

"""
    Tests for the (internal) selections module
"""

from __future__ import absolute_import

import numpy as np
import h5py
import h5py._hl.selections2 as sel

from ..common import TestCase, ut

class TestTypeGeneration(TestCase):

    """
        Internal feature: Determine output types from dataset dtype and fields.
    """

    def test_simple(self):
        """ Non-compound types are handled appropriately """
        dt = np.dtype('i')
        out, format = sel.read_dtypes(dt, ())
        self.assertEqual(out, format)
        self.assertEqual(out, np.dtype('i'))

    def test_simple_fieldexc(self):
        """ Field names for non-field types raises ValueError """
        dt = np.dtype('i')
        with self.assertRaises(ValueError):
            out, format = sel.read_dtypes(dt, ('a',))

    def test_compound_simple(self):
        """ Compound types with elemental subtypes """
        dt = np.dtype( [('a','i'), ('b','f'), ('c','|S10')] )

        # Implicit selection of all fields -> all fields
        out, format = sel.read_dtypes(dt, ())
        self.assertEqual(out, format)
        self.assertEqual(out, dt)

        # Explicit selection of fields -> requested fields
        out, format = sel.read_dtypes(dt, ('a','b'))
        self.assertEqual(out, format)
        self.assertEqual(out, np.dtype( [('a','i'), ('b','f')] ))

        # Explicit selection of exactly one field -> no fields
        out, format = sel.read_dtypes(dt, ('a',))
        self.assertEqual(out, np.dtype('i'))
        self.assertEqual(format, np.dtype( [('a','i')] ))


class TestScalarSliceRules(TestCase):

    """
        Internal feature: selections rules for scalar datasets
    """

    def setUp(self):
        self.f = h5py.File(self.mktemp(), 'w')
        self.dsid = self.f.create_dataset('x', ()).id

    def tearDown(self):
        if self.f:
            self.f.close()

    def test_args(self):
        """ Permissible arguments for scalar slicing """
        shape, selection = sel.read_selections_scalar(self.dsid, ())
        self.assertEqual(shape, None)
        self.assertEqual(selection.get_select_npoints(), 1)

        shape, selection = sel.read_selections_scalar(self.dsid, (Ellipsis,))
        self.assertEqual(shape, ())
        self.assertEqual(selection.get_select_npoints(), 1)

        with self.assertRaises(ValueError):
            shape, selection = sel.read_selections_scalar(self.dsid, (1,))
