# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

"""
    Tests the h5py.AttributeManager.create() method.
"""

from __future__ import absolute_import

import numpy as np
import h5py

from ..common import ut, TestCase

class TestArray(TestCase):

    """
        Check that top-level array types can be created and read.
    """

    def test_int(self):
        # See issue 498

        dt = np.dtype('(3,)i')
        data = np.arange(3, dtype='i')

        self.f.attrs.create('x', data=data, dtype=dt)

        aid = h5py.h5a.open(self.f.id, b'x')

        htype = aid.get_type()
        self.assertEqual(htype.get_class(), h5py.h5t.ARRAY)

        out = self.f.attrs['x']

        self.assertArrayEqual(out, data)

    def test_string_dtype(self):
        # See issue 498 discussion

        self.f.attrs.create('x', data=42, dtype='i8')

    def test_tuple_of_unicode(self):
        # Test that a tuple of unicode strings can be set as an attribute. It will
        # be converted to a numpy array of vlen unicode type:
        data = (u'a', u'b')
        self.f.attrs.create('x', data=data)
        result = self.f.attrs['x']
        self.assertTrue(all(result == data))
        self.assertEqual(result.dtype, np.dtype('O'))

        # However, a numpy array of type U being passed in will not be
        # automatically converted, and should raise an error as it does
        # not map to a h5py dtype
        data_as_U_array = np.array(data)
        self.assertEqual(data_as_U_array.dtype, np.dtype('U1'))
        with self.assertRaises(TypeError):
            self.f.attrs.create('y', data=data_as_U_array)
