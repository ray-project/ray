# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

"""
    Attributes testing module

    Covers all operations which access the .attrs property, with the
    exception of data read/write and type conversion.  Those operations
    are tested by module test_attrs_data.
"""

from __future__ import absolute_import

import six

import numpy as np
import collections

from ..common import TestCase, ut

import h5py
from h5py import File
from h5py import h5a,  h5t
from h5py import AttributeManager


class BaseAttrs(TestCase):

    def setUp(self):
        self.f = File(self.mktemp(), 'w')

    def tearDown(self):
        if self.f:
            self.f.close()


class TestAccess(BaseAttrs):

    """
        Feature: Attribute creation/retrieval via special methods
    """

    def test_create(self):
        """ Attribute creation by direct assignment """
        self.f.attrs['a'] = 4.0
        self.assertEqual(list(self.f.attrs.keys()), ['a'])
        self.assertEqual(self.f.attrs['a'], 4.0)

    def test_overwrite(self):
        """ Attributes are silently overwritten """
        self.f.attrs['a'] = 4.0
        self.f.attrs['a'] = 5.0
        self.assertEqual(self.f.attrs['a'], 5.0)

    def test_rank(self):
        """ Attribute rank is preserved """
        self.f.attrs['a'] = (4.0, 5.0)
        self.assertEqual(self.f.attrs['a'].shape, (2,))
        self.assertArrayEqual(self.f.attrs['a'], np.array((4.0,5.0)))

    def test_single(self):
        """ Attributes of shape (1,) don't become scalars """
        self.f.attrs['a'] = np.ones((1,))
        out = self.f.attrs['a']
        self.assertEqual(out.shape, (1,))
        self.assertEqual(out[()], 1)

    def test_access_exc(self):
        """ Attempt to access missing item raises KeyError """
        with self.assertRaises(KeyError):
            self.f.attrs['a']


class TestDelete(BaseAttrs):

    """
        Feature: Deletion of attributes using __delitem__
    """

    def test_delete(self):
        """ Deletion via "del" """
        self.f.attrs['a'] = 4.0
        self.assertIn('a', self.f.attrs)
        del self.f.attrs['a']
        self.assertNotIn('a', self.f.attrs)

    def test_delete_exc(self):
        """ Attempt to delete missing item raises KeyError """
        with self.assertRaises(KeyError):
            del self.f.attrs['a']


class TestUnicode(BaseAttrs):

    """
        Feature: Attributes can be accessed via Unicode or byte strings
    """

    def test_ascii(self):
        """ Access via pure-ASCII byte string """
        self.f.attrs[b"ascii"] = 42
        out = self.f.attrs[b"ascii"]
        self.assertEqual(out, 42)

    def test_raw(self):
        """ Access via non-ASCII byte string """
        name = b"non-ascii\xfe"
        self.f.attrs[name] = 42
        out = self.f.attrs[name]
        self.assertEqual(out, 42)

    def test_unicode(self):
        """ Access via Unicode string with non-ascii characters """
        name = u"Omega" + six.unichr(0x03A9)
        self.f.attrs[name] = 42
        out = self.f.attrs[name]
        self.assertEqual(out, 42)


class TestCreate(BaseAttrs):

    """
        Options for explicit attribute creation
    """

    def test_named(self):
        """ Attributes created from named types link to the source type object
        """
        self.f['type'] = np.dtype('u8')
        self.f.attrs.create('x', 42, dtype=self.f['type'])
        self.assertEqual(self.f.attrs['x'], 42)
        aid = h5a.open(self.f.id, b'x')
        htype = aid.get_type()
        htype2 = self.f['type'].id
        self.assertEqual(htype, htype2)
        self.assertTrue(htype.committed())


class TestMutableMapping(BaseAttrs):
    '''Tests if the registration of AttributeManager as a MutableMapping
    behaves as expected
    '''
    def test_resolution(self):
        assert issubclass(AttributeManager, collections.MutableMapping)
        assert isinstance(self.f.attrs, collections.MutableMapping)

    def test_validity(self):
        '''
        Test that the required functions are implemented.
        '''
        AttributeManager.__getitem__
        AttributeManager.__setitem__
        AttributeManager.__delitem__
        AttributeManager.__iter__
        AttributeManager.__len__

class TestVlen(BaseAttrs):
    def test_vlen(self):
        a = np.array([np.arange(3), np.arange(4)],
            dtype=h5t.special_dtype(vlen=int))
        self.f.attrs['a'] = a
        self.assertArrayEqual(self.f.attrs['a'][0], a[0])

class TestTrackOrder(BaseAttrs):
    def fill_attrs(self, track_order):
        attrs = self.f.create_group('test', track_order=track_order).attrs
        for i in range(100):
            attrs[str(i)] = i
        return attrs

    @ut.skipUnless(h5py.version.hdf5_version_tuple >= (1, 10, 5), 'HDF5 1.10.5 required')
    # https://forum.hdfgroup.org/t/bug-h5arename-fails-unexpectedly/4881
    def test_track_order(self):
        attrs = self.fill_attrs(track_order=True)  # creation order
        self.assertEqual(list(attrs),
                         [u'' + str(i) for i in range(100)])

    def test_no_track_order(self):
        attrs = self.fill_attrs(track_order=False)  # name alphanumeric
        self.assertEqual(list(attrs),
                         sorted([u'' + str(i) for i in range(100)]))
