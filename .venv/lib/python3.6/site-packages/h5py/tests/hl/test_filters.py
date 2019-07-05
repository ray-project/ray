# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

"""
    Tests the h5py._hl.filters module.

"""
from __future__ import absolute_import

import numpy as np
import h5py

from ..common import ut, TestCase


class TestFilters(TestCase):

    def setUp(self):
        """ like TestCase.setUp but also store the file path """
        self.path = self.mktemp()
        self.f = h5py.File(self.path, 'w')

    @ut.skipUnless(h5py.h5z.filter_avail(h5py.h5z.FILTER_SZIP), 'szip filter required')
    def test_wr_szip_fletcher32_64bit(self):
        """ test combination of szip, fletcher32, and 64bit arrays

        The fletcher32 checksum must be computed after the szip
        compression is applied.

        References:
        - GitHub issue #953
        - https://lists.hdfgroup.org/pipermail/
          hdf-forum_lists.hdfgroup.org/2018-January/010753.html
        """
        self.f.create_dataset("test_data",
                              data=np.zeros(10000, dtype=np.float64),
                              fletcher32=True,
                              compression="szip",
                              )
        self.f.close()

        with h5py.File(self.path, "r") as h5:
            # Access the data which will compute the fletcher32
            # checksum and raise an OSError if something is wrong.
            h5["test_data"][0]

    def test_wr_scaleoffset_fletcher32(self):
        """ make sure that scaleoffset + fletcher32 is prevented
        """
        data = np.linspace(0, 1, 100)
        with self.assertRaises(ValueError):
            self.f.create_dataset("test_data",
                                  data=data,
                                  fletcher32=True,
                                  # retain 3 digits after the decimal point
                                  scaleoffset=3,
                                  )
