# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

"""
    Tests the h5py.File object.
"""

from __future__ import absolute_import

import threading
import h5py

from ..common import ut, TestCase


class TestErrorPrinting(TestCase):

    """
        Verify the error printing is squashed in all threads.
    """

    def test_printing(self):
        """ No console messages should be shown from membership tests """
        # Unfortunately we can't have this test assert anything, as
        # HDF5 writes directly to stderr.  But it will show up in the
        # console output.

        import threading

        def test():
            with h5py.File(self.mktemp(), 'w') as newfile:
                try:
                    doesnt_exist = newfile['doesnt_exist'].value
                except KeyError:
                    pass

        th = threading.Thread(target=test)
        th.start()
        th.join()

    def test_attr_printing(self):
        """ No console messages should be shown for non-existing attributes """

        def test():

            with h5py.File(self.mktemp(), 'w') as newfile:
                newfile['newdata'] = [1,2,3]
                try:
                    nonexistent_attr = newfile['newdata'].attrs['nonexistent_attr']
                except KeyError:
                    pass

        th = threading.Thread(target=test)
        th.start()
        th.join()
