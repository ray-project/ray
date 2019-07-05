# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

"""
    File object test module.

    Tests all aspects of File objects, including their creation.
"""

from __future__ import absolute_import, with_statement

import os, stat
from sys import platform
import tempfile

import six

from ..common import ut, TestCase, UNICODE_FILENAMES, closed_tempfile
from h5py import File
import h5py

try:
    import pathlib
except ImportError:
    pathlib = None


mpi = h5py.get_config().mpi

class TestFileOpen(TestCase):

    """
        Feature: Opening files with Python-style modes.
    """

    def test_default(self):
        """ Default semantics in the presence or absence of a file """
        fname = self.mktemp()

        # No existing file; create a new file and open RW
        with File(fname) as f:
            self.assertTrue(f)
            self.assertEqual(f.mode, 'r+')

        # Existing readonly file; open read-only
        os.chmod(fname, stat.S_IREAD)
        # Running as root (e.g. in a docker container) gives 'r+' as the file
        # mode, even for a read-only file.  See
        # https://github.com/h5py/h5py/issues/696
        exp_mode = 'r+' if os.stat(fname).st_uid == 0 and platform != "win32" else 'r'
        try:
            with File(fname) as f:
                self.assertTrue(f)
                self.assertEqual(f.mode, exp_mode)
        finally:
            os.chmod(fname, stat.S_IWRITE)

        # File exists but is not HDF5; raise IOError
        with open(fname, 'wb') as f:
            f.write(b'\x00')
        with self.assertRaises(IOError):
            File(fname)

    def test_create(self):
        """ Mode 'w' opens file in overwrite mode """
        fname = self.mktemp()
        fid = File(fname, 'w')
        self.assertTrue(fid)
        fid.create_group('foo')
        fid.close()
        fid = File(fname, 'w')
        self.assertNotIn('foo', fid)
        fid.close()

    def test_create_exclusive(self):
        """ Mode 'w-' opens file in exclusive mode """
        fname = self.mktemp()
        fid = File(fname, 'w-')
        self.assert_(fid)
        fid.close()
        with self.assertRaises(IOError):
            File(fname, 'w-')

    def test_append(self):
        """ Mode 'a' opens file in append/readwrite mode, creating if necessary """
        fname = self.mktemp()
        fid = File(fname, 'a')
        try:
            self.assert_(fid)
            fid.create_group('foo')
            self.assert_('foo' in fid)
        finally:
            fid.close()
        fid = File(fname, 'a')
        try:
            self.assert_('foo' in fid)
            fid.create_group('bar')
            self.assert_('bar' in fid)
        finally:
            fid.close()

    def test_readonly(self):
        """ Mode 'r' opens file in readonly mode """
        fname = self.mktemp()
        fid = File(fname, 'w')
        fid.close()
        self.assert_(not fid)
        fid = File(fname, 'r')
        self.assert_(fid)
        with self.assertRaises(ValueError):
            fid.create_group('foo')
        fid.close()

    def test_readwrite(self):
        """ Mode 'r+' opens existing file in readwrite mode """
        fname = self.mktemp()
        fid = File(fname, 'w')
        fid.create_group('foo')
        fid.close()
        fid = File(fname, 'r+')
        self.assert_('foo' in fid)
        fid.create_group('bar')
        self.assert_('bar' in fid)
        fid.close()

    def test_nonexistent_file(self):
        """ Modes 'r' and 'r+' do not create files """
        fname = self.mktemp()
        with self.assertRaises(IOError):
            File(fname, 'r')
        with self.assertRaises(IOError):
            File(fname, 'r+')

    def test_invalid_mode(self):
        """ Invalid modes raise ValueError """
        with self.assertRaises(ValueError):
            File(self.mktemp(), 'mongoose')

class TestModes(TestCase):

    """
        Feature: File mode can be retrieved via file.mode
    """

    def test_mode_attr(self):
        """ Mode equivalent can be retrieved via property """
        fname = self.mktemp()
        with File(fname, 'w') as f:
            self.assertEqual(f.mode, 'r+')
        with File(fname, 'r') as f:
            self.assertEqual(f.mode, 'r')

    def test_mode_external(self):
        """ Mode property works for files opened via external links

        Issue 190.
        """
        fname1 = self.mktemp()
        fname2 = self.mktemp()

        f1 = File(fname1,'w')
        f1.close()

        f2 = File(fname2,'w')
        try:
            f2['External'] = h5py.ExternalLink(fname1, '/')
            f3 = f2['External'].file
            self.assertEqual(f3.mode, 'r+')
        finally:
            f2.close()
            f3.close()

        f2 = File(fname2,'r')
        try:
            f3 = f2['External'].file
            self.assertEqual(f3.mode, 'r')
        finally:
            f2.close()
            f3.close()

class TestDrivers(TestCase):

    """
        Feature: Files can be opened with low-level HDF5 drivers
    """

    @ut.skipUnless(os.name == 'posix', "Stdio driver is supported on posix")
    def test_stdio(self):
        """ Stdio driver is supported on posix """
        fid = File(self.mktemp(), 'w', driver='stdio')
        self.assertTrue(fid)
        self.assertEqual(fid.driver, 'stdio')
        fid.close()

    @ut.skipUnless(os.name == 'posix', "Sec2 driver is supported on posix")
    def test_sec2(self):
        """ Sec2 driver is supported on posix """
        fid = File(self.mktemp(), 'w', driver='sec2')
        self.assert_(fid)
        self.assertEqual(fid.driver, 'sec2')
        fid.close()

    def test_core(self):
        """ Core driver is supported (no backing store) """
        fname = self.mktemp()
        fid = File(fname, 'w', driver='core', backing_store=False)
        self.assert_(fid)
        self.assertEqual(fid.driver, 'core')
        fid.close()
        self.assertFalse(os.path.exists(fname))

    def test_backing(self):
        """ Core driver saves to file when backing store used """
        fname = self.mktemp()
        fid = File(fname, 'w', driver='core', backing_store=True)
        fid.create_group('foo')
        fid.close()
        fid = File(fname, 'r')
        self.assert_('foo' in fid)
        fid.close()

    def test_readonly(self):
        """ Core driver can be used to open existing files """
        fname = self.mktemp()
        fid = File(fname, 'w')
        fid.create_group('foo')
        fid.close()
        fid = File(fname, 'r', driver='core')
        self.assert_(fid)
        self.assert_('foo' in fid)
        with self.assertRaises(ValueError):
            fid.create_group('bar')
        fid.close()

    def test_blocksize(self):
        """ Core driver supports variable block size """
        fname = self.mktemp()
        fid = File(fname, 'w', driver='core', block_size=1024,
                   backing_store=False)
        self.assert_(fid)
        fid.close()

    @ut.skipUnless(mpi, "Parallel HDF5 is required for MPIO driver test")
    def test_mpio(self):
        """ MPIO driver and options """
        from mpi4py import MPI

        fname = self.mktemp()
        with File(fname, 'w', driver='mpio', comm=MPI.COMM_WORLD) as f:
            self.assertTrue(f)
            self.assertEqual(f.driver, 'mpio')

    @ut.skipUnless(mpi, "Parallel HDF5 required")
    @ut.skipIf(h5py.version.hdf5_version_tuple < (1,8,9),
               "mpio atomic file operations were added in HDF5 1.8.9+")
    def test_mpi_atomic(self):
        """ Enable atomic mode for MPIO driver """
        from mpi4py import MPI

        fname = self.mktemp()
        with File(fname, 'w', driver='mpio', comm=MPI.COMM_WORLD) as f:
            self.assertFalse(f.atomic)
            f.atomic = True
            self.assertTrue(f.atomic)

    #TODO: family driver tests

class TestLibver(TestCase):

    """
        Feature: File format compatibility bounds can be specified when
        opening a file.
    """

    def test_default(self):
        """ Opening with no libver arg """
        f = File(self.mktemp(), 'w')
        self.assertEqual(f.libver, ('earliest','latest'))
        f.close()

    def test_single(self):
        """ Opening with single libver arg """
        f = File(self.mktemp(), 'w', libver='latest')
        self.assertEqual(f.libver, ('latest','latest'))
        f.close()

    def test_multiple(self):
        """ Opening with two libver args """
        f = File(self.mktemp(), 'w', libver=('earliest','latest'))
        self.assertEqual(f.libver, ('earliest', 'latest'))
        f.close()

    def test_none(self):
        """ Omitting libver arg results in maximum compatibility """
        f = File(self.mktemp(), 'w')
        self.assertEqual(f.libver, ('earliest', 'latest'))
        f.close()

class TestUserblock(TestCase):

    """
        Feature: Files can be create with user blocks
    """

    def test_create_blocksize(self):
        """ User blocks created with w, w-, x and properties work correctly """
        f = File(self.mktemp(),'w-', userblock_size=512)
        try:
            self.assertEqual(f.userblock_size, 512)
        finally:
            f.close()

        f = File(self.mktemp(),'x', userblock_size=512)
        try:
            self.assertEqual(f.userblock_size, 512)
        finally:
            f.close()

        f = File(self.mktemp(),'w', userblock_size=512)
        try:
            self.assertEqual(f.userblock_size, 512)
        finally:
            f.close()

    def test_write_only(self):
        """ User block only allowed for write """
        name = self.mktemp()
        f = File(name, 'w')
        f.close()

        with self.assertRaises(ValueError):
            f = h5py.File(name, 'r', userblock_size=512)

        with self.assertRaises(ValueError):
            f = h5py.File(name, 'r+', userblock_size=512)

    def test_match_existing(self):
        """ User block size must match that of file when opening for append """
        name = self.mktemp()
        f = File(name, 'w', userblock_size=512)
        f.close()

        with self.assertRaises(ValueError):
            f = File(name, 'a', userblock_size=1024)

        f = File(name, 'a', userblock_size=512)
        try:
            self.assertEqual(f.userblock_size, 512)
        finally:
            f.close()

    def test_power_of_two(self):
        """ User block size must be a power of 2 and at least 512 """
        name = self.mktemp()

        with self.assertRaises(ValueError):
            f = File(name, 'w', userblock_size=128)

        with self.assertRaises(ValueError):
            f = File(name, 'w', userblock_size=513)

        with self.assertRaises(ValueError):
            f = File(name, 'w', userblock_size=1023)

    def test_write_block(self):
        """ Test that writing to a user block does not destroy the file """
        name = self.mktemp()

        f = File(name, 'w', userblock_size=512)
        f.create_group("Foobar")
        f.close()

        pyfile = open(name, 'r+b')
        try:
            pyfile.write(b'X'*512)
        finally:
            pyfile.close()

        f = h5py.File(name, 'r')
        try:
            self.assert_("Foobar" in f)
        finally:
            f.close()

        pyfile = open(name, 'rb')
        try:
            self.assertEqual(pyfile.read(512), b'X'*512)
        finally:
            pyfile.close()

class TestContextManager(TestCase):

    """
        Feature: File objects can be used as context managers
    """

    def test_context_manager(self):
        """ File objects can be used in with statements """
        with File(self.mktemp(), 'w') as fid:
            self.assertTrue(fid)
        self.assertTrue(not fid)

@ut.skipIf(not UNICODE_FILENAMES, "Filesystem unicode support required")
class TestUnicode(TestCase):

    """
        Feature: Unicode filenames are supported
    """

    def test_unicode(self):
        """ Unicode filenames can be used, and retrieved properly via .filename
        """
        fname = self.mktemp(prefix = six.unichr(0x201a))
        fid = File(fname, 'w')
        try:
            self.assertEqual(fid.filename, fname)
            self.assertIsInstance(fid.filename, six.text_type)
        finally:
            fid.close()

    def test_unicode_hdf5_python_consistent(self):
        """ Unicode filenames can be used, and seen correctly from python
        """
        fname = self.mktemp(prefix = six.unichr(0x201a))
        with File(fname, 'w') as f:
            self.assertTrue(os.path.exists(fname))

    def test_nonexistent_file_unicode(self):
        """
        Modes 'r' and 'r+' do not create files even when given unicode names
        """
        fname = self.mktemp(prefix = six.unichr(0x201a))
        with self.assertRaises(IOError):
            File(fname, 'r')
        with self.assertRaises(IOError):
            File(fname, 'r+')


class TestFileProperty(TestCase):

    """
        Feature: A File object can be retrieved from any child object,
        via the .file property
    """

    def test_property(self):
        """ File object can be retrieved from subgroup """
        fname = self.mktemp()
        hfile = File(fname, 'w')
        try:
            hfile2 = hfile['/'].file
            self.assertEqual(hfile, hfile2)
        finally:
            hfile.close()

    def test_close(self):
        """ All retrieved File objects are closed at the same time """
        fname = self.mktemp()
        hfile = File(fname, 'w')
        grp = hfile.create_group('foo')
        hfile2 = grp.file
        hfile3 = hfile['/'].file
        hfile2.close()
        self.assertFalse(hfile)
        self.assertFalse(hfile2)
        self.assertFalse(hfile3)

    def test_mode(self):
        """ Retrieved File objects have a meaningful mode attribute """
        hfile = File(self.mktemp(),'w')
        try:
            grp = hfile.create_group('foo')
            self.assertEqual(grp.file.mode, hfile.mode)
        finally:
            hfile.close()

class TestClose(TestCase):

    """
        Feature: Files can be closed
    """

    def test_close(self):
        """ Close file via .close method """
        fid = File(self.mktemp())
        self.assert_(fid)
        fid.close()
        self.assert_(not fid)

    def test_closed_file(self):
        """ Trying to modify closed file raises ValueError """
        fid = File(self.mktemp(), 'w')
        fid.close()
        with self.assertRaises(ValueError):
            fid.create_group('foo')

    def test_close_multiple_default_driver(self):
        fname = self.mktemp()
        f = h5py.File(fname, 'w')
        f.create_group("test")
        f.close()
        f.close()

    @ut.skipUnless(mpi, "Parallel HDF5 is required for MPIO driver test")
    def test_close_multiple_mpio_driver(self):
        """ MPIO driver and options """
        from mpi4py import MPI

        fname = self.mktemp()
        f = File(fname, 'w', driver='mpio', comm=MPI.COMM_WORLD)
        f.create_group("test")
        f.close()
        f.close()

class TestFlush(TestCase):

    """
        Feature: Files can be flushed
    """

    def test_flush(self):
        """ Flush via .flush method """
        fid = File(self.mktemp(), 'w')
        fid.flush()
        fid.close()


class TestRepr(TestCase):

    """
        Feature: File objects provide a helpful __repr__ string
    """

    def test_repr(self):
        """ __repr__ behaves itself when files are open and closed """
        fid = File(self.mktemp())
        self.assertIsInstance(repr(fid), six.string_types)
        fid.close()
        self.assertIsInstance(repr(fid), six.string_types)

class TestFilename(TestCase):

    """
        Feature: The name of a File object can be retrieved via .filename
    """

    def test_filename(self):
        """ .filename behaves properly for string data """
        fname = self.mktemp()
        fid = File(fname, 'w')
        try:
            self.assertEqual(fid.filename, fname)
            self.assertIsInstance(fid.filename, six.text_type)
        finally:
            fid.close()

class TestBackwardsCompat(TestCase):

    """
        Feature: Deprecated attributes are included to support 1.3 code
    """

    def test_fid(self):
        """ File objects provide a .fid attribute aliased to the file ID """
        with File(self.mktemp(), 'w') as hfile:
            self.assertIs(hfile.fid, hfile.id)


class TestCloseInvalidatesOpenObjectIDs(TestCase):

    """
        Ensure that closing a file invalidates object IDs, as appropriate
    """

    def test_close(self):
        """ Closing a file invalidates any of the file's open objects """
        with File(self.mktemp(), 'w') as f1:
            g1 = f1.create_group('foo')
            self.assertTrue(bool(f1.id))
            self.assertTrue(bool(g1.id))
            f1.close()
            self.assertFalse(bool(f1.id))
            self.assertFalse(bool(g1.id))
        with File(self.mktemp(), 'w') as f2:
            g2 = f2.create_group('foo')
            self.assertTrue(bool(f2.id))
            self.assertTrue(bool(g2.id))
            self.assertFalse(bool(f1.id))
            self.assertFalse(bool(g1.id))

@ut.skipIf(pathlib is None, "pathlib module not installed")
class TestPathlibSupport(TestCase):

    """
        Check that h5py doesn't break on pathlib
    """
    def test_pathlib_accepted_file(self):
        """ Check that pathlib is accepted by h5py.File """
        with closed_tempfile() as f:
            path = pathlib.Path(f)
            with File(path) as f2:
                self.assertTrue(True)

    def test_pathlib_name_match(self):
        """ Check that using pathlib does not affect naming """
        with closed_tempfile() as f:
            path = pathlib.Path(f)
            with File(path) as h5f1:
                pathlib_name = h5f1.filename
            with File(f) as h5f2:
                normal_name = h5f2.filename
            self.assertEqual(pathlib_name, normal_name)
