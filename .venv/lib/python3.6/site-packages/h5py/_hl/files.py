# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

"""
    Implements high-level support for HDF5 file objects.
"""

from __future__ import absolute_import

import sys
import os
from warnings import warn

from .compat import filename_decode, filename_encode

import six

from .base import phil, with_phil
from .group import Group
from .. import h5, h5f, h5p, h5i, h5fd, _objects
from .. import version
from ..h5py_warnings import H5pyDeprecationWarning

mpi = h5.get_config().mpi
hdf5_version = version.hdf5_version_tuple[0:3]

swmr_support = False
if hdf5_version >= h5.get_config().swmr_min_hdf5_version:
    swmr_support = True


libver_dict = {'earliest': h5f.LIBVER_EARLIEST, 'latest': h5f.LIBVER_LATEST}
libver_dict_r = dict((y, x) for x, y in six.iteritems(libver_dict))


def _set_fapl_mpio(plist, **kwargs):
    import mpi4py
    kwargs.setdefault('info', mpi4py.MPI.Info())
    plist.set_fapl_mpio(**kwargs)


def _set_fapl_fileobj(plist, **kwargs):
    plist.set_fileobj_driver(h5fd.fileobj_driver, kwargs.get('fileobj'))


_drivers = {
    'sec2': lambda plist, **kwargs: plist.set_fapl_sec2(**kwargs),
    'stdio': lambda plist, **kwargs: plist.set_fapl_stdio(**kwargs),
    'core': lambda plist, **kwargs: plist.set_fapl_core(**kwargs),
    'family': lambda plist, **kwargs: plist.set_fapl_family(
        memb_fapl=plist.copy(),
        **kwargs
    ),
    'mpio': _set_fapl_mpio,
    'fileobj': _set_fapl_fileobj,
}


def register_driver(name, set_fapl):
    """Register a custom driver.

    Parameters
    ----------
    name : str
        The name of the driver.
    set_fapl : callable[PropFAID, **kwargs] -> NoneType
        The function to set the fapl to use your custom driver.
    """
    _drivers[name] = set_fapl


def unregister_driver(name):
    """Unregister a custom driver.

    Parameters
    ----------
    name : str
        The name of the driver.
    """
    del _drivers[name]


def registered_drivers():
    """Return a frozenset of the names of all of the registered drivers.
    """
    return frozenset(_drivers)


def make_fapl(driver, libver, rdcc_nslots, rdcc_nbytes, rdcc_w0, **kwds):
    """ Set up a file access property list """
    plist = h5p.create(h5p.FILE_ACCESS)

    if libver is not None:
        if libver in libver_dict:
            low = libver_dict[libver]
            high = h5f.LIBVER_LATEST
        else:
            low, high = (libver_dict[x] for x in libver)
    else:
        # we default to earliest
        low, high = h5f.LIBVER_EARLIEST, h5f.LIBVER_LATEST
    plist.set_libver_bounds(low, high)

    cache_settings = list(plist.get_cache())
    if rdcc_nslots is not None:
        cache_settings[1] = rdcc_nslots
    if rdcc_nbytes is not None:
        cache_settings[2] = rdcc_nbytes
    if rdcc_w0 is not None:
        cache_settings[3] = rdcc_w0
    plist.set_cache(*cache_settings)

    if driver is None or (driver == 'windows' and sys.platform == 'win32'):
        # Prevent swallowing unused key arguments
        if kwds:
            msg = "'{key}' is an invalid keyword argument for this function" \
                  .format(key=next(iter(kwds)))
            raise TypeError(msg)
        return plist

    try:
        set_fapl = _drivers[driver]
    except KeyError:
        raise ValueError('Unknown driver type "%s"' % driver)
    else:
        set_fapl(plist, **kwds)

    return plist


def make_fcpl(track_order=False):
    """ Set up a file creation property list """
    if track_order:
        plist = h5p.create(h5p.FILE_CREATE)
        plist.set_link_creation_order(
            h5p.CRT_ORDER_TRACKED | h5p.CRT_ORDER_INDEXED)
        plist.set_attr_creation_order(
            h5p.CRT_ORDER_TRACKED | h5p.CRT_ORDER_INDEXED)
    else:
        plist = None
    return plist


def make_fid(name, mode, userblock_size, fapl, fcpl=None, swmr=False):
    """ Get a new FileID by opening or creating a file.
    Also validates mode argument."""

    if userblock_size is not None:
        if mode in ('r', 'r+'):
            raise ValueError("User block may only be specified "
                             "when creating a file")
        try:
            userblock_size = int(userblock_size)
        except (TypeError, ValueError):
            raise ValueError("User block size must be an integer")
        if fcpl is None:
            fcpl = h5p.create(h5p.FILE_CREATE)
        fcpl.set_userblock(userblock_size)

    if mode == 'r':
        flags = h5f.ACC_RDONLY
        if swmr and swmr_support:
            flags |= h5f.ACC_SWMR_READ
        fid = h5f.open(name, flags, fapl=fapl)
    elif mode == 'r+':
        fid = h5f.open(name, h5f.ACC_RDWR, fapl=fapl)
    elif mode in ['w-', 'x']:
        fid = h5f.create(name, h5f.ACC_EXCL, fapl=fapl, fcpl=fcpl)
    elif mode == 'w':
        fid = h5f.create(name, h5f.ACC_TRUNC, fapl=fapl, fcpl=fcpl)
    elif mode == 'a':
        # Open in append mode (read/write).
        # If that fails, create a new file only if it won't clobber an
        # existing one (ACC_EXCL)
        try:
            fid = h5f.open(name, h5f.ACC_RDWR, fapl=fapl)
        except IOError:
            fid = h5f.create(name, h5f.ACC_EXCL, fapl=fapl, fcpl=fcpl)
    elif mode is None:
        # Try to open in append mode (read/write).
        # If that fails, try readonly, and finally create a new file only
        # if it won't clobber an existing file (ACC_EXCL).
        try:
            fid = h5f.open(name, h5f.ACC_RDWR, fapl=fapl)
        except IOError:
            try:
                fid = h5f.open(name, h5f.ACC_RDONLY, fapl=fapl)
            except IOError:
                fid = h5f.create(name, h5f.ACC_EXCL, fapl=fapl, fcpl=fcpl)
    else:
        raise ValueError("Invalid mode; must be one of r, r+, w, w-, x, a")

    try:
        if userblock_size is not None:
            existing_fcpl = fid.get_create_plist()
            if existing_fcpl.get_userblock() != userblock_size:
                raise ValueError("Requested userblock size (%d) does not match that of existing file (%d)" % (userblock_size, existing_fcpl.get_userblock()))
    except:
        fid.close()
        raise

    return fid


class File(Group):

    """
        Represents an HDF5 file.
    """

    @property
    def attrs(self):
        """ Attributes attached to this object """
        # hdf5 complains that a file identifier is an invalid location for an
        # attribute. Instead of self, pass the root group to AttributeManager:
        from . import attrs
        with phil:
            return attrs.AttributeManager(self['/'])

    @property
    @with_phil
    def filename(self):
        """File name on disk"""
        return filename_decode(h5f.get_name(self.id))

    @property
    @with_phil
    def driver(self):
        """Low-level HDF5 file driver used to open file"""
        drivers = {h5fd.SEC2: 'sec2',
                   h5fd.STDIO: 'stdio',
                   h5fd.CORE: 'core',
                   h5fd.FAMILY: 'family',
                   h5fd.WINDOWS: 'windows',
                   h5fd.MPIO: 'mpio',
                   h5fd.MPIPOSIX: 'mpiposix',
                   h5fd.fileobj_driver: 'fileobj'}
        return drivers.get(self.id.get_access_plist().get_driver(), 'unknown')

    @property
    @with_phil
    def mode(self):
        """ Python mode used to open file """
        return {h5f.ACC_RDONLY: 'r',
                h5f.ACC_RDWR: 'r+'}.get(self.id.get_intent())

    @property
    @with_phil
    def fid(self):
        """File ID (backwards compatibility) """
        warn("File.fid has been deprecated. "
            "Use File.id instead.", H5pyDeprecationWarning)
        return self.id

    @property
    @with_phil
    def libver(self):
        """File format version bounds (2-tuple: low, high)"""
        bounds = self.id.get_access_plist().get_libver_bounds()
        return tuple(libver_dict_r[x] for x in bounds)

    @property
    @with_phil
    def userblock_size(self):
        """ User block size (in bytes) """
        fcpl = self.id.get_create_plist()
        return fcpl.get_userblock()


    if mpi and hdf5_version >= (1, 8, 9):

        @property
        @with_phil
        def atomic(self):
            """ Set/get MPI-IO atomic mode
            """
            return self.id.get_mpi_atomicity()

        @atomic.setter
        @with_phil
        def atomic(self, value):
            # pylint: disable=missing-docstring
            self.id.set_mpi_atomicity(value)

    if swmr_support:
        @property
        def swmr_mode(self):
            """ Controls single-writer multiple-reader mode """
            return self._swmr_mode

        @swmr_mode.setter
        @with_phil
        def swmr_mode(self, value):
            # pylint: disable=missing-docstring
            if value:
                self.id.start_swmr_write()
                self._swmr_mode = True
            else:
                raise ValueError("It is not possible to forcibly switch SWMR mode off.")

    def __init__(self, name, mode=None, driver=None,
                 libver=None, userblock_size=None, swmr=False,
                 rdcc_nslots=None, rdcc_nbytes=None, rdcc_w0=None,
                 track_order=None,
                 **kwds):
        """Create a new file object.

        See the h5py user guide for a detailed explanation of the options.

        name
            Name of the file on disk, or file-like object.  Note: for files
            created with the 'core' driver, HDF5 still requires this be
            non-empty.
        mode
            r        Readonly, file must exist
            r+       Read/write, file must exist
            w        Create file, truncate if exists
            w- or x  Create file, fail if exists
            a        Read/write if exists, create otherwise (default)
        driver
            Name of the driver to use.  Legal values are None (default,
            recommended), 'core', 'sec2', 'stdio', 'mpio'.
        libver
            Library version bounds.  Currently only the strings 'earliest'
            and 'latest' are defined.
        userblock
            Desired size of user block.  Only allowed when creating a new
            file (mode w, w- or x).
        swmr
            Open the file in SWMR read mode. Only used when mode = 'r'.
        rdcc_nbytes
            Total size of the raw data chunk cache in bytes. The default size
            is 1024**2 (1 MB) per dataset.
        rdcc_w0
            The chunk preemption policy for all datasets.  This must be
            between 0 and 1 inclusive and indicates the weighting according to
            which chunks which have been fully read or written are penalized
            when determining which chunks to flush from cache.  A value of 0
            means fully read or written chunks are treated no differently than
            other chunks (the preemption is strictly LRU) while a value of 1
            means fully read or written chunks are always preempted before
            other chunks.  If your application only reads or writes data once,
            this can be safely set to 1.  Otherwise, this should be set lower
            depending on how often you re-read or re-write the same data.  The
            default value is 0.75.
        rdcc_nslots
            The number of chunk slots in the raw data chunk cache for this
            file. Increasing this value reduces the number of cache collisions,
            but slightly increases the memory used. Due to the hashing
            strategy, this value should ideally be a prime number. As a rule of
            thumb, this value should be at least 10 times the number of chunks
            that can fit in rdcc_nbytes bytes. For maximum performance, this
            value should be set approximately 100 times that number of
            chunks. The default value is 521.
        track_order
            Track dataset/group/attribute creation order under root group
            if True. If None use global default h5.get_config().track_order.
        Additional keywords
            Passed on to the selected file driver.

        """
        if swmr and not swmr_support:
            raise ValueError("The SWMR feature is not available in this version of the HDF5 library")

        if isinstance(name, _objects.ObjectID):
            with phil:
                fid = h5i.get_file_id(name)
        else:
            if hasattr(name, 'read') and hasattr(name, 'seek'):
                if driver not in (None, 'fileobj'):
                    raise ValueError("Driver must be 'fileobj' for file-like object if specified.")
                driver = 'fileobj'
                if kwds.get('fileobj', name) != name:
                    raise ValueError("Invalid value of 'fileobj' argument; "
                                     "must equal to file-like object if specified.")
                kwds.update(fileobj=name)
                name = repr(name).encode('ASCII', 'replace')
            else:
                name = filename_encode(name)

            if track_order is None:
                track_order = h5.get_config().track_order

            with phil:
                fapl = make_fapl(driver, libver, rdcc_nslots, rdcc_nbytes, rdcc_w0, **kwds)
                fid = make_fid(name, mode, userblock_size,
                               fapl, fcpl=make_fcpl(track_order=track_order),
                               swmr=swmr)

            if swmr_support:
                self._swmr_mode = False
                if swmr and mode == 'r':
                    self._swmr_mode = True

        Group.__init__(self, fid)

    def close(self):
        """ Close the file.  All open objects become invalid """
        with phil:
            # Check that the file is still open, otherwise skip
            if self.id.valid:
                # We have to explicitly murder all open objects related to the file

                # Close file-resident objects first, then the files.
                # Otherwise we get errors in MPI mode.
                id_list = h5f.get_obj_ids(self.id, ~h5f.OBJ_FILE)
                file_list = h5f.get_obj_ids(self.id, h5f.OBJ_FILE)

                id_list = [x for x in id_list if h5i.get_file_id(x).id == self.id.id]
                file_list = [x for x in file_list if h5i.get_file_id(x).id == self.id.id]

                for id_ in id_list:
                    while id_.valid:
                        h5i.dec_ref(id_)

                for id_ in file_list:
                    while id_.valid:
                        h5i.dec_ref(id_)

                self.id.close()
                _objects.nonlocal_close()

    def flush(self):
        """ Tell the HDF5 library to flush its buffers.
        """
        with phil:
            h5f.flush(self.id)

    @with_phil
    def __enter__(self):
        return self

    @with_phil
    def __exit__(self, *args):
        if self.id:
            self.close()

    @with_phil
    def __repr__(self):
        if not self.id:
            r = u'<Closed HDF5 file>'
        else:
            # Filename has to be forced to Unicode if it comes back bytes
            # Mode is always a "native" string
            filename = self.filename
            if isinstance(filename, bytes):  # Can't decode fname
                filename = filename.decode('utf8', 'replace')
            r = u'<HDF5 file "%s" (mode %s)>' % (os.path.basename(filename),
                                                 self.mode)

        if six.PY2:
            return r.encode('utf8')
        return r
