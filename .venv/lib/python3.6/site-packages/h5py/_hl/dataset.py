# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

"""
    Implements support for high-level dataset access.
"""

from __future__ import absolute_import

import posixpath as pp
import sys
from warnings import warn

from threading import local

import six
from six.moves import xrange    # pylint: disable=redefined-builtin

import numpy

from .. import h5, h5s, h5t, h5r, h5d, h5p, h5fd
from .base import HLObject, phil, with_phil, Empty, is_empty_dataspace
from . import filters
from . import selections as sel
from . import selections2 as sel2
from .datatype import Datatype
from .compat import filename_decode
from .vds import VDSmap, vds_support
from ..h5py_warnings import H5pyDeprecationWarning

_LEGACY_GZIP_COMPRESSION_VALS = frozenset(range(10))
MPI = h5.get_config().mpi


def readtime_dtype(basetype, names):
    """ Make a NumPy dtype appropriate for reading """

    if len(names) == 0:  # Not compound, or we want all fields
        return basetype

    if basetype.names is None:  # Names provided, but not compound
        raise ValueError("Field names only allowed for compound types")

    for name in names:  # Check all names are legal
        if not name in basetype.names:
            raise ValueError("Field %s does not appear in this type." % name)

    return numpy.dtype([(name, basetype.fields[name][0]) for name in names])


def make_new_dset(parent, shape=None, dtype=None, data=None,
                  chunks=None, compression=None, shuffle=None,
                  fletcher32=None, maxshape=None, compression_opts=None,
                  fillvalue=None, scaleoffset=None, track_times=None,
                  external=None, track_order=None, dcpl=None):
    """ Return a new low-level dataset identifier

    Only creates anonymous datasets.
    """

    # Convert data to a C-contiguous ndarray
    if data is not None and not isinstance(data, Empty):
        from . import base
        # normalize strings -> np.dtype objects
        if dtype is not None:
            _dtype = numpy.dtype(dtype)
        else:
            _dtype = None

        # if we are going to a f2 datatype, pre-convert in python
        # to workaround a possible h5py bug in the conversion.
        is_small_float = (_dtype is not None and
                          _dtype.kind == 'f' and
                          _dtype.itemsize == 2)
        data = numpy.asarray(data, order="C",
                             dtype=(_dtype if is_small_float
                                    else base.guess_dtype(data)))

    # Validate shape
    if shape is None:
        if data is None:
            if dtype is None:
                raise TypeError("One of data, shape or dtype must be specified")
            data = Empty(dtype)
        shape = data.shape
    else:
        shape = tuple(shape)
        if data is not None and (numpy.product(shape, dtype=numpy.ulonglong) != numpy.product(data.shape, dtype=numpy.ulonglong)):
            raise ValueError("Shape tuple is incompatible with data")

    tmp_shape = maxshape if maxshape is not None else shape
    # Validate chunk shape
    if isinstance(chunks, tuple) and any(
        chunk > dim for dim, chunk in zip(tmp_shape, chunks) if dim is not None
    ):
        errmsg = "Chunk shape must not be greater than data shape in any dimension. "\
                 "{} is not compatible with {}".format(chunks, shape)
        raise ValueError(errmsg)

    if isinstance(dtype, Datatype):
        # Named types are used as-is
        tid = dtype.id
        dtype = tid.dtype  # Following code needs this
    else:
        # Validate dtype
        if dtype is None and data is None:
            dtype = numpy.dtype("=f4")
        elif dtype is None and data is not None:
            dtype = data.dtype
        else:
            dtype = numpy.dtype(dtype)
        tid = h5t.py_create(dtype, logical=1)

    # Legacy
    if any((compression, shuffle, fletcher32, maxshape, scaleoffset)) and chunks is False:
        raise ValueError("Chunked format required for given storage options")

    # Legacy
    if compression is True:
        if compression_opts is None:
            compression_opts = 4
        compression = 'gzip'

    # Legacy
    if compression in _LEGACY_GZIP_COMPRESSION_VALS:
        if compression_opts is not None:
            raise TypeError("Conflict in compression options")
        compression_opts = compression
        compression = 'gzip'

    dcpl = filters.fill_dcpl(
        dcpl or h5p.create(h5p.DATASET_CREATE), shape, dtype,
        chunks, compression, compression_opts, shuffle, fletcher32,
        maxshape, scaleoffset, external)

    if fillvalue is not None:
        fillvalue = numpy.array(fillvalue)
        dcpl.set_fill_value(fillvalue)

    if track_times in (True, False):
        dcpl.set_obj_track_times(track_times)
    elif track_times is not None:
        raise TypeError("track_times must be either True or False")
    if track_order == True:
        dcpl.set_attr_creation_order(
            h5p.CRT_ORDER_TRACKED | h5p.CRT_ORDER_INDEXED)
    elif track_order == False:
        dcpl.set_attr_creation_order(0)
    elif track_order is not None:
        raise TypeError("track_order must be either True or False")

    if maxshape is not None:
        maxshape = tuple(m if m is not None else h5s.UNLIMITED for m in maxshape)

    if isinstance(data, Empty):
        sid = h5s.create(h5s.NULL)
    else:
        sid = h5s.create_simple(shape, maxshape)


    dset_id = h5d.create(parent.id, None, tid, sid, dcpl=dcpl)

    if (data is not None) and (not isinstance(data, Empty)):
        dset_id.write(h5s.ALL, h5s.ALL, data)

    return dset_id


def make_new_virtual_dset(parent, shape, sources, dtype=None,
                          maxshape=None, fillvalue=None):
    """Return a new low-level dataset identifier for a virtual dataset

    Like make_new_dset(), this creates an anonymous dataset, which can be given
    a name later.
    """
    # create the creation property list
    dcpl = h5p.create(h5p.DATASET_CREATE)
    if fillvalue is not None:
        dcpl.set_fill_value(numpy.array([fillvalue]))

    if maxshape is not None:
        maxshape = tuple(m if m is not None else h5s.UNLIMITED for m in maxshape)

    virt_dspace = h5s.create_simple(shape, maxshape)

    for vspace, fpath, dset, src_dspace in sources:
        dcpl.set_virtual(vspace, fpath, dset, src_dspace)

    dtype = dtype
    if isinstance(dtype, Datatype):
        # Named types are used as-is
        tid = dtype.id
    else:
        if dtype is None:
            dtype = numpy.dtype("=f4")
        else:
            dtype = numpy.dtype(dtype)
        tid = h5t.py_create(dtype, logical=1)

    return h5d.create(parent.id, name=None, tid=tid, space=virt_dspace,
                      dcpl=dcpl)


class AstypeContext(object):

    """
        Context manager which allows changing the type read from a dataset.
    """

    def __init__(self, dset, dtype):
        self._dset = dset
        self._dtype = numpy.dtype(dtype)

    def __enter__(self):
        # pylint: disable=protected-access
        self._dset._local.astype = self._dtype

    def __exit__(self, *args):
        # pylint: disable=protected-access
        self._dset._local.astype = None


if MPI:
    class CollectiveContext(object):

        """ Manages collective I/O in MPI mode """

        # We don't bother with _local as threads are forbidden in MPI mode

        def __init__(self, dset):
            self._dset = dset

        def __enter__(self):
            # pylint: disable=protected-access
            self._dset._dxpl.set_dxpl_mpio(h5fd.MPIO_COLLECTIVE)

        def __exit__(self, *args):
            # pylint: disable=protected-access
            self._dset._dxpl.set_dxpl_mpio(h5fd.MPIO_INDEPENDENT)


class Dataset(HLObject):

    """
        Represents an HDF5 dataset
    """

    def astype(self, dtype):
        """ Get a context manager allowing you to perform reads to a
        different destination type, e.g.:

        >>> with dataset.astype('f8'):
        ...     double_precision = dataset[0:100:2]
        """
        return AstypeContext(self, dtype)

    if MPI:
        @property
        @with_phil
        def collective(self):
            """ Context manager for MPI collective reads & writes """
            return CollectiveContext(self)

    @property
    def dims(self):
        """ Access dimension scales attached to this dataset. """
        from .dims import DimensionManager
        with phil:
            return DimensionManager(self)

    @property
    @with_phil
    def ndim(self):
        """Numpy-style attribute giving the number of dimensions"""
        return self.id.rank

    @property
    @with_phil
    def shape(self):
        """Numpy-style shape tuple giving dataset dimensions"""
        return self.id.shape
    @shape.setter
    @with_phil
    def shape(self, shape):
        # pylint: disable=missing-docstring
        self.resize(shape)

    @property
    @with_phil
    def size(self):
        """Numpy-style attribute giving the total dataset size"""
        if is_empty_dataspace(self.id):
            return None
        return numpy.prod(self.shape, dtype=numpy.intp)

    @property
    @with_phil
    def dtype(self):
        """Numpy dtype representing the datatype"""
        return self.id.dtype

    @property
    @with_phil
    def value(self):
        """  Alias for dataset[()] """
        warn("dataset.value has been deprecated. "
            "Use dataset[()] instead.", H5pyDeprecationWarning)
        return self[()]

    @property
    @with_phil
    def chunks(self):
        """Dataset chunks (or None)"""
        dcpl = self._dcpl
        if dcpl.get_layout() == h5d.CHUNKED:
            return dcpl.get_chunk()
        return None

    @property
    @with_phil
    def compression(self):
        """Compression strategy (or None)"""
        for x in ('gzip','lzf','szip'):
            if x in self._filters:
                return x
        return None

    @property
    @with_phil
    def compression_opts(self):
        """ Compression setting.  Int(0-9) for gzip, 2-tuple for szip. """
        return self._filters.get(self.compression, None)

    @property
    @with_phil
    def shuffle(self):
        """Shuffle filter present (T/F)"""
        return 'shuffle' in self._filters

    @property
    @with_phil
    def fletcher32(self):
        """Fletcher32 filter is present (T/F)"""
        return 'fletcher32' in self._filters

    @property
    @with_phil
    def scaleoffset(self):
        """Scale/offset filter settings. For integer data types, this is
        the number of bits stored, or 0 for auto-detected. For floating
        point data types, this is the number of decimal places retained.
        If the scale/offset filter is not in use, this is None."""
        try:
            return self._filters['scaleoffset'][1]
        except KeyError:
            return None

    @property
    def external(self):
        """External file settings. Returns a list of tuples of
        (name, offset, size) for each external file entry, or returns None
        if no external files are used."""
        count = self._dcpl.get_external_count()
        if count<=0:
            return None
        ext_list = list()
        for x in xrange(count):
            (name, offset, size) = self._dcpl.get_external(x)
            ext_list.append( (filename_decode(name), offset, size) )
        return ext_list

    @property
    @with_phil
    def maxshape(self):
        """Shape up to which this dataset can be resized.  Axes with value
        None have no resize limit. """
        space = self.id.get_space()
        dims = space.get_simple_extent_dims(True)
        return tuple(x if x != h5s.UNLIMITED else None for x in dims)

    @property
    @with_phil
    def fillvalue(self):
        """Fill value for this dataset (0 by default)"""
        arr = numpy.ndarray((1,), dtype=self.dtype)
        self._dcpl.get_fill_value(arr)
        return arr[0]

    @with_phil
    def __init__(self, bind):
        """ Create a new Dataset object by binding to a low-level DatasetID.
        """
        if not isinstance(bind, h5d.DatasetID):
            raise ValueError("%s is not a DatasetID" % bind)
        HLObject.__init__(self, bind)

        self._dcpl = self.id.get_create_plist()
        self._dxpl = h5p.create(h5p.DATASET_XFER)
        self._filters = filters.get_filters(self._dcpl)
        self._local = local()
        self._local.astype = None

    def resize(self, size, axis=None):
        """ Resize the dataset, or the specified axis.

        The dataset must be stored in chunked format; it can be resized up to
        the "maximum shape" (keyword maxshape) specified at creation time.
        The rank of the dataset cannot be changed.

        "Size" should be a shape tuple, or if an axis is specified, an integer.

        BEWARE: This functions differently than the NumPy resize() method!
        The data is not "reshuffled" to fit in the new shape; each axis is
        grown or shrunk independently.  The coordinates of existing data are
        fixed.
        """
        with phil:
            if self.chunks is None:
                raise TypeError("Only chunked datasets can be resized")

            if axis is not None:
                if not (axis >=0 and axis < self.id.rank):
                    raise ValueError("Invalid axis (0 to %s allowed)" % (self.id.rank-1))
                try:
                    newlen = int(size)
                except TypeError:
                    raise TypeError("Argument must be a single int if axis is specified")
                size = list(self.shape)
                size[axis] = newlen

            size = tuple(size)
            self.id.set_extent(size)
            #h5f.flush(self.id)  # THG recommends

    @with_phil
    def __len__(self):
        """ The size of the first axis.  TypeError if scalar.

        Limited to 2**32 on 32-bit systems; Dataset.len() is preferred.
        """
        size = self.len()
        if size > sys.maxsize:
            raise OverflowError("Value too big for Python's __len__; use Dataset.len() instead.")
        return size

    def len(self):
        """ The size of the first axis.  TypeError if scalar.

        Use of this method is preferred to len(dset), as Python's built-in
        len() cannot handle values greater then 2**32 on 32-bit systems.
        """
        with phil:
            shape = self.shape
            if len(shape) == 0:
                raise TypeError("Attempt to take len() of scalar dataset")
            return shape[0]

    @with_phil
    def __iter__(self):
        """ Iterate over the first axis.  TypeError if scalar.

        BEWARE: Modifications to the yielded data are *NOT* written to file.
        """
        shape = self.shape
        if len(shape) == 0:
            raise TypeError("Can't iterate over a scalar dataset")
        for i in xrange(shape[0]):
            yield self[i]

    @with_phil
    def __getitem__(self, args):
        """ Read a slice from the HDF5 dataset.

        Takes slices and recarray-style field names (more than one is
        allowed!) in any order.  Obeys basic NumPy rules, including
        broadcasting.

        Also supports:

        * Boolean "mask" array indexing
        """
        args = args if isinstance(args, tuple) else (args,)
        if is_empty_dataspace(self.id):
            if not (args == tuple() or args == (Ellipsis,)):
                raise ValueError("Empty datasets cannot be sliced")
            return Empty(self.dtype)

        # Sort field indices from the rest of the args.
        names = tuple(x for x in args if isinstance(x, six.string_types))
        args = tuple(x for x in args if not isinstance(x, six.string_types))
        if six.PY2:
            names = tuple(x.encode('utf-8') if isinstance(x, six.text_type) else x for x in names)

        new_dtype = getattr(self._local, 'astype', None)
        if new_dtype is not None:
            new_dtype = readtime_dtype(new_dtype, names)
        else:
            # This is necessary because in the case of array types, NumPy
            # discards the array information at the top level.
            new_dtype = readtime_dtype(self.id.dtype, names)
        mtype = h5t.py_create(new_dtype)

        # === Special-case region references ====

        if len(args) == 1 and isinstance(args[0], h5r.RegionReference):

            obj = h5r.dereference(args[0], self.id)
            if obj != self.id:
                raise ValueError("Region reference must point to this dataset")

            sid = h5r.get_region(args[0], self.id)
            mshape = sel.guess_shape(sid)
            if mshape is None:
                return numpy.array((0,), dtype=new_dtype)
            if numpy.product(mshape, dtype=numpy.ulonglong) == 0:
                return numpy.array(mshape, dtype=new_dtype)
            out = numpy.empty(mshape, dtype=new_dtype)
            sid_out = h5s.create_simple(mshape)
            sid_out.select_all()
            self.id.read(sid_out, sid, out, mtype)
            return out

        # === Check for zero-sized datasets =====

        if numpy.product(self.shape, dtype=numpy.ulonglong) == 0:
            # These are the only access methods NumPy allows for such objects
            if args == (Ellipsis,) or args == tuple():
                return numpy.empty(self.shape, dtype=new_dtype)

        # === Scalar dataspaces =================

        if self.shape == ():
            fspace = self.id.get_space()
            selection = sel2.select_read(fspace, args)
            arr = numpy.ndarray(selection.mshape, dtype=new_dtype)
            for mspace, fspace in selection:
                self.id.read(mspace, fspace, arr, mtype)
            if len(names) == 1:
                arr = arr[names[0]]
            if selection.mshape is None:
                return arr[()]
            return arr

        # === Everything else ===================

        # Perform the dataspace selection.
        selection = sel.select(self.shape, args, dsid=self.id)

        if selection.nselect == 0:
            return numpy.ndarray(selection.mshape, dtype=new_dtype)

        # Up-converting to (1,) so that numpy.ndarray correctly creates
        # np.void rows in case of multi-field dtype. (issue 135)
        single_element = selection.mshape == ()
        mshape = (1,) if single_element else selection.mshape
        arr = numpy.ndarray(mshape, new_dtype, order='C')

        # HDF5 has a bug where if the memory shape has a different rank
        # than the dataset, the read is very slow
        if len(mshape) < len(self.shape):
            # pad with ones
            mshape = (1,)*(len(self.shape)-len(mshape)) + mshape

        # Perform the actual read
        mspace = h5s.create_simple(mshape)
        fspace = selection.id
        self.id.read(mspace, fspace, arr, mtype, dxpl=self._dxpl)

        # Patch up the output for NumPy
        if len(names) == 1:
            arr = arr[names[0]]     # Single-field recarray convention
        if arr.shape == ():
            arr = arr.item()
        if single_element:
            arr = arr[0]
        return arr

    @with_phil
    def __setitem__(self, args, val):
        """ Write to the HDF5 dataset from a Numpy array.

        NumPy's broadcasting rules are honored, for "simple" indexing
        (slices and integers).  For advanced indexing, the shapes must
        match.
        """
        args = args if isinstance(args, tuple) else (args,)

        # Sort field indices from the slicing
        names = tuple(x for x in args if isinstance(x, six.string_types))
        args = tuple(x for x in args if not isinstance(x, six.string_types))
        if six.PY2:
            names = tuple(x.encode('utf-8') if isinstance(x, six.text_type) else x for x in names)

        # Generally we try to avoid converting the arrays on the Python
        # side.  However, for compound literals this is unavoidable.
        vlen = h5t.check_dtype(vlen=self.dtype)
        if vlen is not None and vlen not in (bytes, six.text_type):
            try:
                val = numpy.asarray(val, dtype=vlen)
            except ValueError:
                try:
                    val = numpy.array([numpy.array(x, dtype=vlen)
                                       for x in val], dtype=self.dtype)
                except ValueError:
                    pass
            if vlen == val.dtype:
                if val.ndim > 1:
                    tmp = numpy.empty(shape=val.shape[:-1], dtype=object)
                    tmp.ravel()[:] = [i for i in val.reshape(
                        (numpy.product(val.shape[:-1], dtype=numpy.ulonglong), val.shape[-1]))]
                else:
                    tmp = numpy.array([None], dtype=object)
                    tmp[0] = val
                val = tmp
        elif self.dtype.kind == "O" or \
          (self.dtype.kind == 'V' and \
          (not isinstance(val, numpy.ndarray) or val.dtype.kind != 'V') and \
          (self.dtype.subdtype == None)):
            if len(names) == 1 and self.dtype.fields is not None:
                # Single field selected for write, from a non-array source
                if not names[0] in self.dtype.fields:
                    raise ValueError("No such field for indexing: %s" % names[0])
                dtype = self.dtype.fields[names[0]][0]
                cast_compound = True
            else:
                dtype = self.dtype
                cast_compound = False

            val = numpy.asarray(val, dtype=dtype.base, order='C')
            if cast_compound:
                val = val.view(numpy.dtype([(names[0], dtype)]))
                val = val.reshape(val.shape[:len(val.shape) - len(dtype.shape)])
        else:
            val = numpy.asarray(val, order='C')

        # Check for array dtype compatibility and convert
        if self.dtype.subdtype is not None:
            shp = self.dtype.subdtype[1]
            valshp = val.shape[-len(shp):]
            if valshp != shp:  # Last dimension has to match
                raise TypeError("When writing to array types, last N dimensions have to match (got %s, but should be %s)" % (valshp, shp,))
            mtype = h5t.py_create(numpy.dtype((val.dtype, shp)))
            mshape = val.shape[0:len(val.shape)-len(shp)]

        # Make a compound memory type if field-name slicing is required
        elif len(names) != 0:

            mshape = val.shape

            # Catch common errors
            if self.dtype.fields is None:
                raise TypeError("Illegal slicing argument (not a compound dataset)")
            mismatch = [x for x in names if x not in self.dtype.fields]
            if len(mismatch) != 0:
                mismatch = ", ".join('"%s"'%x for x in mismatch)
                raise ValueError("Illegal slicing argument (fields %s not in dataset type)" % mismatch)

            # Write non-compound source into a single dataset field
            if len(names) == 1 and val.dtype.fields is None:
                subtype = h5t.py_create(val.dtype)
                mtype = h5t.create(h5t.COMPOUND, subtype.get_size())
                mtype.insert(self._e(names[0]), 0, subtype)

            # Make a new source type keeping only the requested fields
            else:
                fieldnames = [x for x in val.dtype.names if x in names] # Keep source order
                mtype = h5t.create(h5t.COMPOUND, val.dtype.itemsize)
                for fieldname in fieldnames:
                    subtype = h5t.py_create(val.dtype.fields[fieldname][0])
                    offset = val.dtype.fields[fieldname][1]
                    mtype.insert(self._e(fieldname), offset, subtype)

        # Use mtype derived from array (let DatasetID.write figure it out)
        else:
            mshape = val.shape
            mtype = None

        # Perform the dataspace selection
        selection = sel.select(self.shape, args, dsid=self.id)

        if selection.nselect == 0:
            return

        # Broadcast scalars if necessary.
        if mshape == () and selection.mshape != ():
            if self.dtype.subdtype is not None:
                raise TypeError("Scalar broadcasting is not supported for array dtypes")
            val2 = numpy.empty(selection.mshape[-1], dtype=val.dtype)
            val2[...] = val
            val = val2
            mshape = val.shape

        # Perform the write, with broadcasting
        # Be careful to pad memory shape with ones to avoid HDF5 chunking
        # glitch, which kicks in for mismatched memory/file selections
        if len(mshape) < len(self.shape):
            mshape_pad = (1,)*(len(self.shape)-len(mshape)) + mshape
        else:
            mshape_pad = mshape
        mspace = h5s.create_simple(mshape_pad, (h5s.UNLIMITED,)*len(mshape_pad))
        for fspace in selection.broadcast(mshape):
            self.id.write(mspace, fspace, val, mtype, dxpl=self._dxpl)

    def read_direct(self, dest, source_sel=None, dest_sel=None):
        """ Read data directly from HDF5 into an existing NumPy array.

        The destination array must be C-contiguous and writable.
        Selections must be the output of numpy.s_[<args>].

        Broadcasting is supported for simple indexing.
        """
        with phil:
            if is_empty_dataspace(self.id):
                raise TypeError("Empty datasets have no numpy representation")
            if source_sel is None:
                source_sel = sel.SimpleSelection(self.shape)
            else:
                source_sel = sel.select(self.shape, source_sel, self.id)  # for numpy.s_
            fspace = source_sel.id

            if dest_sel is None:
                dest_sel = sel.SimpleSelection(dest.shape)
            else:
                dest_sel = sel.select(dest.shape, dest_sel, self.id)

            for mspace in dest_sel.broadcast(source_sel.mshape):
                self.id.read(mspace, fspace, dest, dxpl=self._dxpl)

    def write_direct(self, source, source_sel=None, dest_sel=None):
        """ Write data directly to HDF5 from a NumPy array.

        The source array must be C-contiguous.  Selections must be
        the output of numpy.s_[<args>].

        Broadcasting is supported for simple indexing.
        """
        with phil:
            if is_empty_dataspace(self.id):
                raise TypeError("Empty datasets cannot be written to")
            if source_sel is None:
                source_sel = sel.SimpleSelection(source.shape)
            else:
                source_sel = sel.select(source.shape, source_sel, self.id)  # for numpy.s_
            mspace = source_sel.id

            if dest_sel is None:
                dest_sel = sel.SimpleSelection(self.shape)
            else:
                dest_sel = sel.select(self.shape, dest_sel, self.id)

            for fspace in dest_sel.broadcast(source_sel.mshape):
                self.id.write(mspace, fspace, source, dxpl=self._dxpl)

    @with_phil
    def __array__(self, dtype=None):
        """ Create a Numpy array containing the whole dataset.  DON'T THINK
        THIS MEANS DATASETS ARE INTERCHANGEABLE WITH ARRAYS.  For one thing,
        you have to read the whole dataset every time this method is called.
        """
        arr = numpy.empty(self.shape, dtype=self.dtype if dtype is None else dtype)

        # Special case for (0,)*-shape datasets
        if numpy.product(self.shape, dtype=numpy.ulonglong) == 0:
            return arr

        self.read_direct(arr)
        return arr

    @with_phil
    def __repr__(self):
        if not self:
            r = u'<Closed HDF5 dataset>'
        else:
            if self.name is None:
                namestr = u'("anonymous")'
            else:
                name = pp.basename(pp.normpath(self.name))
                namestr = u'"%s"' % (name if name != u'' else u'/')
            r = u'<HDF5 dataset %s: shape %s, type "%s">' % (
                namestr, self.shape, self.dtype.str
            )
        if six.PY2:
            return r.encode('utf8')
        return r

    if hasattr(h5d.DatasetID, "refresh"):
        @with_phil
        def refresh(self):
            """ Refresh the dataset metadata by reloading from the file.

            This is part of the SWMR features and only exist when the HDF5
            library version >=1.9.178
            """
            self._id.refresh()

    if hasattr(h5d.DatasetID, "flush"):
        @with_phil
        def flush(self):
            """ Flush the dataset data and metadata to the file.
            If the dataset is chunked, raw data chunks are written to the file.

            This is part of the SWMR features and only exist when the HDF5
            library version >=1.9.178
            """
            self._id.flush()

    if vds_support:
        @property
        def is_virtual(self):
            return self._dcpl.get_layout() == h5d.VIRTUAL

        def virtual_sources(self):
            if not self.is_virtual:
                raise RuntimeError("Not a virtual dataset")
            dcpl = self._dcpl
            return [
                VDSmap(dcpl.get_virtual_vspace(j),
                       dcpl.get_virtual_filename(j),
                       dcpl.get_virtual_dsetname(j),
                       dcpl.get_virtual_srcspace(j))
                for j in range(dcpl.get_virtual_count())]
