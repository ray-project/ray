# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

"""
    High-level interface for creating HDF5 virtual datasets
"""

from copy import deepcopy as copy
from collections import namedtuple
from .selections import SimpleSelection, select
from .. import h5s, h5
from .. import version


class VDSmap(namedtuple('VDSmap', ('vspace', 'file_name',
                                   'dset_name', 'src_space'))):
    '''Defines a region in a virtual dataset mapping to part of a source dataset
    '''


vds_support = False
hdf5_version = version.hdf5_version_tuple[0:3]

if hdf5_version >= h5.get_config().vds_min_hdf5_version:
    vds_support = True


class VirtualSource(object):
    """Source definition for virtual data sets.

    Instantiate this class to represent an entire source dataset, and then
    slice it to indicate which regions should be used in the virtual dataset.

    path_or_dataset
        The path to a file, or an h5py dataset. If a dataset is given,
        the other parameters are ignored, and the relevant values taken from
        the dataset instead.
    name
        The name of the source dataset within the file.
    shape
        A tuple giving the shape of the dataset.
    dtype
        Numpy dtype or string.
    maxshape
        The source dataset is resizable up to this shape. Use None for
        axes you want to be unlimited.
    """
    def __init__(self, path_or_dataset, name=None,
                 shape=None, dtype=None, maxshape=None):
        from .dataset import Dataset
        if isinstance(path_or_dataset, Dataset):
            failed = {k: v
                      for k, v in
                      {'name': name, 'shape': shape,
                       'dtype': dtype, 'maxshape': maxshape}.items()
                      if v is not None}
            if failed:
                raise TypeError("If a Dataset is passed as the first argument "
                                "then no other arguments may be passed.  You "
                                "passed {failed}".format(failed=failed))
            ds = path_or_dataset
            path = ds.file.filename
            name = ds.name
            shape = ds.shape
            dtype = ds.dtype
            maxshape = ds.maxshape
        else:
            path = path_or_dataset
            if name is None:
                raise TypeError("The name parameter is required when "
                                "specifying a source by path")
            if shape is None:
                raise TypeError("The shape parameter is required when "
                                "specifying a source by path")
        self.path = path
        self.name = name
        self.dtype = dtype

        if maxshape is None:
            self.maxshape = shape
        else:
            self.maxshape = tuple([h5s.UNLIMITED if ix is None else ix
                                   for ix in maxshape])
        self.sel = SimpleSelection(shape)

    @property
    def shape(self):
        return self.sel.mshape

    def __getitem__(self, key):
        tmp = copy(self)
        tmp.sel = select(self.shape, key, dsid=None)
        return tmp

class VirtualLayout(object):
    """Object for building a virtual dataset.

    Instantiate this class to define a virtual dataset, assign to slices of it
    (using VirtualSource objects), and then pass it to
    group.create_virtual_dataset() to add the virtual dataset to a file.

    This class does not allow access to the data; the virtual dataset must
    be created in a file before it can be used.

    shape
        A tuple giving the shape of the dataset.
    dtype
        Numpy dtype or string.
    maxshape
        The source dataset is resizable up to this shape. Use None for
        axes you want to be unlimited.
    """
    def __init__(self, shape, dtype=None, maxshape=None):
        self.shape = shape
        self.dtype = dtype
        self.maxshape = maxshape
        self.sources = []

    def __setitem__(self, key, source):
        sel = select(self.shape, key, dsid=None)
        self.sources.append(VDSmap(sel.id,
                                   source.path,
                                   source.name,
                                   source.sel.id))
