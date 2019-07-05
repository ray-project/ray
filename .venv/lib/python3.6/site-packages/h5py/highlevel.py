# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

# pylint: disable=unused-import

"""
    This is the deprecated legacy high-level interface.

    Everything here is canonically located at the root of the package.
    New code should import directly from there, e.g. "from h5py import File".
"""

from __future__ import absolute_import

import warnings
from .h5py_warnings import H5pyDeprecationWarning
warnings.warn(
    "The h5py.highlevel module is deprecated, code should import directly "
    "from h5py, e.g. 'from h5py import File'.",
    H5pyDeprecationWarning,
    stacklevel=2
)

from ._hl import filters
from ._hl.base import is_hdf5, HLObject
from ._hl.files import File
from ._hl.group import Group, SoftLink, ExternalLink, HardLink
from ._hl.dataset import Dataset
from ._hl.datatype import Datatype
from ._hl.attrs import AttributeManager
