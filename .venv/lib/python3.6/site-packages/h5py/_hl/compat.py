"""
Compatibility module for high-level h5py
"""
import sys
import six

WINDOWS_ENCODING = "mbcs"


try:
    from os import fspath
except ImportError:
    def fspath(path):
        """
        Return the string representation of the path.
        If str or bytes is passed in, it is returned unchanged.
        This code comes from PEP 519, modified to support earlier versions of
        python.

        This is required for python < 3.6.
        """
        if isinstance(path, (six.text_type, six.binary_type)):
            return path

        # Work from the object's type to match method resolution of other magic
        # methods.
        path_type = type(path)
        try:
            return path_type.__fspath__(path)
        except AttributeError:
            if hasattr(path_type, '__fspath__'):
                raise
            try:
                import pathlib
            except ImportError:
                pass
            else:
                if isinstance(path, pathlib.PurePath):
                    return six.text_type(path)

            raise TypeError("expected str, bytes or os.PathLike object, not "
                            + path_type.__name__)

# This is from python 3.5 stdlib (hence lacks PEP 519 changes)
# This was introduced into python 3.2, so python < 3.2 does not have this
# Effectively, this is only required for python 2.6 and 2.7, and can be removed
# once support for them is dropped
def _fscodec():
    encoding = sys.getfilesystemencoding()
    if encoding == 'mbcs':
        errors = 'strict'
    else:
        try:
            from codecs import lookup_error
            lookup_error('surrogateescape')
        except LookupError:
            errors = 'strict'
        else:
            errors = 'surrogateescape'

    def fsencode(filename):
        """
        Encode filename to the filesystem encoding with 'surrogateescape' error
        handler, return bytes unchanged. On Windows, use 'strict' error handler if
        the file system encoding is 'mbcs' (which is the default encoding).
        """
        if isinstance(filename, six.binary_type):
            return filename
        elif isinstance(filename, six.text_type):
            return filename.encode(encoding, errors)
        else:
            raise TypeError("expect bytes or str, not %s" % type(filename).__name__)

    def fsdecode(filename):
        """
        Decode filename from the filesystem encoding with 'surrogateescape' error
        handler, return str unchanged. On Windows, use 'strict' error handler if
        the file system encoding is 'mbcs' (which is the default encoding).
        """
        if isinstance(filename, six.text_type):
            return filename
        elif isinstance(filename, six.binary_type):
            return filename.decode(encoding, errors)
        else:
            raise TypeError("expect bytes or str, not %s" % type(filename).__name__)

    return fsencode, fsdecode

_fsencode, _fsdecode = _fscodec()
del _fscodec

try:
    from os import fsencode
except ImportError:
    fsencode = _fsencode

try:
    from os import fsdecode
except ImportError:
    fsdecode = _fsdecode


def filename_encode(filename):
    """
    Encode filename for use in the HDF5 library.

    Due to how HDF5 handles filenames on different systems, this should be
    called on any filenames passed to the HDF5 library. See the documentation on
    filenames in h5py for more information.
    """
    filename = fspath(filename)
    if sys.platform == "win32":
        if isinstance(filename, six.text_type):
            return filename.encode(WINDOWS_ENCODING, "strict")
        return filename
    return fsencode(filename)


def filename_decode(filename):
    """
    Decode filename used by HDF5 library.

    Due to how HDF5 handles filenames on different systems, this should be
    called on any filenames passed from the HDF5 library. See the documentation
    on filenames in h5py for more information.
    """
    if sys.platform == "win32":
        if isinstance(filename, six.binary_type):
            return filename.decode(WINDOWS_ENCODING, "strict")
        elif isinstance(filename, six.text_type):
            return filename
        else:
            raise TypeError("expect bytes or str, not %s" % type(filename).__name__)
    return fsdecode(filename)
