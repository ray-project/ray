# Copyright 2015 The TensorFlow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
"""File IO methods that wrap the C++ FileSystem API.

The C++ FileSystem API is SWIG wrapped in file_io.i. These functions call those
to accomplish basic File IO operations.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import binascii
import collections
import glob
import os
import shutil
import six
import uuid

from . import compat, errors


# A good default block size depends on the system in question.
# A somewhat conservative default chosen here.
_DEFAULT_BLOCK_SIZE = 16 * 1024 * 1024


class FileIO(object):
    # Only methods needed for TensorBoard are implemented.

    def __init__(self, filename, mode):
        self.filename = compat.as_bytes(filename)
        self.mode = compat.as_bytes(mode)
        self.f = open(self.filename, self.mode)

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.f.close()

    def __iter__(self):
        return self.f

    def size(self):
        return os.stat(self.filename).st_size

    def read(self):
        return self.f.read()

    def close(self):
        self.f.close()


class GFile(FileIO):
    # Same interface as FileIO but through GFile class

    def __init__(self, filename, mode):
        super(GFile, self).__init__(filename, mode)


# @tf_export("gfile.Exists")
def Exists(filename):
    """Determines whether a path exists or not.

    Args:
      filename: string, a path

    Returns:
      True if the path exists, whether its a file or a directory.
      False if the path does not exist and there are no filesystem errors.

    Raises:
      errors.OpError: Propagates any errors reported by the FileSystem API.
    """
    try:
        return os.path.exists(compat.as_bytes(filename))
    except errors.NotFoundError:
        return False
    return True


# @tf_export("gfile.Remove")
def Remove(filename):
    """Deletes the file located at 'filename'.

    Args:
        filename: string, a filename

    Raises:
        errors.OpError: Propagates any errors reported by the FileSystem API.
        E.g., NotFoundError if the file does not exist.
    """
    fn = compat.as_bytes(filename)
    if not os.path.exists(fn) or not os.path.isfile(fn):
        return
    os.remove(fn)


def read_file_to_string(filename, binary_mode=False):
    """Reads the entire contents of a file to a string.

    Args:
      filename: string, path to a file
      binary_mode: whether to open the file in binary mode or not. This changes
          the type of the object returned.

    Returns:
      contents of the file as a string or bytes.

    Raises:
      errors.OpError: Raises variety of errors that are subtypes e.g.
      NotFoundError etc.
    """
    if binary_mode:
        f = FileIO(filename, mode="rb")
    else:
        f = FileIO(filename, mode="r")
    return f.read()


def write_string_to_file(filename, file_content):
    """Writes a string to a given file.

    Args:
      filename: string, path to a file
      file_content: string, contents that need to be written to the file

    Raises:
      errors.OpError: If there are errors during the operation.
    """
    with FileIO(filename, mode="w") as f:
        f.write(file_content)


# @tf_export("gfile.Glob")
def Glob(filename):
    """Returns a list of files that match the given pattern(s).

    Args:
    filename: string or iterable of strings. The glob pattern(s).

    Returns:
    A list of strings containing filenames that match the given pattern(s).

    Raises:
    errors.OpError: If there are filesystem / directory listing errors.
    """
    if isinstance(filename, six.string_types):
        return [
            # compat the filenames to string from bytes.
            compat.as_str_any(matching_filename)
            for matching_filename in glob.glob(compat.as_bytes(filename))
        ]
    else:
        return [
            # compat the filenames to string from bytes.
            compat.as_str_any(matching_filename)
            for single_filename in filename
            for matching_filename in glob.glob(compat.as_bytes(single_filename))
        ]


# @tf_export("gfile.MkDir")
def MkDir(dirname):
    """Creates a directory with the name 'dirname'.

    Args:
    dirname: string, name of the directory to be created

    Notes:
    The parent directories need to exist. Use recursive_create_dir instead if
    there is the possibility that the parent dirs don't exist.

    Raises:
    errors.OpError: If the operation fails.
    """
    os.mkdir(compat.as_bytes(dirname))


# @tf_export("gfile.MakeDirs")
def MakeDirs(dirname):
    """Creates a directory and all parent/intermediate directories.

    It succeeds if dirname already exists and is writable.

    Args:
    dirname: string, name of the directory to be created

    Raises:
    errors.OpError: If the operation fails.
    """
    os.makedirs(compat.as_bytes(dirname))


# @tf_export("gfile.Copy")
def Copy(oldpath, newpath, overwrite=False):
    """Copies data from oldpath to newpath.

    Args:
    oldpath: string, name of the file who's contents need to be copied
    newpath: string, name of the file to which to copy to
    overwrite: boolean, if false its an error for newpath to be occupied by an
        existing file.

    Raises:
    errors.OpError: If the operation fails.
    """
    newpath_exists = os.path.exists(newpath)
    if newpath_exists and overwrite or not newpath_exists:
        shutil.copy2(compat.as_bytes(oldpath), compat.as_bytes(newpath))


# @tf_export("gfile.Rename")
def Rename(oldname, newname, overwrite=False):
    """Rename or move a file / directory.

    Args:
      oldname: string, pathname for a file
      newname: string, pathname to which the file needs to be moved
      overwrite: boolean, if false it's an error for `newname` to be occupied by
          an existing file.

    Raises:
      errors.OpError: If the operation fails.
    """
    newname_exists = os.path.exists(newname)
    if newname_exists and overwrite or not newname_exists:
        os.rename(compat.as_bytes(oldname), compat.as_bytes(newname))


def atomic_write_string_to_file(filename, contents, overwrite=True):
    """Writes to `filename` atomically.

    This means that when `filename` appears in the filesystem, it will contain
    all of `contents`. With write_string_to_file, it is possible for the file
    to appear in the filesystem with `contents` only partially written.

    Accomplished by writing to a temp file and then renaming it.

    Args:
      filename: string, pathname for a file
      contents: string, contents that need to be written to the file
      overwrite: boolean, if false it's an error for `filename` to be occupied by
          an existing file.
    """
    temp_pathname = filename + ".tmp" + uuid.uuid4().hex
    write_string_to_file(temp_pathname, contents)
    try:
        Rename(temp_pathname, filename, overwrite)
    except Exception:
        Remove(temp_pathname)
        raise


# @tf_export("gfile.DeleteRecursively")
def DeleteRecursively(dirname):
    """Deletes everything under dirname recursively.

    Args:
      dirname: string, a path to a directory

    Raises:
      errors.OpError: If the operation fails.
    """
    os.removedirs(dirname)


# @tf_export("gfile.IsDirectory")
def IsDirectory(dirname):
    """Returns whether the path is a directory or not.

    Args:
      dirname: string, path to a potential directory

    Returns:
      True, if the path is a directory; False otherwise
    """
    return os.path.isdir(compat.as_bytes(dirname))


# @tf_export("gfile.ListDirectory")
def ListDirectory(dirname):
    """Returns a list of entries contained within a directory.

    The list is in arbitrary order. It does not contain the special entries "."
    and "..".

    Args:
      dirname: string, path to a directory

    Returns:
      [filename1, filename2, ... filenameN] as strings

    Raises:
      errors.NotFoundError if directory doesn't exist
    """
    if not IsDirectory(dirname):
        raise errors.NotFoundError(None, None, "Could not find directory")
    try:
        entries = os.listdir(compat.as_bytes(dirname))

    except Exception:
        entries = os.listdir(compat.as_str_any(dirname))

    entries = [compat.as_str_any(item) for item in entries]
    return entries


# @tf_export("gfile.Walk")
def Walk(top, in_order=True):
    """Recursive directory tree generator for directories.

    Args:
      top: string, a Directory name
      in_order: bool, Traverse in order if True, post order if False.

    Errors that happen while listing directories are ignored.

    Yields:
      Each yield is a 3-tuple:  the pathname of a directory, followed by lists of
      all its subdirectories and leaf files.
      (dirname, [subdirname, subdirname, ...], [filename, filename, ...])
      as strings
    """
    top = compat.as_str_any(top)
    try:
        listing = ListDirectory(top)
    except errors.NotFoundError:
        return

    files = []
    subdirs = []
    for item in listing:
        full_path = os.path.join(top, item)
        if IsDirectory(full_path):
            subdirs.append(item)
        else:
            files.append(item)

    here = (top, subdirs, files)

    if in_order:
        yield here

    for subdir in subdirs:
        for subitem in os.walk(os.path.join(top, subdir), in_order):
            yield subitem

    if not in_order:
        yield here


# Data returned from the Stat call.
StatData = collections.namedtuple('StatData', ['length'])


# @tf_export("gfile.Stat")
def Stat(filename):
    """Returns file statistics for a given path.

    Args:
      filename: string, path to a file

    Returns:
      FileStatistics struct that contains information about the path

    Raises:
      errors.OpError: If the operation fails.
    """
    result = None
    try:
        # Size of the file is given by .st_size as returned from
        # os.stat() but TB uses .length so we set length.
        len = os.stat(compat.as_bytes(filename)).st_size
        result = StatData(len)
    except Exception:
        pass

    if result is None:
        raise errors.NotFoundError(None, None, 'Unable to stat file')
    return result


def filecmp(filename_a, filename_b):
    """Compare two files, returning True if they are the same, False otherwise.

    We check size first and return False quickly if the files are different sizes.
    If they are the same size, we continue to generating a crc for the whole file.

    You might wonder: why not use Python's filecmp.cmp() instead? The answer is
    that the builtin library is not robust to the many different filesystems
    TensorFlow runs on, and so we here perform a similar comparison with
    the more robust FileIO.

    Args:
      filename_a: string path to the first file.
      filename_b: string path to the second file.

    Returns:
      True if the files are the same, False otherwise.
    """
    size_a = FileIO(filename_a, "rb").size()
    size_b = FileIO(filename_b, "rb").size()
    if size_a != size_b:
        return False

    # Size is the same. Do a full check.
    crc_a = file_crc32(filename_a)
    crc_b = file_crc32(filename_b)
    return crc_a == crc_b


def file_crc32(filename, block_size=_DEFAULT_BLOCK_SIZE):
    """Get the crc32 of the passed file.

    The crc32 of a file can be used for error checking; two files with the same
    crc32 are considered equivalent. Note that the entire file must be read
    to produce the crc32.

    Args:
      filename: string, path to a file
      block_size: Integer, process the files by reading blocks of `block_size`
        bytes. Use -1 to read the file as once.

    Returns:
      hexadecimal as string, the crc32 of the passed file.
    """
    crc = 0
    with FileIO(filename, mode="rb") as f:
        chunk = f.read(n=block_size)
        while chunk:
            crc = binascii.crc32(chunk, crc)
            chunk = f.read(n=block_size)
    return hex(crc & 0xFFFFFFFF)
