# Based on http://stackoverflow.com/questions/2333872/atomic-writing-to-file-with-python

import os
from contextlib import contextmanager

# We would ideally atomically replace any existing file with the new
# version. However, on Windows there's no Python-only solution prior
# to Python 3.3. (This library includes a C extension to do so:
# https://pypi.python.org/pypi/pyosreplace/0.1.)
#
# Correspondingly, we make a best effort, but on Python < 3.3 use a
# replace method which could result in the file temporarily
# disappearing.
import sys
if sys.version_info >= (3, 3):
    # Python 3.3 and up have a native `replace` method
    from os import replace
elif sys.platform.startswith("win"):
    def replace(src, dst):
        # TODO: on Windows, this will raise if the file is in use,
        # which is possible. We'll need to make this more robust over
        # time.
        try:
            os.remove(dst)
        except OSError:
            pass
        os.rename(src, dst)
else:
    # POSIX rename() is always atomic
    from os import rename as replace

@contextmanager
def atomic_write(filepath, binary=False, fsync=False):
    """ Writeable file object that atomically updates a file (using a temporary file). In some cases (namely Python < 3.3 on Windows), this could result in an existing file being temporarily unlinked.

    :param filepath: the file path to be opened
    :param binary: whether to open the file in a binary mode instead of textual
    :param fsync: whether to force write the file to disk
    """

    tmppath = filepath + '~'
    while os.path.isfile(tmppath):
        tmppath += '~'
    try:
        with open(tmppath, 'wb' if binary else 'w') as file:
            yield file
            if fsync:
                file.flush()
                os.fsync(file.fileno())
        replace(tmppath, filepath)
    finally:
        try:
            os.remove(tmppath)
        except (IOError, OSError):
            pass
