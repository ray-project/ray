import contextlib
import shutil
import tempfile

@contextlib.contextmanager
def tempdir():
    temp = tempfile.mkdtemp()
    yield temp
    shutil.rmtree(temp)
