import pathlib
import tempfile

_GLOBAL_MARK_FILE = pathlib.Path(tempfile.gettempdir()) / "__workflow_test"


def unset_global_mark():
    if _GLOBAL_MARK_FILE.exists():
        _GLOBAL_MARK_FILE.unlink()


def set_global_mark():
    _GLOBAL_MARK_FILE.touch()


def check_global_mark():
    return _GLOBAL_MARK_FILE.exists()
