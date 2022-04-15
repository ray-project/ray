import pathlib
import tempfile

_GLOBAL_MARK_PATH = pathlib.Path(tempfile.gettempdir())


def unset_global_mark(name="workflow"):
    mark_file = _GLOBAL_MARK_PATH / f"workflow-{name}"
    if mark_file.exists():
        mark_file.unlink()


def set_global_mark(name="workflow"):
    mark_file = _GLOBAL_MARK_PATH / f"workflow-{name}"
    mark_file.touch()


def check_global_mark(name="workflow"):
    mark_file = _GLOBAL_MARK_PATH / f"workflow-{name}"
    return mark_file.exists()


def _alter_storage(new_storage):
    raise Exception("This method is deprecated.")


def clear_marks():
    files = _GLOBAL_MARK_PATH.glob("**/workflow-*")
    for file in files:
        file.unlink()
