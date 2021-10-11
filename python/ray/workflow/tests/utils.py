import pathlib
import tempfile
import os
import ray
from ray import workflow
from ray.workflow.storage import set_global_storage

_GLOBAL_MARK_PATH= pathlib.Path(tempfile.gettempdir())


def unset_global_mark(name="__workflow_test"):
    mark_file = _GLOBAL_MARK_PATH / name
    if mark_file.exists():
        mark_file.unlink()


def set_global_mark(name="__workflow_test"):
    mark_file = _GLOBAL_MARK_PATH / name
    mark_file.touch()


def check_global_mark(name="__workflow_test"):
    mark_file = _GLOBAL_MARK_PATH / name
    return mark_file.exists()


def _alter_storage(new_storage):
    set_global_storage(new_storage)
    # alter the storage
    ray.shutdown()
    os.system("ray stop --force")
    workflow.init(new_storage)
