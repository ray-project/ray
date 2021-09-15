import pathlib
import tempfile
import os
import ray
from ray import workflow
from ray.workflow.storage import set_global_storage

_GLOBAL_MARK_FILE = pathlib.Path(tempfile.gettempdir()) / "__workflow_test"


def unset_global_mark():
    if _GLOBAL_MARK_FILE.exists():
        _GLOBAL_MARK_FILE.unlink()


def set_global_mark():
    _GLOBAL_MARK_FILE.touch()


def check_global_mark():
    return _GLOBAL_MARK_FILE.exists()


def _alter_storage(new_storage):
    set_global_storage(new_storage)
    # alter the storage
    ray.shutdown()
    os.system("ray stop --force")
    workflow.init(new_storage)
