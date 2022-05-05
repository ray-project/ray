import pathlib
import tempfile

from ray.remote_function import RemoteFunction
from ray import workflow
from ray.workflow.common import WORKFLOW_OPTIONS

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


def update_workflow_options(f: RemoteFunction, **workflow_options) -> RemoteFunction:
    new_metadata = f._default_options.get("_metadata", {}).copy()
    # copy again because the origina copy is shallow copy
    new_workflow_options = new_metadata.get(WORKFLOW_OPTIONS, {}).copy()
    new_workflow_options.update(**workflow_options)
    new_metadata[WORKFLOW_OPTIONS] = new_workflow_options
    return f.options(_metadata=new_metadata)


def run_workflow_dag_with_options(
    f: RemoteFunction, args, workflow_id=None, **workflow_options
):
    wf = workflow.create(update_workflow_options(f, **workflow_options).bind(*args))
    return wf.run(workflow_id=workflow_id)
