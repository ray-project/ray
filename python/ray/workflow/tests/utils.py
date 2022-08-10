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


def assert_task_checkpoints(wf_storage, task_id, mode: str):
    """Assert the checkpoint status of a workflow task."""
    result = wf_storage.inspect_task(task_id)
    if mode == "all_skipped":
        assert not result.output_object_valid
        assert result.output_task_id is None
        assert not result.args_valid
        assert not result.func_body_valid
        assert not result.task_options
    elif mode == "output_skipped":
        assert not result.output_object_valid
        assert result.output_task_id is None
        assert result.args_valid
        assert result.func_body_valid
        assert result.task_options is not None
    elif mode == "checkpointed":
        assert result.output_object_valid or result.output_task_id is not None
    else:
        raise ValueError("Unknown mode.")


def skip_client_mode_test():
    import pytest
    from ray._private.client_mode_hook import client_mode_should_convert

    if client_mode_should_convert(auto_init=False):
        pytest.skip("Not for Ray client test")
