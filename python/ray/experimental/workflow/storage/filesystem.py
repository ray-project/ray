import contextlib
import json
import pathlib
from typing import Any, Dict, Callable
import uuid

from ray.experimental.workflow.common import StepID
from ray.experimental.workflow.storage.base import (
    Storage, ArgsType, StepStatus, DataLoadError, DataSaveError)

import ray.cloudpickle

# constants used in filesystem
OBJECTS_DIR = "objects"
STEPS_DIR = "steps"
STEP_INPUTS_METADATA = "inputs.json"
STEP_OUTPUTS_METADATA = "outputs.json"
STEP_ARGS = "args.pkl"
STEP_OUTPUT = "output.pkl"
STEP_FUNC_BODY = "func_body.pkl"


@contextlib.contextmanager
def _open_atomic(path: pathlib.Path, mode="r"):
    """Open file with atomic file writing support. File reading is also
    adapted to atomic file writing (for example, the backup file
    is used when an atomic write failed previously.)

    TODO(suquark): race condition like two processes writing the
    same file is still not safe. This may not be an issue, because
    in our current implementation, we only need to guarantee the
    file is either fully written or not existing.

    Args:
        path: The file path.
        mode: Open mode same as "open()".

    Returns:
        File object.
    """
    if "a" in mode or "+" in mode:
        raise ValueError("Atomic open does not support appending.")
    # backup file is hidden by default
    backup_path = path.with_name(f".{path.name}.backup")
    if "r" in mode:  # read mode
        if _file_exists(path):
            f = open(path, mode)
        else:
            raise FileNotFoundError(path)
        try:
            yield f
        finally:
            f.close()
    elif "x" in mode:  # create mode
        if path.exists():
            raise FileExistsError(path)
        tmp_new_fn = path.with_suffix(f".{path.name}.{uuid.uuid4().hex}")
        if not tmp_new_fn.parent.exists():
            tmp_new_fn.parent.mkdir(parents=True)
        f = open(tmp_new_fn, mode)
        write_ok = True
        try:
            yield f
        except Exception:
            write_ok = False
            raise
        finally:
            f.close()
            if write_ok:
                # "commit" file if writing succeeded
                tmp_new_fn.rename(path)
            else:
                # remove file if writing failed
                tmp_new_fn.unlink()
    elif "w" in mode:  # overwrite mode
        # backup existing file
        if path.exists():
            # remove an even older backup file
            if backup_path.exists():
                backup_path.unlink()
            path.rename(backup_path)
        tmp_new_fn = path.with_suffix(f".{path.name}.{uuid.uuid4().hex}")
        if not tmp_new_fn.parent.exists():
            tmp_new_fn.parent.mkdir(parents=True)
        f = open(tmp_new_fn, mode)
        write_ok = True
        try:
            yield f
        except Exception:
            write_ok = False
            raise
        finally:
            f.close()
            if write_ok:
                tmp_new_fn.rename(path)
                # cleanup the backup file
                if backup_path.exists():
                    backup_path.unlink()
            else:
                # remove file if writing failed
                tmp_new_fn.unlink()
    else:
        raise ValueError(f"Unknown file open mode {mode}.")


def _file_exists(path: pathlib.Path) -> bool:
    """During atomic writing, we backup the original file. If the writing
    failed during the middle, then only the backup exists. We consider the
    file exists if the file or the backup file exists. We also automatically
    restore the backup file to the original path if only backup file exists.

    Args:
        path: File path.

    Returns:
        True if the file and backup exists.
    """
    backup_path = path.with_name(f".{path.name}.backup")
    if path.exists():
        return True
    elif backup_path.exists():
        backup_path.rename(path)
        return True
    return False


class FilesystemStorageImpl(Storage):
    """Filesystem implementation for accessing workflow storage.

    We do not repeat the same comments for abstract methods in the base class.
    """

    def __init__(self, workflow_root_dir: str):
        self._workflow_root_dir = pathlib.Path(workflow_root_dir)

    def load_step_input_metadata(self, workflow_id: str,
                                 step_id: StepID) -> Dict[str, Any]:
        step_dir = self._workflow_root_dir / workflow_id / STEPS_DIR / step_id
        try:
            with _open_atomic(step_dir / STEP_INPUTS_METADATA) as f:
                return json.load(f)
        except Exception as e:
            raise DataLoadError from e

    def save_step_input_metadata(self, workflow_id: str, step_id: StepID,
                                 metadata: Dict[str, Any]) -> None:
        step_dir = self._workflow_root_dir / workflow_id / STEPS_DIR / step_id
        try:
            with _open_atomic(step_dir / STEP_INPUTS_METADATA, "w") as f:
                json.dump(metadata, f)
        except Exception as e:
            raise DataSaveError from e

    def load_step_output_metadata(self, workflow_id: str,
                                  step_id: StepID) -> Dict[str, Any]:
        step_dir = self._workflow_root_dir / workflow_id / STEPS_DIR / step_id
        try:
            with _open_atomic(step_dir / STEP_OUTPUTS_METADATA) as f:
                return json.load(f)
        except Exception as e:
            raise DataLoadError from e

    def save_step_output_metadata(self, workflow_id: str, step_id: StepID,
                                  metadata: Dict[str, Any]) -> None:
        step_dir = self._workflow_root_dir / workflow_id / STEPS_DIR / step_id
        try:
            with _open_atomic(step_dir / STEP_OUTPUTS_METADATA, "w") as f:
                json.dump(metadata, f)
        except Exception as e:
            raise DataSaveError from e

    def load_step_output(self, workflow_id: str, step_id: StepID) -> Any:
        step_dir = self._workflow_root_dir / workflow_id / STEPS_DIR / step_id
        try:
            with _open_atomic(step_dir / STEP_OUTPUT, "rb") as f:
                return ray.cloudpickle.load(f)
        except Exception as e:
            raise DataLoadError from e

    def save_step_output(self, workflow_id: str, step_id: StepID,
                         output: Any) -> None:
        step_dir = self._workflow_root_dir / workflow_id / STEPS_DIR / step_id
        try:
            with _open_atomic(step_dir / STEP_OUTPUT, "wb") as f:
                ray.cloudpickle.dump(output, f)
        except Exception as e:
            raise DataSaveError from e

    def load_step_func_body(self, workflow_id: str,
                            step_id: StepID) -> Callable:
        step_dir = self._workflow_root_dir / workflow_id / STEPS_DIR / step_id
        try:
            with _open_atomic(step_dir / STEP_FUNC_BODY, "rb") as f:
                return ray.cloudpickle.load(f)
        except Exception as e:
            raise DataLoadError from e

    def save_step_func_body(self, workflow_id: str, step_id: StepID,
                            func_body: Callable) -> None:
        step_dir = self._workflow_root_dir / workflow_id / STEPS_DIR / step_id
        try:
            with _open_atomic(step_dir / STEP_FUNC_BODY, "wb") as f:
                ray.cloudpickle.dump(func_body, f)
        except Exception as e:
            raise DataSaveError from e

    def load_step_args(self, workflow_id: str, step_id: StepID) -> ArgsType:
        step_dir = self._workflow_root_dir / workflow_id / STEPS_DIR / step_id
        try:
            with _open_atomic(step_dir / STEP_ARGS, "rb") as f:
                return ray.cloudpickle.load(f)
        except Exception as e:
            raise DataLoadError from e

    def save_step_args(self, workflow_id: str, step_id: StepID,
                       args: ArgsType) -> None:
        step_dir = self._workflow_root_dir / workflow_id / STEPS_DIR / step_id
        try:
            with _open_atomic(step_dir / STEP_ARGS, "wb") as f:
                ray.cloudpickle.dump(args, f)
        except Exception as e:
            raise DataSaveError from e

    def load_object_ref(self, workflow_id: str, object_id) -> ray.ObjectRef:
        objects_dir = self._workflow_root_dir / workflow_id / OBJECTS_DIR
        try:
            with _open_atomic(objects_dir / object_id, "rb") as f:
                obj = ray.cloudpickle.load(f)
            return ray.put(obj)  # simulate an ObjectRef
        except Exception as e:
            raise DataLoadError from e

    def save_object_ref(self, workflow_id: str, rref: ray.ObjectRef) -> None:
        objects_dir = self._workflow_root_dir / workflow_id / OBJECTS_DIR
        try:
            obj = ray.get(rref)
            with _open_atomic(objects_dir / rref.hex(), "wb") as f:
                ray.cloudpickle.dump(obj, f)
        except Exception as e:
            raise DataSaveError from e

    def get_step_status(self, workflow_id: str, step_id: StepID) -> StepStatus:
        step_dir = self._workflow_root_dir / workflow_id / STEPS_DIR / step_id
        return StepStatus(
            output_object_exists=(step_dir / STEP_OUTPUT).exists(),
            output_metadata_exists=(step_dir / STEP_OUTPUTS_METADATA).exists(),
            input_metadata_exists=(step_dir / STEP_INPUTS_METADATA).exists(),
            args_exists=(step_dir / STEP_ARGS).exists(),
            func_body_exists=(step_dir / STEP_FUNC_BODY).exists(),
        )

    @property
    def storage_url(self) -> str:
        return str(self._workflow_root_dir)
