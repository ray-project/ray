import contextlib
import json
import pathlib
from typing import Any, Dict, Callable, Optional
import uuid

from ray.experimental.workflow.common import StepID
from ray.experimental.workflow.storage.base import (Storage, ArgsType,
                                                    StepInspectResult)

import ray.cloudpickle

# constants used in filesystem
OBJECTS_DIR = "objects"
STEPS_DIR = "steps"
STEP_INPUTS_METADATA = "inputs.json"
STEP_OUTPUTS_METADATA = "outputs.json"
STEP_OUTPUTS_FORWARD = "forward_outputs.json"
STEP_ARGS = "args.pkl"
STEP_OUTPUT = "output.pkl"
STEP_FUNC_BODY = "func_body.pkl"


@contextlib.contextmanager
def _open_atomic(path: pathlib.Path, mode="r"):
    """Open file with atomic file writing support. File reading is also
    adapted to atomic file writing (for example, the backup file
    is used when an atomic write failed previously.)

    TODO(suquark): race conditions like two processes writing the
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
        raise ValueError("Atomic write does not support appending.")
    backup_path = path.with_suffix(path.suffix + ".backup")
    if "r" in mode:  # read mode
        if path.exists():
            f = open(path, mode)
        elif backup_path.exists():
            f = open(backup_path, mode)
        else:
            raise FileNotFoundError(path)
        try:
            yield f
        finally:
            f.close()
    elif "x" in mode:  # create mode
        if path.exists():
            raise FileExistsError(path)
        tmp_new_fn = path.with_suffix(path.suffix + "." + uuid.uuid4().hex)
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
        tmp_new_fn = path.with_suffix(path.suffix + "." + uuid.uuid4().hex)
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
    file exists if the file or the backup file exists.

    Args:
        path: File path.

    Returns:
        True if the file and backup exists.
    """
    backup_path = path.with_suffix(path.suffix + ".backup")
    return path.exists() or backup_path.exists()


class FilesystemStorageImpl(Storage):
    """Filesystem implementation for accessing workflow storage."""

    def __init__(self, workflow_root_dir: str):
        self._workflow_root_dir = pathlib.Path(workflow_root_dir)

    def load_step_input_metadata(self, workflow_id: str,
                                 step_id: StepID) -> Dict[str, Any]:
        step_dir = self._workflow_root_dir / workflow_id / STEPS_DIR / step_id
        with _open_atomic(step_dir / STEP_INPUTS_METADATA, "w") as f:
            return json.load(f)

    def dump_step_input_metadata(self, workflow_id: str, step_id: StepID,
                                 metadata: Dict[str, Any]) -> None:
        step_dir = self._workflow_root_dir / workflow_id / STEPS_DIR / step_id
        with _open_atomic(step_dir / STEP_INPUTS_METADATA, "w") as f:
            json.dump(metadata, f)

    def load_step_output_metadata(self, workflow_id: str,
                                  step_id: StepID) -> Dict[str, Any]:
        step_dir = self._workflow_root_dir / workflow_id / STEPS_DIR / step_id
        with _open_atomic(step_dir / STEP_OUTPUTS_METADATA, "w") as f:
            return json.load(f)

    def dump_step_output_metadata(self, workflow_id: str, step_id: StepID,
                                  metadata: Dict[str, Any]) -> None:
        step_dir = self._workflow_root_dir / workflow_id / STEPS_DIR / step_id
        with _open_atomic(step_dir / STEP_OUTPUTS_METADATA, "w") as f:
            json.dump(metadata, f)

    def update_output_forward(self, workflow_id: str,
                              forward_output_to: StepID,
                              output_step_id: StepID) -> None:
        steps_dir = self._workflow_root_dir / workflow_id / STEPS_DIR
        if forward_output_to == "":
            # actually it equals to 'steps_dir / ""',
            # but we won't use the trick here because it is not obvious.
            target_dir = steps_dir
        else:
            target_dir = steps_dir / forward_output_to
        with _open_atomic(target_dir / STEP_OUTPUTS_FORWARD, "w") as f:
            json.dump({"step_id": output_step_id}, f)

    def load_step_output(self, workflow_id: str, step_id: StepID) -> Any:
        step_dir = self._workflow_root_dir / workflow_id / STEPS_DIR / step_id
        with _open_atomic(step_dir / STEP_OUTPUT, "rb") as f:
            return ray.cloudpickle.load(f)

    def dump_step_output(self, workflow_id: str, step_id: StepID,
                         output: Any) -> None:
        step_dir = self._workflow_root_dir / workflow_id / STEPS_DIR / step_id
        with _open_atomic(step_dir / STEP_OUTPUT, "wb") as f:
            ray.cloudpickle.dump(output, f)

    def load_step_func_body(self, workflow_id: str,
                            step_id: StepID) -> Callable:
        step_dir = self._workflow_root_dir / workflow_id / STEPS_DIR / step_id
        with _open_atomic(step_dir / STEP_FUNC_BODY, "rb") as f:
            return ray.cloudpickle.load(f)

    def dump_step_func_body(self, workflow_id: str, step_id: StepID,
                            func_body: Callable) -> None:
        step_dir = self._workflow_root_dir / workflow_id / STEPS_DIR / step_id
        with _open_atomic(step_dir / STEP_FUNC_BODY, "wb") as f:
            ray.cloudpickle.dump(func_body, f)

    def load_step_args(self, workflow_id: str, step_id: StepID) -> ArgsType:
        step_dir = self._workflow_root_dir / workflow_id / STEPS_DIR / step_id
        with _open_atomic(step_dir / STEP_ARGS, "rb") as f:
            return ray.cloudpickle.load(f)

    def dump_step_args(self, workflow_id: str, step_id: StepID,
                       args: ArgsType) -> None:
        step_dir = self._workflow_root_dir / workflow_id / STEPS_DIR / step_id
        with _open_atomic(step_dir / STEP_ARGS, "wb") as f:
            ray.cloudpickle.dump(args, f)

    def load_object_ref(self, workflow_id: str, object_id) -> ray.ObjectRef:
        objects_dir = self._workflow_root_dir / workflow_id / OBJECTS_DIR
        with _open_atomic(objects_dir / object_id, "rb") as f:
            obj = ray.cloudpickle.load(f)
        return ray.put(obj)  # simulate an ObjectRef

    def dump_object_ref(self, workflow_id: str, rref: ray.ObjectRef) -> None:
        objects_dir = self._workflow_root_dir / workflow_id / OBJECTS_DIR
        obj = ray.get(rref)
        with _open_atomic(objects_dir / rref.hex(), "wb") as f:
            ray.cloudpickle.dump(obj, f)

    def get_entrypoint_step_id(self, workflow_id: str) -> StepID:
        steps_dir = self._workflow_root_dir / workflow_id / STEPS_DIR
        step_id = self._locate_output_step(steps_dir)
        if not isinstance(step_id, str):
            raise ValueError("Cannot locate workflow entrypoint step.")
        return step_id

    def _locate_output_step(self, step_dir: pathlib.Path) -> Optional[StepID]:
        """Locate where the output comes from with the given step dir.

        Args:
            step_dir: The path to search answers.

        Returns
            If found, return the ID of the step that produce results.
            Otherwise return None.
        """
        # read outputs forward file (take a shortcut)
        try:
            with _open_atomic(step_dir / STEP_OUTPUTS_FORWARD) as f:
                metadata = json.load(f)
            return metadata["step_id"]
        except (FileNotFoundError, json.JSONDecodeError):
            pass
        # read outputs metadata
        try:
            with _open_atomic(step_dir / STEP_OUTPUTS_METADATA) as f:
                metadata = json.load(f)
            return metadata["step_id"]
        except (FileNotFoundError, json.JSONDecodeError):
            return None

    def inspect_step(self, workflow_id: str,
                     step_id: StepID) -> StepInspectResult:
        step_dir = self._workflow_root_dir / workflow_id / STEPS_DIR / step_id
        if not step_dir.exists():
            return StepInspectResult()

        # does this step contains output checkpoint file?
        if _file_exists(step_dir / STEP_OUTPUT):
            return StepInspectResult(output_object_valid=True)

        # do we know where the output comes from?
        output_step_id = self._locate_output_step(step_dir)
        if output_step_id is not None:
            return StepInspectResult(output_step_id=output_step_id)

        # read inputs metadata
        try:
            with _open_atomic(step_dir / STEP_INPUTS_METADATA) as f:
                metadata = json.load(f)
            input_object_refs = metadata["object_refs"]
            input_workflows = metadata["workflows"]
        except (FileNotFoundError, json.JSONDecodeError):
            input_object_refs = None
            input_workflows = None
        return StepInspectResult(
            args_valid=_file_exists(step_dir / STEP_ARGS),
            func_body_valid=_file_exists(step_dir / STEP_FUNC_BODY),
            object_refs=input_object_refs,
            workflows=input_workflows,
        )

    def validate_workflow(self, workflow_id: str) -> None:
        workflow_dir = self._workflow_root_dir / workflow_id
        if not workflow_dir.exists():
            raise ValueError(f"Cannot find the workflow job '{workflow_dir}'")
        steps_dir = workflow_dir / STEPS_DIR
        if not (workflow_dir / STEPS_DIR).exists():
            raise ValueError(f"The workflow job record is invalid: {steps_dir}"
                             " not found.")
