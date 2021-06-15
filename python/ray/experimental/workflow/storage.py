import json
import pathlib
from typing import Dict, List, Optional, Any, Set
import uuid

from dataclasses import dataclass

import ray
import ray.cloudpickle

from ray.experimental.workflow.common import Workflow, WorkflowInputs
from ray.experimental.workflow import workflow_context
from ray.experimental.workflow import serialization_context
from ray.experimental.workflow import configs

from ray.experimental.workflow.constants import (
    STEPS_DIR, OBJECTS_DIR, STEP_INPUTS_METADATA, STEP_OUTPUTS_METADATA,
    STEP_FUNC_BODY, STEP_ARGS, STEP_OUTPUT, STEP_OUTPUTS_FORWARD)

# TODO(suquark): in the future we may use general URLs to support
# different storages, e.g. "s3://xxxx/xxx". Currently we just use
# 'pathlib.Path' for convenience.


class WorkflowStorage:
    def __init__(self, workflow_root_dir: pathlib.Path):
        self._workflow_root_dir = workflow_root_dir

    def _write_atomic(self, writer, mode, obj, path: pathlib.Path, overwrite):
        # TODO(suquark): race conditions like two processes writing the
        # same file is still not safe. This may not be an issue, because
        # in our current implementation, we only need to guarantee the
        # file is either fully written or not existing.
        fullpath = self._workflow_root_dir / path
        if fullpath.exists() and not overwrite:
            raise FileExistsError(fullpath)
        tmp_new_filename = fullpath.with_suffix(fullpath.suffix + "." +
                                                uuid.uuid4().hex)
        with open(tmp_new_filename, mode) as f:
            writer(obj, f)
        if fullpath.exists():
            backup_path = fullpath.with_suffix(fullpath.suffix + ".backup")
            if backup_path.exists():
                backup_path.unlink()
            fullpath.rename(backup_path)
        tmp_new_filename.rename(fullpath)

    def _read_object(self, reader, mode, path):
        fullpath = self._workflow_root_dir / path
        if fullpath.exists():
            with open(fullpath, mode) as f:
                return reader(f)
        backup_path = fullpath.with_suffix(fullpath.suffix + ".backup")
        if backup_path.exists():
            with open(backup_path, mode) as f:
                return reader(f)
        raise FileNotFoundError(fullpath)

    def file_exists(self, path):
        fullpath = self._workflow_root_dir / path
        backup_path = fullpath.with_suffix(fullpath.suffix + ".backup")
        return fullpath.exists() or backup_path.exists()

    def write_object_atomic(self, obj, path: pathlib.Path, overwrite=True):
        self._write_atomic(ray.cloudpickle.dump, "wb", obj, path, overwrite)

    def write_json_atomic(self, json_obj, path: pathlib.Path, overwrite=True):
        self._write_atomic(json.dump, "w", json_obj, path, overwrite)

    def read_object(self, path: pathlib.Path):
        return self._read_object(ray.cloudpickle.load, "rb", path)

    def read_json(self, path: pathlib.Path):
        return self._read_object(json.load, "r", path)

    def create_directory(self, path: pathlib.Path, parents=True,
                         exist_ok=True):
        dir_path = self._workflow_root_dir / path
        dir_path.mkdir(parents=parents, exist_ok=exist_ok)


class WorkflowStepLogger:
    def __init__(self, step_id: Optional[str] = None):
        context = workflow_context.get_workflow_step_context()
        self._storage = WorkflowStorage(context.workflow_root_dir)
        self._workflow_dir = pathlib.Path(context.workflow_id)
        all_steps_dir: pathlib.Path = self._workflow_dir / STEPS_DIR
        objects_dir: pathlib.Path = self._workflow_dir / OBJECTS_DIR

        if step_id is None:
            scope = workflow_context.get_scope()
            if scope:
                step_dir = all_steps_dir / scope[-1]
            else:
                step_dir = all_steps_dir
        else:
            step_dir = all_steps_dir / step_id
        self._storage.create_directory(step_dir)
        self._objects_dir = objects_dir
        self._step_dir = step_dir

    def save_inputs(self, inputs: WorkflowInputs):
        args = inputs.args
        f = inputs.func_body
        metadata = {
            "step_id": inputs.step_id,
            "name": f.__module__ + "." + f.__qualname__,
            "object_refs": inputs.object_refs,
            "workflows": inputs.workflows,
        }
        with serialization_context.workflow_args_keeping_context():
            # TODO(suquark): in the future we should write to storage directly
            # with plasma store object in memory.
            args_obj = ray.get(args)
        self._storage.write_json_atomic(metadata,
                                        self._step_dir / STEP_INPUTS_METADATA)
        self._storage.write_object_atomic(f, self._step_dir / STEP_FUNC_BODY)
        self._storage.write_object_atomic(args_obj, self._step_dir / STEP_ARGS)

    def save_output(self, output: Any):
        self._storage.write_object_atomic(output, self._step_dir / STEP_OUTPUT)

    def save_outputs_metadata(self, metadata: Dict[str, Any]):
        self._storage.write_json_atomic(metadata,
                                        self._step_dir / STEP_OUTPUTS_METADATA)

    def update_output_forward(self, forward_output_to: str,
                              output_step_id: str):
        if forward_output_to == "":
            # actually it equals to 'self._workflow_dir / STEPS_DIR / ""',
            # but we won't use the trick here because it is not obvious.
            target_dir = self._workflow_dir / STEPS_DIR
        else:
            target_dir = self._workflow_dir / STEPS_DIR / forward_output_to
        self._storage.write_json_atomic({
            "step_id": output_step_id
        }, target_dir / STEP_OUTPUTS_FORWARD)


def save_workflow_dag(workflow: Workflow, forward_output_to: Optional[str]):
    assert not workflow.executed
    workflows: Set[Workflow] = set()
    workflow._visit_workflow_dag(workflows)
    for w in workflows:
        if not w.skip_saving_inputs:
            step_logger = WorkflowStepLogger(w.id)
            step_logger.save_inputs(w.get_inputs())
    step_logger = WorkflowStepLogger()
    step_logger.save_outputs_metadata({
        "step_id": workflow.id,
    })
    if forward_output_to is not None:
        step_logger.update_output_forward(forward_output_to, workflow.id)


def save_workflow_output(output: Any, forward_output_to: Optional[str]):
    if isinstance(output, ray.ObjectRef):
        obj = ray.get(output)
    else:
        obj = output
    step_logger = WorkflowStepLogger()
    step_logger.save_output(obj)
    if forward_output_to is not None:
        step_logger.update_output_forward(
            forward_output_to, workflow_context.get_current_step_id())


@dataclass
class StepInspectResult:
    args_valid: bool = False
    func_body_valid: bool = False
    object_refs: Optional[List[str]] = None
    workflows: Optional[List[str]] = None
    output_object_valid: bool = False
    output_step_id: Optional[str] = None

    def is_recoverable(self) -> bool:
        return (self.args_valid and self.object_refs is not None
                and self.workflows is not None and self.func_body_valid)


class WorkflowStorageReader:
    """
    Access workflow from storage.
    """

    def __init__(self, job_id: str, workflow_root_dir: Optional[str] = None):
        if workflow_root_dir is not None:
            self._workflow_root_dir = pathlib.Path(workflow_root_dir)
        else:
            self._workflow_root_dir = configs.get_default_workflow_root_dir()
        self._storage = WorkflowStorage(self._workflow_root_dir)
        self._job_id = job_id
        job_dir = self._workflow_root_dir / job_id
        if not job_dir.exists():
            raise ValueError(f"Cannot find the workflow job '{job_dir}'")
        steps_dir = job_dir / STEPS_DIR
        if not steps_dir.exists():
            raise ValueError(f"The workflow job record is invalid: {STEPS_DIR}"
                             " not found.")
        if not (steps_dir / STEP_OUTPUTS_METADATA).exists():
            raise ValueError(
                f"The workflow job record is invalid: {STEPS_DIR}/"
                f"{STEP_OUTPUTS_METADATA} not found.")
        try:
            with open(steps_dir / STEP_OUTPUTS_METADATA) as f:
                output_metadata = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse JSON {STEPS_DIR}/"
                             f"{STEP_OUTPUTS_METADATA}.") from e
        if not isinstance(output_metadata.get("step_id"), str):
            raise ValueError("Cannot locate workflow entrypoint step.")
        # NOTE: "objects_dir" may not exist. We do not check it.
        self._job_dir = job_dir
        self._steps_dir = pathlib.Path(job_dir) / STEPS_DIR
        self._objects_dir = pathlib.Path(job_dir) / OBJECTS_DIR
        self._entrypoint_step_id = output_metadata["step_id"]

    @property
    def entrypoint_step_id(self):
        return self._entrypoint_step_id

    @property
    def workflow_root_dir(self):
        return self._workflow_root_dir

    @property
    def job_id(self):
        return self._job_dir

    def get_object_path(self, object_id):
        return self._objects_dir / object_id

    def inspect_step(self, step_id: str) -> StepInspectResult:
        """
        Get the status of a workflow step. The status indicates whether
        the workflow step can be recovered etc.

        Args:
            step_id: The ID of a workflow step

        Returns:
            The status of the step.
        """
        step_dir = self._steps_dir / step_id
        if not step_dir.exists():
            return StepInspectResult()

        # read inputs metadata
        input_object_refs = None
        input_workflows = None
        try:
            metadata = self._storage.read_json(step_dir / STEP_INPUTS_METADATA)
            input_object_refs = metadata.get("object_refs")
            if not isinstance(input_object_refs, list):
                input_object_refs = None
            input_workflows = metadata.get("workflows")
            if not isinstance(input_workflows, list):
                input_workflows = None
        except (FileNotFoundError, json.JSONDecodeError):
            pass

        # read outputs metadata
        output_step_id = None
        try:
            metadata = self._storage.read_json(
                step_dir / STEP_OUTPUTS_METADATA)
            output_step_id = metadata.get("step_id")
            if not isinstance(output_step_id, str):
                output_step_id = None
        except (FileNotFoundError, json.JSONDecodeError):
            pass

        return StepInspectResult(
            args_valid=self._storage.file_exists(step_dir / STEP_ARGS),
            object_refs=input_object_refs,
            workflows=input_workflows,
            func_body_valid=self._storage.file_exists(
                step_dir / STEP_FUNC_BODY),
            output_object_valid=self._storage.file_exists(
                step_dir / STEP_OUTPUT),
            output_step_id=output_step_id,
        )

    def read_object(self, object_id):
        object_file = self._objects_dir / object_id
        with open(object_file, "rb") as f:
            return ray.cloudpickle.load(f)


class WorkflowStepReader:
    """
    Access workflow step from storage.
    """

    def __init__(self, workflow_root_dir: pathlib.Path, workflow_id: str):
        self._storage = WorkflowStorage(workflow_root_dir)
        self._workflow_dir = pathlib.Path(workflow_id)
        self._steps_dir = self._workflow_dir / STEPS_DIR
        self._objects_dir = self._workflow_dir / OBJECTS_DIR

    def get_func_body(self, step_id: str):
        return self._storage.read_object(
            self._steps_dir / step_id / STEP_FUNC_BODY)

    def get_step_output(self, step_id: str):
        return self._storage.read_object(
            self._steps_dir / step_id / STEP_OUTPUT)

    def get_step_args(self, step_id: str):
        return self._storage.read_object(self._steps_dir / step_id / STEP_ARGS)

    def read_object_ref(self, object_id) -> ray.ObjectRef:
        object_file = self._objects_dir / object_id
        with open(object_file, "rb") as f:
            obj = ray.cloudpickle.load(f)
        return ray.put(obj)  # simulate an ObjectRef
