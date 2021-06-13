import json
import pathlib
from typing import Dict, List, Optional, Any, Set

from dataclasses import dataclass

import ray
import ray.cloudpickle

from ray.experimental.workflow.common import Workflow
from ray.experimental.workflow import checkpoint_result
from ray.experimental.workflow import workflow_context
from ray.experimental.workflow import configs

from ray.experimental.workflow.constants import (
    STEPS_DIR, TASK_BODY_FILE, OBJECTS_DIR, NON_BLOCKING_CHECKPOINTING,
    STEP_INPUTS_METADATA, STEP_OUTPUTS_METADATA)


def _get_current_store_dir():
    context = workflow_context.get_workflow_step_context()
    return context.workflow_root_dir / context.workflow_id


class WorkflowStepLogger:
    def __init__(self, step_id: Optional[str] = None):
        current_store_dir = _get_current_store_dir()
        all_steps_dir: pathlib.Path = current_store_dir / STEPS_DIR
        objects_dir = current_store_dir / OBJECTS_DIR
        if not objects_dir.exists():
            objects_dir.mkdir(parents=True, exist_ok=True)
        if step_id is None:
            scope = workflow_context.get_scope()
            if scope:
                step_dir = all_steps_dir / scope[-1]
            else:
                step_dir = all_steps_dir
        else:
            step_dir = all_steps_dir / step_id
        if not step_dir.exists():
            step_dir.mkdir(parents=True, exist_ok=False)
        self.objects_dir = objects_dir
        self.step_dir = step_dir

    def save_task_body(self, func):
        pickled_function = ray.cloudpickle.dumps(func)
        with open(self.step_dir / TASK_BODY_FILE, "wb") as f:
            f.write(pickled_function)

    def save_inputs_metadata(self, metadata: Dict[str, Any]):
        with open(self.step_dir / STEP_INPUTS_METADATA, "w") as f:
            json.dump(metadata, f, indent=4)

    def save_outputs_metadata(self, metadata: Dict[str, Any]):
        with open(self.step_dir / STEP_OUTPUTS_METADATA, "w") as f:
            json.dump(metadata, f, indent=4)

    def save_objects(self, object_refs: List[ray.ObjectRef]):
        checkpoint_result.checkpoint_refs(
            object_refs,
            self.objects_dir,
            nonblocking=NON_BLOCKING_CHECKPOINTING)


def _save_workflow_inputs(workflow: Workflow):
    step_logger = WorkflowStepLogger(workflow.id)
    step_logger.save_objects([workflow._input_placeholder])
    f = workflow._original_function
    step_logger.save_task_body(f)
    step_logger.save_inputs_metadata(workflow.get_metadata())


def save_workflow_dag(workflow: Workflow):
    assert not workflow.executed
    workflows: Set[Workflow] = set()
    workflow._visit_workflow_dag(workflows)
    for w in workflows:
        _save_workflow_inputs(w)
    step_logger = WorkflowStepLogger()
    step_logger.save_outputs_metadata({
        "output_type": "workflow",
        "step_id": workflow.id,
    })


def save_workflow_output(output: Any):
    if not isinstance(output, ray.ObjectRef):
        rref = ray.put(output)
    else:
        rref = output
    step_logger = WorkflowStepLogger()
    step_logger.save_objects([rref])
    step_logger.save_outputs_metadata({
        "output_type": "object",
        "object_id": rref.hex(),
    })


def _file_integrity_check(path: pathlib.Path) -> bool:
    if not path.exists():
        return False
    # TODO(suquark): check the digest file.
    return True


class StepInspectResult(dataclass):
    input_placeholder: Optional[str] = None
    input_placeholder_object_valid: bool = False
    input_object_refs: Optional[List[str]] = None
    input_workflows: Optional[List[str]] = None
    task_body_valid: bool = False
    output_type: Optional[str] = None
    output_object_id: Optional[str] = None
    output_object_valid: bool = False
    output_step_id: Optional[str] = None

    def is_recoverable(self) -> bool:
        return (self.input_placeholder_object_valid
                and self.input_object_refs is not None
                and self.input_workflows is not None and self.task_body_valid)


class WorkflowStorageReader:
    """
    Access workflow from storage.
    """

    def __init__(self, job_id: str, workflow_root_dir: Optional[str] = None):
        if workflow_root_dir is not None:
            job_dir = pathlib.Path(workflow_root_dir) / job_id
        else:
            job_dir = configs.get_default_workflow_root_dir() / job_id
        if not job_dir.exists():
            raise ValueError(f"Cannot find the workflow job '{job_dir}'")
        steps_dir = job_dir / STEPS_DIR
        objects_dir = job_dir / OBJECTS_DIR
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
        if (output_metadata.get("output_type") != "workflow"
                or not isinstance(output_metadata.get("step_id"), str)):
            raise ValueError("Cannot locate workflow entrypoint step.")
        if not objects_dir.exists():
            raise ValueError(
                f"The workflow job record is invalid: {OBJECTS_DIR}"
                " not found.")
        self._job_dir = job_dir
        self._steps_dir = steps_dir
        self._objects_dir = objects_dir
        self._entrypoint_step_id = output_metadata["step_id"]

    @property
    def entrypoint_step_id(self):
        return self._entrypoint_step_id

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
        input_placeholder = None
        input_object_refs = None
        input_workflows = None
        input_placeholder_object_valid = False
        try:
            with open(step_dir / STEP_INPUTS_METADATA) as f:
                metadata = json.load(f)
                input_placeholder = metadata.get("input_placeholder")
                if not isinstance(input_placeholder, str):
                    input_placeholder = None
                else:
                    input_placeholder_object_valid = _file_integrity_check(
                        self._objects_dir / input_placeholder)
                input_object_refs = metadata.get("input_object_refs")
                if not isinstance(input_object_refs, list):
                    input_object_refs = None
                input_workflows = metadata.get("input_workflows")
                if not isinstance(input_workflows, list):
                    input_workflows = None
        except (FileNotFoundError, json.JSONDecodeError):
            pass

        task_body_valid = _file_integrity_check(step_dir / TASK_BODY_FILE)

        # read outputs metadata
        output_type = None
        output_object_id = None
        output_object_valid = False
        output_step_id = None
        try:
            with open(step_dir / STEP_OUTPUTS_METADATA) as f:
                metadata = json.load(f)
                input_placeholder = metadata.get("output_type")
                if not isinstance(output_type, str):
                    output_type = None
                else:
                    if output_type == "object":
                        output_object_id = metadata.get("object_id")
                        if isinstance(output_object_id, str):
                            output_object_valid = _file_integrity_check(
                                self._objects_dir / output_object_id)
                        else:
                            output_object_id = None
                    elif output_type == "workflow":
                        output_step_id = metadata.get("step_id")
                        if not isinstance(output_step_id, str):
                            output_step_id = None
        except (FileNotFoundError, json.JSONDecodeError):
            pass

        return StepInspectResult(
            input_placeholder=input_placeholder,
            input_placeholder_object_valid=input_placeholder_object_valid,
            input_object_refs=input_object_refs,
            input_workflows=input_workflows,
            task_body_valid=task_body_valid,
            output_type=output_type,
            output_object_id=output_object_id,
            output_object_valid=output_object_valid,
            output_step_id=output_step_id,
        )

    def read_object(self, object_id):
        object_file = self._objects_dir / object_id
        with open(object_file, "rb") as f:
            return ray.cloudpickle.load(f)
