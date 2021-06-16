"""
This module is higher-level abstraction of storage directly used by
workflows.
"""

import json
import pathlib
from typing import Dict, List, Optional, Any, Set, Callable, Tuple, Union

from dataclasses import dataclass

import ray
from ray.experimental.workflow import storage
from ray.experimental.workflow.common import Workflow, WorkflowInputs, StepID
from ray.experimental.workflow import workflow_context
from ray.experimental.workflow import serialization_context

from ray.experimental.workflow.constants import (
    STEPS_DIR, OBJECTS_DIR, STEP_INPUTS_METADATA, STEP_OUTPUTS_METADATA,
    STEP_FUNC_BODY, STEP_ARGS, STEP_OUTPUT, STEP_OUTPUTS_FORWARD)


@dataclass
class StepInspectResult:
    args_valid: bool = False
    func_body_valid: bool = False
    object_refs: Optional[List[str]] = None
    workflows: Optional[List[str]] = None
    output_object_valid: bool = False
    output_step_id: Optional[StepID] = None

    def is_recoverable(self) -> bool:
        return (self.args_valid and self.object_refs is not None
                and self.workflows is not None and self.func_body_valid)


class WorkflowStorage:
    """Access workflow in storage. This is a high-level function,
    which does not care about the underlining storage implementation."""

    def __init__(self,
                 workflow_id: Optional[str] = None,
                 store: Optional[storage.Storage] = None):
        if workflow_id is None:
            context = workflow_context.get_workflow_step_context()
            workflow_id = context.workflow_id
        if store is None:
            store = storage.get_global_storage()
        self._storage = store
        self._workflow_dir = pathlib.Path(workflow_id)
        self._steps_dir = self._workflow_dir / STEPS_DIR
        self._objects_dir = self._workflow_dir / OBJECTS_DIR

    def write_step_inputs(self, step_id: StepID, inputs: WorkflowInputs):
        """Save workflow inputs."""
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
        step_dir = self._steps_dir / step_id
        if not self._storage.directory_exists(step_dir):
            self._storage.create_directory(step_dir)
        self._storage.write_json_atomic(metadata,
                                        step_dir / STEP_INPUTS_METADATA)
        self._storage.write_object_atomic(f, step_dir / STEP_FUNC_BODY)
        self._storage.write_object_atomic(args_obj, step_dir / STEP_ARGS)

    def write_step_output_metadata(self, step_id: StepID,
                                   metadata: Dict[str, Any]) -> None:
        """Save the output metadata json."""
        step_dir = self._steps_dir / step_id
        self._storage.write_json_atomic(metadata,
                                        step_dir / STEP_OUTPUTS_METADATA)

    def update_output_forward(self, forward_output_to: StepID,
                              output_step_id: StepID) -> None:
        """Update output forward. The output of 'output_step_id' should
        forward to the step 'forward_output_to'. When resume from
        'forward_output_to' step, that step can directly read
        the output of 'output_step_id'.

        Args:
            forward_output_to: step 'forward_output_to'.
            output_step_id: step 'output_step_id'.
        """
        if forward_output_to == "":
            # actually it equals to 'self._workflow_dir / STEPS_DIR / ""',
            # but we won't use the trick here because it is not obvious.
            target_dir = self._workflow_dir / STEPS_DIR
        else:
            target_dir = self._workflow_dir / STEPS_DIR / forward_output_to
        self._storage.write_json_atomic({
            "step_id": output_step_id
        }, target_dir / STEP_OUTPUTS_FORWARD)

    def write_step_output(self, step_id: StepID, output: Any) -> None:
        """Save the output of a workflow step.

        Args:
            output: The output object.
        """
        # convert
        obj = ray.get(output) if isinstance(output, ray.ObjectRef) else output
        step_dir = self._steps_dir / step_id
        self._storage.write_object_atomic(obj, step_dir / STEP_OUTPUT)

    def read_step_output(self, step_id: StepID) -> Any:
        """Get the output of the workflow step from checkpoint.

        Args:
            step_id: ID of the workflow step.

        Returns:
            Output of the workflow step.
        """
        return self._storage.read_object(
            self._steps_dir / step_id / STEP_OUTPUT)

    def read_step_func_body(self, step_id: StepID) -> Callable:
        """Get the function body of the workflow step.

        Args:
            step_id: ID of the workflow step.

        Returns:
            A callable function.
        """
        return self._storage.read_object(
            self._steps_dir / step_id / STEP_FUNC_BODY)

    def read_step_args(self, step_id: StepID) -> Tuple[List, Dict[str, Any]]:
        """Get the input arguments of the workflow step. This must be
        done under a serialization context, otherwise the arguments would
        not be reconstructed successfully.

        Args:
            step_id: ID of the workflow step.

        Returns:
            Args and kwargs.
        """
        return self._storage.read_object(self._steps_dir / step_id / STEP_ARGS)

    def read_object_ref(self, object_id) -> ray.ObjectRef:
        """Get the input object ref.

        Returns:
            The object ref.
        """
        obj = self._storage.read_object(self._objects_dir / object_id)
        return ray.put(obj)  # simulate an ObjectRef

    def get_entrypoint_step_id(self) -> StepID:
        step_id = self._locate_output_step(self._steps_dir)
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
            metadata = self._storage.read_json(step_dir / STEP_OUTPUTS_FORWARD)
            return metadata["step_id"]
        except (FileNotFoundError, json.JSONDecodeError):
            pass
        # read outputs metadata
        try:
            metadata = self._storage.read_json(
                step_dir / STEP_OUTPUTS_METADATA)
            return metadata["step_id"]
        except (FileNotFoundError, json.JSONDecodeError):
            return None

    def inspect_step(self, step_id: StepID) -> StepInspectResult:
        """
        Get the status of a workflow step. The status indicates whether
        the workflow step can be recovered etc.

        Args:
            step_id: The ID of a workflow step

        Returns:
            The status of the step.
        """
        step_dir = self._steps_dir / step_id
        if not self._storage.directory_exists(step_dir):
            return StepInspectResult()

        # does this step contains output checkpoint file?
        if self._storage.file_exists(step_dir / STEP_OUTPUT):
            return StepInspectResult(output_object_valid=True)

        # do we know where the output comes from?
        output_step_id = self._locate_output_step(step_dir)
        if output_step_id is not None:
            return StepInspectResult(output_step_id=output_step_id)

        # read inputs metadata
        try:
            metadata = self._storage.read_json(step_dir / STEP_INPUTS_METADATA)
            input_object_refs = metadata["object_refs"]
            input_workflows = metadata["workflows"]
        except (FileNotFoundError, json.JSONDecodeError):
            input_object_refs = None
            input_workflows = None
        return StepInspectResult(
            args_valid=self._storage.file_exists(step_dir / STEP_ARGS),
            object_refs=input_object_refs,
            workflows=input_workflows,
            func_body_valid=self._storage.file_exists(
                step_dir / STEP_FUNC_BODY),
        )

    def validate_workflow(self) -> None:
        """Check if the workflow structured correctly."""
        if not self._storage.directory_exists(self._workflow_dir):
            raise ValueError("Cannot find the workflow job "
                             f"'{self._workflow_dir}'")
        if not self._storage.directory_exists(self._steps_dir):
            raise ValueError(f"The workflow job record is invalid: {STEPS_DIR}"
                             " not found.")

    def commit_step(self, step_id: StepID, ret: Union[Workflow, Any],
                    forward_output_to: Optional[StepID]) -> None:
        """When a workflow step returns,
        1. If the returned object is a workflow, this means we are a nested
           workflow. We save the DAG of the nested workflow.
        2. Otherwise, checkpoint the

        Args:
            step_id: The ID of the workflow step. If it is an empty string,
                it means we are in the workflow job driver process.
            ret: The returned object from a workflow step.
            forward_output_to: The output should also forward to the step
                referred by 'forward_output_to'. When resume from that step,
                that step can directly read this output.
        """
        if isinstance(ret, Workflow):
            # The case for nested workflow.
            assert not ret.executed
            workflows: Set[Workflow] = set()
            ret._visit_workflow_dag(workflows)
            for w in workflows:
                if not w.skip_saving_inputs:
                    self.write_step_inputs(w.id, w.get_inputs())
            self.write_step_output_metadata(step_id, {"step_id": ret.id})
            forward_src = ret.id
        else:
            self.write_step_output(step_id, ret)
            forward_src = step_id
        if forward_output_to is not None:
            self.update_output_forward(forward_output_to, forward_src)
