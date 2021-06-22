"""
This module is higher-level abstraction of storage directly used by
workflows.
"""

from typing import Dict, List, Optional, Any, Callable, Tuple, Union

from dataclasses import dataclass

import ray
from ray.experimental.workflow import storage
from ray.experimental.workflow.common import Workflow, WorkflowInputs, StepID
from ray.experimental.workflow import workflow_context
from ray.experimental.workflow import serialization_context


@dataclass
class StepInspectResult:
    # The step output checkpoint exists and valid. If this field
    # is set, we do not set all other fields below.
    output_object_valid: bool = False
    # The ID of the step that could contain the output checkpoint of this
    # step. If this field is set, we do not set all other fields below.
    output_step_id: Optional[StepID] = None
    # The step input arguments checkpoint exists and valid.
    args_valid: bool = False
    # The step function body checkpoint exists and valid.
    func_body_valid: bool = False
    # The object refs in the inputs of the workflow.
    object_refs: Optional[List[str]] = None
    # The workflows in the inputs of the workflow.
    workflows: Optional[List[str]] = None

    def is_recoverable(self) -> bool:
        return (self.output_object_valid or self.output_step_id
                or (self.args_valid and self.object_refs is not None
                    and self.workflows is not None and self.func_body_valid))


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
        self._workflow_id = workflow_id

    def load_step_output(self, step_id: StepID) -> Any:
        """Load the output of the workflow step from checkpoint.

        Args:
            step_id: ID of the workflow step.

        Returns:
            Output of the workflow step.
        """
        return self._storage.load_step_output(self._workflow_id, step_id)

    def load_step_func_body(self, step_id: StepID) -> Callable:
        """Load the function body of the workflow step.

        Args:
            step_id: ID of the workflow step.

        Returns:
            A callable function.
        """
        return self._storage.load_step_func_body(self._workflow_id, step_id)

    def load_step_args(
            self, step_id: StepID, workflows: List[Any],
            object_refs: List[ray.ObjectRef]) -> Tuple[List, Dict[str, Any]]:
        """Load the input arguments of the workflow step. This must be
        done under a serialization context, otherwise the arguments would
        not be reconstructed successfully.

        Args:
            step_id: ID of the workflow step.
            workflows: The workflows in the original arguments,
                replaced by the actual workflow outputs.
            object_refs: The object refs in the original arguments.

        Returns:
            Args and kwargs.
        """
        with serialization_context.workflow_args_resolving_context(
                workflows, object_refs):
            return self._storage.load_step_args(self._workflow_id, step_id)

    def load_object_ref(self, object_id: str) -> ray.ObjectRef:
        """Load the input object ref.

        Args:
            object_id: The hex ObjectID.

        Returns:
            The object ref.
        """
        return self._storage.load_object_ref(self._workflow_id, object_id)

    def _update_dynamic_output(self, outer_most_step_id: StepID,
                               dynamic_output_step_id: StepID) -> None:
        """Update dynamic output.

        There are two steps involved:
        1. outer_most_step[id=outer_most_step_id]
        2. dynamic_step[id=dynamic_output_step_id]

        Initially, the output of outer_most_step comes from its *direct*
        nested step. However, the nested step could also have a nested
        step inside it (unknown currently and could be generated
        dynamically in the future). We call such steps "dynamic_step".

        When the dynamic_step produces its output, the output of
        outer_most_step can be updated and point to the dynamic_step. This
        creates a "shortcut" and accelerate workflow resuming. This is
        critical for scalability of virtual actors.

        Args:
            outer_most_step_id: ID of outer_most_step. See
                "workflow_manager.postprocess_workflow_step" for explanation.
            dynamic_output_step_id: ID of dynamic_step.
        """
        metadata = self._storage.load_step_output_metadata(
            self._workflow_id, outer_most_step_id)
        if (dynamic_output_step_id != metadata["output_step_id"]
                and dynamic_output_step_id !=
                metadata.get("dynamic_output_step_id")):
            metadata["dynamic_output_step_id"] = dynamic_output_step_id
            self._storage.save_step_output_metadata(
                self._workflow_id, outer_most_step_id, metadata)

    def _locate_output_step_id(self, step_id: StepID):
        metadata = self._storage.load_step_output_metadata(
            self._workflow_id, step_id)
        return (metadata.get("dynamic_output_step_id")
                or metadata["output_step_id"])

    def get_entrypoint_step_id(self) -> StepID:
        """Load the entrypoint step ID of the workflow.

        Returns:
            The ID of the entrypoint step.
        """
        # empty StepID represents the workflow driver
        try:
            return self._locate_output_step_id("")
        except Exception as e:
            raise ValueError("Fail to get entrypoint step ID from workflow"
                             f"[id={self._workflow_id}]") from e

    def inspect_step(self, step_id: StepID) -> StepInspectResult:
        """
        Get the status of a workflow step. The status indicates whether
        the workflow step can be recovered etc.

        Args:
            step_id: The ID of a workflow step

        Returns:
            The status of the step.
        """
        field_list = self._storage.get_step_status(self._workflow_id, step_id)
        # does this step contains output checkpoint file?
        if field_list.output_object_exists:
            return StepInspectResult(output_object_valid=True)
        # do we know where the output comes from?
        if field_list.output_metadata_exists:
            return StepInspectResult(
                output_step_id=self._locate_output_step_id(step_id))

        # read inputs metadata
        try:
            metadata = self._storage.load_step_input_metadata(
                self._workflow_id, step_id)
            input_object_refs = metadata["object_refs"]
            input_workflows = metadata["workflows"]
        except storage.DataLoadError:
            input_object_refs = None
            input_workflows = None
        return StepInspectResult(
            args_valid=field_list.args_exists,
            func_body_valid=field_list.func_body_exists,
            object_refs=input_object_refs,
            workflows=input_workflows,
        )

    def _write_step_inputs(self, step_id: StepID, inputs: WorkflowInputs):
        """Save workflow inputs."""
        f = inputs.func_body
        metadata = {
            "name": f.__module__ + "." + f.__qualname__,
            "object_refs": inputs.object_refs,
            "workflows": inputs.workflows,
        }
        with serialization_context.workflow_args_keeping_context():
            # TODO(suquark): in the future we should write to storage directly
            # with plasma store object in memory.
            args_obj = ray.get(inputs.args)
        self._storage.save_step_input_metadata(self._workflow_id, step_id,
                                               metadata)
        self._storage.save_step_func_body(self._workflow_id, step_id, f)
        self._storage.save_step_args(self._workflow_id, step_id, args_obj)

    def commit_step(self, step_id: StepID, ret: Union[Workflow, Any],
                    outer_most_step_id: Optional[StepID]) -> None:
        """When a workflow step returns,
        1. If the returned object is a workflow, this means we are a nested
           workflow. We save the DAG of the nested workflow.
        2. Otherwise, checkpoint the output.

        Args:
            step_id: The ID of the workflow step. If it is an empty string,
                it means we are in the workflow job driver process.
            ret: The returned object from a workflow step.
            outer_most_step_id: See
                "workflow_manager.postprocess_workflow_step" for explanation.
        """
        if isinstance(ret, Workflow):
            # This workflow step returns a nested workflow.
            assert not ret.executed
            if not ret.skip_saving_workflow_dag:
                for w in ret.iter_workflows_in_dag():
                    self._write_step_inputs(w.id, w.get_inputs())
                assert step_id != ret.id
                self._storage.save_step_output_metadata(
                    self._workflow_id, step_id, {"output_step_id": ret.id})
            dynamic_output_id = ret.id
        else:
            # This workflow step returns a object.
            ret = ray.get(ret) if isinstance(ret, ray.ObjectRef) else ret
            self._storage.save_step_output(self._workflow_id, step_id, ret)
            dynamic_output_id = step_id
        if outer_most_step_id is not None:
            self._update_dynamic_output(outer_most_step_id, dynamic_output_id)
