import abc
from abc import abstractmethod

from dataclasses import dataclass

import ray
from ray.experimental.workflow.common import StepID
from typing import Any, Callable, Optional, List, Dict, Tuple

ArgsType = Tuple[List[Any], Dict[str, Any]]  # args and kwargs


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
        return (self.args_valid and self.object_refs is not None
                and self.workflows is not None and self.func_body_valid)


class Storage(metaclass=abc.ABCMeta):
    @abstractmethod
    def load_step_input_metadata(self, workflow_id: str,
                                 step_id: StepID) -> Dict[str, Any]:
        """Dump the input metadata of a step.

        Args:
            workflow_id: ID of the workflow job.
            step_id: ID of the step.

        Returns:
            A metadata dict.
        """

    @abstractmethod
    def dump_step_input_metadata(self, workflow_id: str, step_id: StepID,
                                 metadata: Dict[str, Any]) -> None:
        """Dump the input metadata of a step.

        Args:
            workflow_id: ID of the workflow job.
            step_id: ID of the step.
            metadata: A metadata dict.
        """

    @abstractmethod
    def load_step_output_metadata(self, workflow_id: str,
                                  step_id: StepID) -> Dict[str, Any]:
        """Dump the output metadata of a step.

        Args:
            workflow_id: ID of the workflow job.
            step_id: ID of the step.

        Returns:
            A metadata dict.
        """

    @abstractmethod
    def dump_step_output_metadata(self, workflow_id: str, step_id: StepID,
                                  metadata: Dict[str, Any]) -> None:
        """Dump the output metadata of a step.

        Args:
            workflow_id: ID of the workflow job.
            step_id: ID of the step.
            metadata: A metadata dict.
        """

    @abstractmethod
    def update_output_forward(self, workflow_id: str,
                              forward_output_to: StepID,
                              output_step_id: StepID) -> None:
        """Update output forward. The output of 'output_step_id' should
        forward to the step 'forward_output_to'. When resume from
        'forward_output_to' step, that step can directly read
        the output of 'output_step_id'.

        Args:
            workflow_id: ID of the workflow job.
            forward_output_to: step 'forward_output_to'.
            output_step_id: step 'output_step_id'.
        """
        pass

    @abstractmethod
    def load_step_output(self, workflow_id: str, step_id: StepID) -> Any:
        """Load the output of the workflow step from checkpoint.

        Args:
            workflow_id: ID of the workflow job.
            step_id: ID of the workflow step.

        Returns:
            Output of the workflow step.
        """
        pass

    @abstractmethod
    def dump_step_output(self, workflow_id: str, step_id: StepID,
                         output: Any) -> None:
        """Dump the output of a workflow step.

        Args:
            workflow_id: ID of the workflow job.
            output: The output object.
        """
        pass

    @abstractmethod
    def load_step_func_body(self, workflow_id: str,
                            step_id: StepID) -> Callable:
        """Load the function body of the workflow step.

        Args:
            workflow_id: ID of the workflow job.
            step_id: ID of the workflow step.

        Returns:
            A callable function.
        """

    @abstractmethod
    def dump_step_func_body(self, workflow_id: str, step_id: StepID,
                            func_body: Callable) -> None:
        """Get the function body of the workflow step.

        Args:
            workflow_id: ID of the workflow job.
            step_id: ID of the workflow step.
            func_body: The step function to be written.
        """
        pass

    @abstractmethod
    def load_step_args(self, workflow_id: str, step_id: StepID) -> ArgsType:
        """Load the input arguments of the workflow step. This must be
        done under a serialization context, otherwise the arguments would
        not be reconstructed successfully.

        Args:
            workflow_id: ID of the workflow job.
            step_id: ID of the workflow step.

        Returns:
            Args and kwargs.
        """
        pass

    @abstractmethod
    def dump_step_args(self, workflow_id: str, step_id: StepID,
                       args: ArgsType) -> None:
        """Get the function body of the workflow step.

        Args:
            workflow_id: ID of the workflow job.
            step_id: ID of the workflow step.
            args: The step input args to be written.
        """
        pass

    @abstractmethod
    def load_object_ref(self, workflow_id: str,
                        object_id: str) -> ray.ObjectRef:
        """Load the input object ref.

        Args:
            workflow_id: ID of the workflow job.
            object_id: The hex ObjectID.
        Returns:
            The object ref.
        """
        pass

    @abstractmethod
    def dump_object_ref(self, workflow_id: str, rref: ray.ObjectRef) -> None:
        """Dump the input object ref.

        Args:
            workflow_id: ID of the workflow job.
            rref: The ObjectRef to be saved.
        """
        pass

    @abstractmethod
    def get_entrypoint_step_id(self, workflow_id: str) -> StepID:
        """Get the entrypoint step ID of the workflow.

        Args:
            workflow_id: ID of the workflow job.

        Returns:
            The ID of the entrypoint step.
        """
        pass

    @abstractmethod
    def inspect_step(self, workflow_id: str,
                     step_id: StepID) -> StepInspectResult:
        """
        Get the status of a workflow step. The status indicates whether
        the workflow step can be recovered etc.

        Args:
            workflow_id: ID of the workflow job.
            step_id: The ID of a workflow step

        Returns:
            The status of the step.
        """
        pass

    @abstractmethod
    def validate_workflow(self, workflow_id: str) -> None:
        """Check if the workflow structured correctly.

        Args:
            workflow_id: ID of the workflow job.
        """
        pass
