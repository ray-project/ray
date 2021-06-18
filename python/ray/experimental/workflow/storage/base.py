import abc
from abc import abstractmethod

from dataclasses import dataclass

import ray
from ray.experimental.workflow.common import StepID
from typing import Any, Callable, List, Dict, Tuple

ArgsType = Tuple[List[Any], Dict[str, Any]]  # args and kwargs


# TODO(suquark): I did not come up with a better name :(
@dataclass
class StepChecklist:
    # does the step output checkpoint exists?
    output_object_exists: bool
    # does the step output metadata exists?
    output_metadata_exists: bool
    # does the step input metadata exists?
    input_metadata_exists: bool
    # does the step input argument exists?
    args_exists: bool
    # does the step function body exists?
    func_body_exists: bool


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
    def load_step_output(self, workflow_id: str, step_id: StepID) -> Any:
        """Load the output of the workflow step from checkpoint.

        Args:
            workflow_id: ID of the workflow job.
            step_id: ID of the workflow step.

        Returns:
            Output of the workflow step.
        """

    @abstractmethod
    def dump_step_output(self, workflow_id: str, step_id: StepID,
                         output: Any) -> None:
        """Dump the output of a workflow step.

        Args:
            workflow_id: ID of the workflow job.
            output: The output object.
        """

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

    @abstractmethod
    def dump_step_args(self, workflow_id: str, step_id: StepID,
                       args: ArgsType) -> None:
        """Get the function body of the workflow step.

        Args:
            workflow_id: ID of the workflow job.
            step_id: ID of the workflow step.
            args: The step input args to be written.
        """

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

    @abstractmethod
    def dump_object_ref(self, workflow_id: str, rref: ray.ObjectRef) -> None:
        """Dump the input object ref.

        Args:
            workflow_id: ID of the workflow job.
            rref: The ObjectRef to be saved.
        """

    @abstractmethod
    def step_field_exists(self, workflow_id: str,
                          step_id: StepID) -> StepChecklist:
        """Check the existence of step fields in the storage.

        Args:
            workflow_id: ID of the workflow job.
            step_id: ID of the step.

        Returns:
            A dataclass of the step fields.
        """

    @abstractmethod
    def validate_workflow(self, workflow_id: str) -> None:
        """Check if the workflow structured correctly.

        Args:
            workflow_id: ID of the workflow job.
        """
