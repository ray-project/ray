import abc
from abc import abstractmethod

from dataclasses import dataclass

import ray
from ray.experimental.workflow.common import StepID
from typing import Any, Callable, List, Dict, Tuple

ArgsType = Tuple[List[Any], Dict[str, Any]]  # args and kwargs


class DataLoadError(Exception):
    pass


class DataSaveError(Exception):
    pass


@dataclass
class StepStatus:
    # does the step output checkpoint exist?
    output_object_exists: bool
    # does the step output metadata exist?
    output_metadata_exists: bool
    # does the step input metadata exist?
    input_metadata_exists: bool
    # does the step input argument exist?
    args_exists: bool
    # does the step function body exist?
    func_body_exists: bool


class Storage(metaclass=abc.ABCMeta):
    """Abstract base class for the low-level workflow storage.
    This class only provides low level primitives, e.g. save a certain
    type of object.
    """

    @abstractmethod
    def load_step_input_metadata(self, workflow_id: str,
                                 step_id: StepID) -> Dict[str, Any]:
        """Load the input metadata of a step.

        Args:
            workflow_id: ID of the workflow job.
            step_id: ID of the step.

        Raises:
            DataLoadError: if we fail to load the metadata.

        Returns:
            A metadata dict.
        """

    @abstractmethod
    def save_step_input_metadata(self, workflow_id: str, step_id: StepID,
                                 metadata: Dict[str, Any]) -> None:
        """Save the input metadata of a step.

        Args:
            workflow_id: ID of the workflow job.
            step_id: ID of the step.
            metadata: A metadata dict.

        Raises:
            DataSaveError: if we fail to save the metadata.
        """

    @abstractmethod
    def load_step_output_metadata(self, workflow_id: str,
                                  step_id: StepID) -> Dict[str, Any]:
        """Load the output metadata of a step.

        Args:
            workflow_id: ID of the workflow job.
            step_id: ID of the step.

        Raises:
            DataLoadError: if we fail to load the metadata.

        Returns:
            A metadata dict.
        """

    @abstractmethod
    def save_step_output_metadata(self, workflow_id: str, step_id: StepID,
                                  metadata: Dict[str, Any]) -> None:
        """Save the output metadata of a step.

        Args:
            workflow_id: ID of the workflow job.
            step_id: ID of the step.
            metadata: A metadata dict.

        Raises:
            DataSaveError: if we fail to save the metadata.
        """

    @abstractmethod
    def load_step_output(self, workflow_id: str, step_id: StepID) -> Any:
        """Load the output of the workflow step from checkpoint.

        Args:
            workflow_id: ID of the workflow job.
            step_id: ID of the workflow step.

        Raises:
            DataLoadError: if we fail to load the output.

        Returns:
            Output of the workflow step.
        """

    @abstractmethod
    def save_step_output(self, workflow_id: str, step_id: StepID,
                         output: Any) -> None:
        """Save the output of a workflow step.

        Args:
            workflow_id: ID of the workflow job.
            output: The output object.

        Raises:
            DataSaveError: if we fail to save the output.
        """

    @abstractmethod
    def load_step_func_body(self, workflow_id: str,
                            step_id: StepID) -> Callable:
        """Load the function body of the workflow step.

        Args:
            workflow_id: ID of the workflow job.
            step_id: ID of the workflow step.

        Raises:
            DataLoadError: if we fail to load the function body.

        Returns:
            A callable function.
        """

    @abstractmethod
    def save_step_func_body(self, workflow_id: str, step_id: StepID,
                            func_body: Callable) -> None:
        """Save the function body of the workflow step.

        Args:
            workflow_id: ID of the workflow job.
            step_id: ID of the workflow step.
            func_body: The step function to be written.

        Raises:
            DataSaveError: if we fail to save the function body.
        """

    @abstractmethod
    def load_step_args(self, workflow_id: str, step_id: StepID) -> ArgsType:
        """Load the input arguments of the workflow step. This must be
        done under a serialization context, otherwise the arguments would
        not be reconstructed successfully.

        Args:
            workflow_id: ID of the workflow job.
            step_id: ID of the workflow step.

        Raises:
            DataLoadError: if we fail to load the arguments.

        Returns:
            Args and kwargs.
        """

    @abstractmethod
    def save_step_args(self, workflow_id: str, step_id: StepID,
                       args: ArgsType) -> None:
        """Save the function body of the workflow step.

        Args:
            workflow_id: ID of the workflow job.
            step_id: ID of the workflow step.
            args: The step input args to be written.

        Raises:
            DataSaveError: if we fail to save the arguments.
        """

    @abstractmethod
    def load_object_ref(self, workflow_id: str,
                        object_id: str) -> ray.ObjectRef:
        """Load the input object ref.

        Args:
            workflow_id: ID of the workflow job.
            object_id: The hex ObjectID.

        Raises:
            DataLoadError: if we fail to load the object ref.

        Returns:
            The object ref.
        """

    @abstractmethod
    def save_object_ref(self, workflow_id: str, rref: ray.ObjectRef) -> None:
        """Save the input object ref.

        Args:
            workflow_id: ID of the workflow job.
            rref: The ObjectRef to be saved.

        Raises:
            DataSaveError: if we fail to save the object ref.
        """

    @abstractmethod
    def get_step_status(self, workflow_id: str, step_id: StepID) -> StepStatus:
        """Check the status of a step in the storage.

        Args:
            workflow_id: ID of the workflow job.
            step_id: ID of the step.

        Returns:
            A dataclass of the step fields.
        """

    @property
    @abstractmethod
    def storage_url(self) -> str:
        """Get the URL of the storage."""
