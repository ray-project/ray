import abc
from abc import abstractmethod

from dataclasses import dataclass
import functools

import ray
from ray.experimental.workflow.common import StepID
from typing import Any, Callable, List, Dict, Tuple

ArgsType = Tuple[List[Any], Dict[str, Any]]  # args and kwargs


class DataLoadError(Exception):
    pass


class DataSaveError(Exception):
    pass


def data_load_error(func):
    @functools.wraps(func)
    async def _func(*args, **kvargs):
        try:
            ret = await func(*args, **kvargs)
            return ret
        except Exception as e:
            raise DataLoadError from e

    return _func


def data_save_error(func):
    @functools.wraps(func)
    async def _func(*args, **kv_args):
        try:
            ret = await func(*args, **kv_args)
            return ret
        except Exception as e:
            raise DataSaveError from e

    return _func


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
    async def load_step_input_metadata(self, workflow_id: str,
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
    async def save_step_input_metadata(self, workflow_id: str, step_id: StepID,
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
    async def load_step_output_metadata(self, workflow_id: str,
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
    async def save_step_output_metadata(self, workflow_id: str,
                                        step_id: StepID,
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
    async def load_step_output(self, workflow_id: str, step_id: StepID) -> Any:
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
    async def save_step_output(self, workflow_id: str, step_id: StepID,
                               output: Any) -> None:
        """Save the output of a workflow step.

        Args:
            workflow_id: ID of the workflow job.
            output: The output object.

        Raises:
            DataSaveError: if we fail to save the output.
        """

    @abstractmethod
    async def load_step_func_body(self, workflow_id: str,
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
    async def save_step_func_body(self, workflow_id: str, step_id: StepID,
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
    async def load_step_args(self, workflow_id: str,
                             step_id: StepID) -> ArgsType:
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
    async def save_step_args(self, workflow_id: str, step_id: StepID,
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
    async def load_object_ref(self, workflow_id: str,
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
    async def save_object_ref(self, workflow_id: str,
                              obj_ref: ray.ObjectRef) -> None:
        """Save the input object ref.

        Args:
            workflow_id: ID of the workflow job.
            obj_ref: The ObjectRef to be saved.

        Raises:
            DataSaveError: if we fail to save the object ref.
        """

    @abstractmethod
    async def get_step_status(self, workflow_id: str,
                              step_id: StepID) -> StepStatus:
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

    @abstractmethod
    async def load_actor_class_body(self, workflow_id: str) -> type:
        """Load the class body of the virtual actor.

        Args:
            workflow_id: ID of the workflow job.

        Raises:
            DataLoadError: if we fail to load the class body.
        """

    @abstractmethod
    async def save_actor_class_body(self, workflow_id: str, cls: type) -> None:
        """Save the class body of the virtual actor.

        Args:
            workflow_id: ID of the workflow job.
            cls: The class body used by the virtual actor.

        Raises:
            DataSaveError: if we fail to save the class body.
        """

    @abstractmethod
    async def save_workflow_meta(self, workflow_id: str,
                                 metadata: Dict[str, Any]) -> None:
        """Save the meta of the workflow.

        Args:
            workflow_id: ID of the workflow
            metadata: A metadata dict

        Raises:
            DataSaveError: if we fail to save the metadata.
        """

    @abstractmethod
    async def load_workflow_meta(self, workflow_id: str) -> Dict[str, Any]:
        """Load the meta of the workflow.

        Args:
            workflow_id: ID of the workflow

        Raises:
            DataLoadError: if we fail to load the metadata.

        Returns:
            A metadata dict
        """

    @abstractmethod
    async def list_workflow(self) -> List[str]:
        """List all the workflows inside the storage.

        Raises:
            DataLoadError: if we fail to load the metadata.

        Returns:
            A list of workflow ids
        """

    @abstractmethod
    async def load_workflow_progress(self, workflow_id: str) -> Dict[str, Any]:
        """Load the latest progress of a workflow. This is used by a
        virtual actor.

        Args:
            workflow_id: ID of the workflow job.

        Raises:
            DataLoadError: if we fail to load the progress.

        Returns:
            Metadata about the workflow progress.
        """

    @abstractmethod
    async def save_workflow_progress(self, workflow_id: str,
                                     metadata: Dict[str, Any]) -> None:
        """Save the latest progress of a workflow. This is used by a
        virtual actor.

        Args:
            workflow_id: ID of the workflow job.
            metadata: Metadata about the workflow progress.

        Raises:
            DataSaveError: if we fail to save the progress.
        """

    @abstractmethod
    def __reduce__(self):
        """Reduce the storage to a serializable object."""
