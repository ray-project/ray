from abc import abstractmethod

import ray
from typing import Any, Dict, Callable
from ray.experimental.workflow.common import StepID
from ray.experimental.workflow.storage.base import (
    Storage, ArgsType, data_load_error, data_save_error)

# constants used in filesystem
OBJECTS_DIR = "objects"
STEPS_DIR = "steps"
STEP_INPUTS_METADATA = "inputs.json"
STEP_OUTPUTS_METADATA = "outputs.json"
STEP_ARGS = "args.pkl"
STEP_OUTPUT = "output.pkl"
STEP_FUNC_BODY = "func_body.pkl"
CLASS_BODY = "class_body.pkl"
WORKFLOW_META = "workflow_meta.json"
WORKFLOW_PROGRESS = "progress.json"


class PathBasedStorage(Storage):
    """Abstract storage based on path."""

    @abstractmethod
    def _get_path(self, *names: str) -> str:
        """Get path from name sections."""

    @abstractmethod
    async def _put_object(self, path: str, data: Any,
                          is_json: bool = False) -> None:
        """Put object into storage.

        Args:
            path: The path of the object.
            data: The object data.
            is_json: True if the object is a json object.
        """

    @abstractmethod
    async def _get_object(self, path: str, is_json: bool = False) -> Any:
        """Get object from storage.

        Args:
            path: The path of the object.
            is_json: True if the object is a json object.

        Returns:
            The object from storage.
        """

    @data_load_error
    async def load_step_input_metadata(self, workflow_id: str,
                                       step_id: StepID) -> Dict[str, Any]:
        path = self._get_path(workflow_id, STEPS_DIR, step_id,
                              STEP_INPUTS_METADATA)
        return await self._get_object(path, True)

    @data_save_error
    async def save_step_input_metadata(self, workflow_id: str, step_id: StepID,
                                       metadata: Dict[str, Any]) -> None:
        path = self._get_path(workflow_id, STEPS_DIR, step_id,
                              STEP_INPUTS_METADATA)
        await self._put_object(path, metadata, True)

    @data_load_error
    async def load_step_output_metadata(self, workflow_id: str,
                                        step_id: StepID) -> Dict[str, Any]:
        path = self._get_path(workflow_id, STEPS_DIR, step_id,
                              STEP_OUTPUTS_METADATA)
        data = await self._get_object(path, True)
        return data

    @data_save_error
    async def save_step_output_metadata(self, workflow_id: str,
                                        step_id: StepID,
                                        metadata: Dict[str, Any]) -> None:
        path = self._get_path(workflow_id, STEPS_DIR, step_id,
                              STEP_OUTPUTS_METADATA)
        await self._put_object(path, metadata, True)

    @data_load_error
    async def load_step_output(self, workflow_id: str, step_id: StepID) -> Any:
        path = self._get_path(workflow_id, STEPS_DIR, step_id, STEP_OUTPUT)
        return await self._get_object(path)

    @data_save_error
    async def save_step_output(self, workflow_id: str, step_id: StepID,
                               output: Any) -> None:
        path = self._get_path(workflow_id, STEPS_DIR, step_id, STEP_OUTPUT)
        await self._put_object(path, output)

    @data_load_error
    async def load_step_func_body(self, workflow_id: str,
                                  step_id: StepID) -> Callable:
        path = self._get_path(workflow_id, STEPS_DIR, step_id, STEP_FUNC_BODY)
        return await self._get_object(path)

    @data_save_error
    async def save_step_func_body(self, workflow_id: str, step_id: StepID,
                                  func_body: Callable) -> None:
        path = self._get_path(workflow_id, STEPS_DIR, step_id, STEP_FUNC_BODY)
        await self._put_object(path, func_body)

    @data_load_error
    async def load_step_args(self, workflow_id: str,
                             step_id: StepID) -> ArgsType:
        path = self._get_path(workflow_id, STEPS_DIR, step_id, STEP_ARGS)
        return await self._get_object(path)

    @data_save_error
    async def save_step_args(self, workflow_id: str, step_id: StepID,
                             args: ArgsType) -> None:
        path = self._get_path(workflow_id, STEPS_DIR, step_id, STEP_ARGS)
        await self._put_object(path, args)

    @data_load_error
    async def load_object_ref(self, workflow_id: str,
                              object_id) -> ray.ObjectRef:
        path = self._get_path(workflow_id, OBJECTS_DIR, object_id)
        data = await self._get_object(path)
        return ray.put(data)  # simulate an ObjectRef

    @data_save_error
    async def save_object_ref(self, workflow_id: str,
                              obj_ref: ray.ObjectRef) -> None:
        path = self._get_path(workflow_id, OBJECTS_DIR, obj_ref.hex())
        data = await obj_ref
        await self._put_object(path, data)

    @data_load_error
    async def load_actor_class_body(self, workflow_id: str) -> type:
        path = self._get_path(workflow_id, CLASS_BODY)
        return await self._get_object(path)

    @data_save_error
    async def save_actor_class_body(self, workflow_id: str, cls: type) -> None:
        path = self._get_path(workflow_id, CLASS_BODY)
        await self._put_object(path, cls)

    @data_save_error
    async def save_workflow_meta(self, workflow_id: str,
                                 metadata: Dict[str, Any]) -> None:
        path = self._get_path(workflow_id, WORKFLOW_META)
        await self._put_object(path, metadata, True)

    @data_load_error
    async def load_workflow_meta(self, workflow_id: str) -> Dict[str, Any]:
        path = self._get_path(workflow_id, WORKFLOW_META)
        return await self._get_object(path, True)

    @data_load_error
    async def load_workflow_progress(self, workflow_id: str) -> Dict[str, Any]:
        path = self._get_path(workflow_id, STEPS_DIR, WORKFLOW_PROGRESS)
        return await self._get_object(path, True)

    @data_save_error
    async def save_workflow_progress(self, workflow_id: str,
                                     metadata: Dict[str, Any]) -> None:
        path = self._get_path(workflow_id, STEPS_DIR, WORKFLOW_PROGRESS)
        await self._put_object(path, metadata, True)
