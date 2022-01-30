"""
This module is higher-level abstraction of storage directly used by
workflows.
"""

import asyncio
from typing import Dict, List, Optional, Any, Callable, Tuple, Union
from dataclasses import dataclass
import logging

import ray
from ray import cloudpickle
from ray._private import signature
from ray.workflow import storage
from ray.workflow.common import (
    Workflow,
    StepID,
    WorkflowMetaData,
    WorkflowStatus,
    WorkflowRef,
    WorkflowNotFoundError,
    WorkflowStepRuntimeOptions,
)
from ray.workflow import workflow_context
from ray.workflow import serialization
from ray.workflow import serialization_context
from ray.workflow.storage import DataLoadError, DataSaveError, KeyNotFoundError
from ray.types import ObjectRef

logger = logging.getLogger(__name__)

ArgsType = Tuple[List[Any], Dict[str, Any]]  # args and kwargs

# constants used for keys
OBJECTS_DIR = "objects"
STEPS_DIR = "steps"
STEP_INPUTS_METADATA = "inputs.json"
STEP_USER_METADATA = "user_step_metadata.json"
STEP_PRERUN_METADATA = "pre_step_metadata.json"
STEP_POSTRUN_METADATA = "post_step_metadata.json"
STEP_OUTPUTS_METADATA = "outputs.json"
STEP_ARGS = "args.pkl"
STEP_OUTPUT = "output.pkl"
STEP_EXCEPTION = "exception.pkl"
STEP_FUNC_BODY = "func_body.pkl"
CLASS_BODY = "class_body.pkl"
WORKFLOW_META = "workflow_meta.json"
WORKFLOW_USER_METADATA = "user_run_metadata.json"
WORKFLOW_PRERUN_METADATA = "pre_run_metadata.json"
WORKFLOW_POSTRUN_METADATA = "post_run_metadata.json"
WORKFLOW_PROGRESS = "progress.json"
# Without this counter, we're going to scan all steps to get the number of
# steps with a given name. This can be very expensive if there are too
# many duplicates.
DUPLICATE_NAME_COUNTER = "duplicate_name_counter"


# TODO: Get rid of this and use asyncio.run instead once we don't support py36
def asyncio_run(coro):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(coro)


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
    # The workflows in the inputs of the workflow.
    workflows: Optional[List[str]] = None
    # The dynamically referenced workflows in the input of the workflow.
    workflow_refs: Optional[List[str]] = None
    # The options of the workflow step.
    step_options: Optional[WorkflowStepRuntimeOptions] = None
    # step throw exception
    step_raised_exception: bool = False

    def is_recoverable(self) -> bool:
        return (
            self.output_object_valid
            or self.output_step_id
            or (self.args_valid and self.workflows is not None and self.func_body_valid)
        )


class WorkflowStorage:
    """Access workflow in storage. This is a higher-level abstraction,
    which does not care about the underlining storage implementation."""

    def __init__(self, workflow_id: str, store: storage.Storage):
        self._storage = store
        self._workflow_id = workflow_id

    def load_step_output(self, step_id: StepID) -> Any:
        """Load the output of the workflow step from checkpoint.

        Args:
            step_id: ID of the workflow step.

        Returns:
            Output of the workflow step.
        """

        tasks = [
            self._get(self._key_step_output(step_id), no_exception=True),
            self._get(self._key_step_exception(step_id), no_exception=True),
        ]
        ((output_ret, output_err), (exception_ret, exception_err)) = asyncio_run(
            asyncio.gather(*tasks)
        )
        # When we have output, always return output first
        if output_err is None:
            return output_ret

        # When we don't have output, check exception
        if exception_err is None:
            raise exception_ret

        # In this case, there is no such step
        raise output_err

    def save_step_output(
        self,
        step_id: StepID,
        ret: Union[Workflow, Any],
        *,
        exception: Optional[Exception],
        outer_most_step_id: StepID,
    ) -> None:
        """When a workflow step returns,
        1. If the returned object is a workflow, this means we are a nested
           workflow. We save the output metadata that points to the workflow.
        2. Otherwise, checkpoint the output.

        Args:
            step_id: The ID of the workflow step. If it is an empty string,
                it means we are in the workflow job driver process.
            ret: The returned object from a workflow step.
            exception: This step should throw exception.
            outer_most_step_id: See WorkflowStepContext.
        """
        tasks = []
        dynamic_output_id = None
        if isinstance(ret, Workflow):
            # This workflow step returns a nested workflow.
            assert step_id != ret.step_id
            assert exception is None
            tasks.append(
                self._put(
                    self._key_step_output_metadata(step_id),
                    {"output_step_id": ret.step_id},
                    True,
                )
            )
            dynamic_output_id = ret.step_id
        else:
            if exception is None:
                # This workflow step returns a object.
                ret = ray.get(ret) if isinstance(ret, ray.ObjectRef) else ret
                promise = serialization.dump_to_storage(
                    self._key_step_output(step_id),
                    ret,
                    self._workflow_id,
                    self._storage,
                )
                tasks.append(promise)
                # tasks.append(self._put(self._key_step_output(step_id), ret))
                dynamic_output_id = step_id
                # TODO (yic): Delete exception file
            else:
                assert ret is None
                promise = serialization.dump_to_storage(
                    self._key_step_exception(step_id),
                    exception,
                    self._workflow_id,
                    self._storage,
                )
                tasks.append(promise)
                # tasks.append(
                #     self._put(self._key_step_exception(step_id), exception))

        # Finish checkpointing.
        asyncio_run(asyncio.gather(*tasks))

        # NOTE: if we update the dynamic output before
        # finishing checkpointing, then during recovery, the dynamic could
        # would point to a checkpoint that does not exist.
        if dynamic_output_id is not None:
            asyncio_run(
                self._update_dynamic_output(outer_most_step_id, dynamic_output_id)
            )

    def load_step_func_body(self, step_id: StepID) -> Callable:
        """Load the function body of the workflow step.

        Args:
            step_id: ID of the workflow step.

        Returns:
            A callable function.
        """
        return asyncio_run(self._get(self._key_step_function_body(step_id)))

    def gen_step_id(self, step_name: str) -> int:
        async def _gen_step_id():
            key = self._key_num_steps_with_name(step_name)
            try:
                val = await self._get(key, True)
                await self._put(key, val + 1, True)
                return val + 1
            except KeyNotFoundError:
                await self._put(key, 0, True)
                return 0

        return asyncio_run(_gen_step_id())

    def load_step_args(
        self, step_id: StepID, workflows: List[Any], workflow_refs: List[WorkflowRef]
    ) -> Tuple[List, Dict[str, Any]]:
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
            workflows, workflow_refs
        ):
            flattened_args = asyncio_run(self._get(self._key_step_args(step_id)))
            # dereference arguments like Ray remote functions
            flattened_args = [
                ray.get(a) if isinstance(a, ray.ObjectRef) else a
                for a in flattened_args
            ]
            return signature.recover_args(flattened_args)

    def save_object_ref(self, obj_ref: ray.ObjectRef) -> None:
        """Save the object ref.

        Args:
            obj_ref: The object reference

        Returns:
            None
        """
        return asyncio_run(self._save_object_ref(obj_ref))

    def load_object_ref(self, object_id: str) -> ray.ObjectRef:
        """Load the input object ref.

        Args:
            object_id: The hex ObjectID.

        Returns:
            The object ref.
        """

        async def _load_obj_ref() -> ray.ObjectRef:
            data = await self._get(self._key_obj_id(object_id))
            ref = _put_obj_ref.remote((data,))
            return ref

        return asyncio_run(_load_obj_ref())

    async def _update_dynamic_output(
        self, outer_most_step_id: StepID, dynamic_output_step_id: StepID
    ) -> None:
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
            outer_most_step_id: See WorkflowStepContext for explanation.
            dynamic_output_step_id: ID of dynamic_step.
        """
        # outer_most_step_id == "" indicates the root step of a
        # workflow. This would directly update "outputs.json" in
        # the workflow dir, and we want to avoid it.
        if outer_most_step_id is None or outer_most_step_id == "":
            return

        metadata = await self._get(
            self._key_step_output_metadata(outer_most_step_id), True
        )
        if dynamic_output_step_id != metadata[
            "output_step_id"
        ] and dynamic_output_step_id != metadata.get("dynamic_output_step_id"):
            metadata["dynamic_output_step_id"] = dynamic_output_step_id
            await self._put(
                self._key_step_output_metadata(outer_most_step_id), metadata, True
            )

    async def _locate_output_step_id(self, step_id: StepID) -> str:
        metadata = await self._get(self._key_step_output_metadata(step_id), True)
        return metadata.get("dynamic_output_step_id") or metadata["output_step_id"]

    def get_entrypoint_step_id(self) -> StepID:
        """Load the entrypoint step ID of the workflow.

        Returns:
            The ID of the entrypoint step.
        """
        # empty StepID represents the workflow driver
        try:
            return asyncio_run(self._locate_output_step_id(""))
        except Exception as e:
            raise ValueError(
                "Fail to get entrypoint step ID from workflow"
                f"[id={self._workflow_id}]"
            ) from e

    def inspect_step(self, step_id: StepID) -> StepInspectResult:
        """
        Get the status of a workflow step. The status indicates whether
        the workflow step can be recovered etc.

        Args:
            step_id: The ID of a workflow step

        Returns:
            The status of the step.
        """
        return asyncio_run(self._inspect_step(step_id))

    async def _inspect_step(self, step_id: StepID) -> StepInspectResult:
        items = await self._scan(self._key_step_prefix(step_id))
        keys = set(items)
        # does this step contains output checkpoint file?
        if STEP_OUTPUT in keys:
            return StepInspectResult(output_object_valid=True)
        # do we know where the output comes from?
        if STEP_OUTPUTS_METADATA in keys:
            output_step_id = await self._locate_output_step_id(step_id)
            return StepInspectResult(output_step_id=output_step_id)

        # read inputs metadata
        try:
            metadata = await self._get(self._key_step_input_metadata(step_id), True)
            return StepInspectResult(
                args_valid=(STEP_ARGS in keys),
                func_body_valid=(STEP_FUNC_BODY in keys),
                workflows=metadata["workflows"],
                workflow_refs=metadata["workflow_refs"],
                step_options=WorkflowStepRuntimeOptions.from_dict(
                    metadata["step_options"]
                ),
                step_raised_exception=(STEP_EXCEPTION in keys),
            )
        except Exception:
            return StepInspectResult(
                args_valid=(STEP_ARGS in keys),
                func_body_valid=(STEP_FUNC_BODY in keys),
                step_raised_exception=(STEP_EXCEPTION in keys),
            )

    async def _save_object_ref(self, identifier: str, obj_ref: ray.ObjectRef):
        data = await obj_ref
        await self._put(self._key_obj_id(identifier), data)

    def load_actor_class_body(self) -> type:
        """Load the class body of the virtual actor.

        Raises:
            DataLoadError: if we fail to load the class body.
        """
        return asyncio_run(self._get(self._key_class_body()))

    def save_actor_class_body(self, cls: type) -> None:
        """Save the class body of the virtual actor.

        Args:
            cls: The class body used by the virtual actor.

        Raises:
            DataSaveError: if we fail to save the class body.
        """
        asyncio_run(self._put(self._key_class_body(), cls))

    def save_step_prerun_metadata(self, step_id: StepID, metadata: Dict[str, Any]):
        """Save pre-run metadata of the current step.

        Args:
            step_id: ID of the workflow step.
            metadata: pre-run metadata of the current step.

        Raises:
            DataSaveError: if we fail to save the pre-run metadata.
        """

        asyncio_run(self._put(self._key_step_prerun_metadata(step_id), metadata, True))

    def save_step_postrun_metadata(self, step_id: StepID, metadata: Dict[str, Any]):
        """Save post-run metadata of the current step.

        Args:
            step_id: ID of the workflow step.
            metadata: post-run metadata of the current step.

        Raises:
            DataSaveError: if we fail to save the post-run metadata.
        """

        asyncio_run(self._put(self._key_step_postrun_metadata(step_id), metadata, True))

    def save_workflow_user_metadata(self, metadata: Dict[str, Any]):
        """Save user metadata of the current workflow.

        Args:
            metadata: user metadata of the current workflow.

        Raises:
            DataSaveError: if we fail to save the user metadata.
        """

        asyncio_run(self._put(self._key_workflow_user_metadata(), metadata, True))

    def save_workflow_prerun_metadata(self, metadata: Dict[str, Any]):
        """Save pre-run metadata of the current workflow.

        Args:
            metadata: pre-run metadata of the current workflow.

        Raises:
            DataSaveError: if we fail to save the pre-run metadata.
        """

        asyncio_run(self._put(self._key_workflow_prerun_metadata(), metadata, True))

    def save_workflow_postrun_metadata(self, metadata: Dict[str, Any]):
        """Save post-run metadata of the current workflow.

        Args:
            metadata: post-run metadata of the current workflow.

        Raises:
            DataSaveError: if we fail to save the post-run metadata.
        """

        asyncio_run(self._put(self._key_workflow_postrun_metadata(), metadata, True))

    def load_step_metadata(self, step_id: StepID) -> Dict[str, Any]:
        """Load the metadata of the given step.

        Returns:
            The metadata of the given step.
        """

        async def _load_step_metadata():
            if not await self._scan([self._workflow_id, "steps", step_id]):
                if not await self._scan([self._workflow_id]):
                    raise ValueError("No such workflow_id {}".format(self._workflow_id))
                else:
                    raise ValueError(
                        "No such step_id {} in workflow {}".format(
                            step_id, self._workflow_id
                        )
                    )

            tasks = [
                self._get(self._key_step_input_metadata(step_id), True, True),
                self._get(self._key_step_prerun_metadata(step_id), True, True),
                self._get(self._key_step_postrun_metadata(step_id), True, True),
            ]

            (
                (input_metadata, _),
                (prerun_metadata, _),
                (postrun_metadata, _),
            ) = await asyncio.gather(*tasks)

            input_metadata = input_metadata or {}
            prerun_metadata = prerun_metadata or {}
            postrun_metadata = postrun_metadata or {}

            metadata = input_metadata
            metadata["stats"] = {}
            metadata["stats"].update(prerun_metadata)
            metadata["stats"].update(postrun_metadata)

            return metadata

        return asyncio_run(_load_step_metadata())

    def load_workflow_metadata(self) -> Dict[str, Any]:
        """Load the metadata of the current workflow.

        Returns:
            The metadata of the current workflow.
        """

        async def _load_workflow_metadata():
            if not await self._scan([self._workflow_id]):
                raise ValueError("No such workflow_id {}".format(self._workflow_id))

            tasks = [
                self._get(self._key_workflow_metadata(), True, True),
                self._get(self._key_workflow_user_metadata(), True, True),
                self._get(self._key_workflow_prerun_metadata(), True, True),
                self._get(self._key_workflow_postrun_metadata(), True, True),
            ]

            (
                (status_metadata, _),
                (user_metadata, _),
                (prerun_metadata, _),
                (postrun_metadata, _),
            ) = await asyncio.gather(*tasks)

            status_metadata = status_metadata or {}
            user_metadata = user_metadata or {}
            prerun_metadata = prerun_metadata or {}
            postrun_metadata = postrun_metadata or {}

            metadata = status_metadata
            metadata["user_metadata"] = user_metadata
            metadata["stats"] = {}
            metadata["stats"].update(prerun_metadata)
            metadata["stats"].update(postrun_metadata)

            return metadata

        return asyncio_run(_load_workflow_metadata())

    def save_workflow_meta(self, metadata: WorkflowMetaData) -> None:
        """Save the metadata of the current workflow.

        Args:
            metadata: WorkflowMetaData of the current workflow.

        Raises:
            DataSaveError: if we fail to save the class body.
        """

        metadata = {
            "status": metadata.status.value,
        }
        asyncio_run(self._put(self._key_workflow_metadata(), metadata, True))

    def load_workflow_meta(self) -> Optional[WorkflowMetaData]:
        """Load the metadata of the current workflow.

        Returns:
            The metadata of the current workflow. If it doesn't exist,
            return None.
        """

        try:
            metadata = asyncio_run(self._get(self._key_workflow_metadata(), True))
            return WorkflowMetaData(status=WorkflowStatus(metadata["status"]))
        except KeyNotFoundError:
            return None

    async def _list_workflow(self) -> List[Tuple[str, WorkflowStatus]]:
        prefix = self._storage.make_key("")
        workflow_ids = await self._storage.scan_prefix(prefix)
        metadata = await asyncio.gather(
            *[
                self._get([workflow_id, WORKFLOW_META], True)
                for workflow_id in workflow_ids
            ]
        )
        return [
            (wid, WorkflowStatus(meta["status"]) if meta else None)
            for (wid, meta) in zip(workflow_ids, metadata)
        ]

    def list_workflow(self) -> List[Tuple[str, WorkflowStatus]]:
        return asyncio_run(self._list_workflow())

    def advance_progress(self, finished_step_id: "StepID") -> None:
        """Save the latest progress of a workflow. This is used by a
        virtual actor.

        Args:
            finished_step_id: The step that contains the latest output.

        Raises:
            DataSaveError: if we fail to save the progress.
        """
        asyncio_run(
            self._put(
                self._key_workflow_progress(),
                {
                    "step_id": finished_step_id,
                },
                True,
            )
        )

    def get_latest_progress(self) -> "StepID":
        """Load the latest progress of a workflow. This is used by a
        virtual actor.

        Raises:
            DataLoadError: if we fail to load the progress.

        Returns:
            The step that contains the latest output.
        """
        return asyncio_run(self._get(self._key_workflow_progress(), True))["step_id"]

    def delete_workflow(self):
        prefix = self._storage.make_key(self._workflow_id)

        scan = []
        scan_future = self._storage.scan_prefix(prefix)
        delete_future = self._storage.delete_prefix(prefix)

        try:
            # TODO (Alex): There's a race condition here if someone tries to
            # start the workflow between thesea ops.
            scan = asyncio_run(scan_future)
            asyncio_run(delete_future)
        except FileNotFoundError:
            # TODO (Alex): Different file systems seem to have different
            # behavior when deleting a prefix that doesn't exist, so we may
            # need to catch a broader class of exceptions.
            pass

        if not scan:
            raise WorkflowNotFoundError(self._workflow_id)

    async def _put(self, paths: List[str], data: Any, is_json: bool = False) -> str:
        """
        Serialize and put an object in the object store.

        Args:
            paths: The path components to store the object at.
            data: The data to be stored.
            is_json: If true, json encode the data, otherwise pickle it.
            update: If false, do not upload data when the path already exists.
        """
        key = self._storage.make_key(*paths)
        try:
            upload_tasks: List[ObjectRef] = []
            if not is_json:
                await serialization.dump_to_storage(
                    paths, data, self._workflow_id, self._storage
                )
            else:
                value = data
                outer_coro = self._storage.put(key, value, is_json=is_json)
                # The serializer only kicks off the upload tasks, and returns
                # the location they will be uploaded to in order to allow those
                # uploads to be parallelized. We should wait for those uploads
                # to be finished before we consider the object fully
                # serialized.
                await asyncio.gather(outer_coro, *upload_tasks)
        except Exception as e:
            raise DataSaveError from e

        return key

    async def _get(
        self, paths: List[str], is_json: bool = False, no_exception: bool = False
    ) -> Any:
        err = None
        ret = None
        try:
            key = self._storage.make_key(*paths)
            unmarshaled = await self._storage.get(key, is_json=is_json)
            if is_json:
                ret = unmarshaled
            else:
                ret = cloudpickle.loads(unmarshaled)
        except KeyNotFoundError as e:
            err = e
        except Exception as e:
            err = DataLoadError()
            err.__cause__ = e

        if no_exception:
            return (ret, err)
        else:
            if err is None:
                return ret
            else:
                raise err

    async def _scan(self, paths: List[str]) -> Any:
        try:
            prefix = self._storage.make_key(*paths)
            return await self._storage.scan_prefix(prefix)
        except Exception as e:
            raise DataLoadError from e

    # The following functions are helper functions to get the key
    # for a specific fields

    def _key_workflow_progress(self):
        return [self._workflow_id, STEPS_DIR, WORKFLOW_PROGRESS]

    def _key_step_input_metadata(self, step_id):
        return [self._workflow_id, STEPS_DIR, step_id, STEP_INPUTS_METADATA]

    def _key_step_user_metadata(self, step_id):
        return [self._workflow_id, STEPS_DIR, step_id, STEP_USER_METADATA]

    def _key_step_prerun_metadata(self, step_id):
        return [self._workflow_id, STEPS_DIR, step_id, STEP_PRERUN_METADATA]

    def _key_step_postrun_metadata(self, step_id):
        return [self._workflow_id, STEPS_DIR, step_id, STEP_POSTRUN_METADATA]

    def _key_step_output(self, step_id):
        return [self._workflow_id, STEPS_DIR, step_id, STEP_OUTPUT]

    def _key_step_exception(self, step_id):
        return [self._workflow_id, STEPS_DIR, step_id, STEP_EXCEPTION]

    def _key_step_output_metadata(self, step_id):
        return [self._workflow_id, STEPS_DIR, step_id, STEP_OUTPUTS_METADATA]

    def _key_step_function_body(self, step_id):
        return [self._workflow_id, STEPS_DIR, step_id, STEP_FUNC_BODY]

    def _key_step_args(self, step_id):
        return [self._workflow_id, STEPS_DIR, step_id, STEP_ARGS]

    def _key_obj_id(self, object_id):
        return [self._workflow_id, OBJECTS_DIR, object_id]

    def _key_step_prefix(self, step_id):
        return [self._workflow_id, STEPS_DIR, step_id, ""]

    def _key_class_body(self):
        return [self._workflow_id, CLASS_BODY]

    def _key_workflow_metadata(self):
        return [self._workflow_id, WORKFLOW_META]

    def _key_workflow_user_metadata(self):
        return [self._workflow_id, WORKFLOW_USER_METADATA]

    def _key_workflow_prerun_metadata(self):
        return [self._workflow_id, WORKFLOW_PRERUN_METADATA]

    def _key_workflow_postrun_metadata(self):
        return [self._workflow_id, WORKFLOW_POSTRUN_METADATA]

    def _key_num_steps_with_name(self, name):
        return [self._workflow_id, DUPLICATE_NAME_COUNTER, name]


def get_workflow_storage(workflow_id: Optional[str] = None) -> WorkflowStorage:
    """Get the storage for the workflow.

    Args:
        workflow_id: The ID of the storage.

    Returns:
        A workflow storage.
    """
    store = storage.get_global_storage()
    if workflow_id is None:
        workflow_id = workflow_context.get_workflow_step_context().workflow_id
    return WorkflowStorage(workflow_id, store)


def _load_object_ref(paths: List[str], wf_storage: WorkflowStorage) -> ObjectRef:
    @ray.remote(num_cpus=0)
    def load_ref(paths: List[str], wf_storage: WorkflowStorage):
        return asyncio.get_event_loop().run_until_complete(wf_storage._get(paths))

    return load_ref.remote(paths, wf_storage)


@ray.remote(num_cpus=0)
def _put_obj_ref(ref: Tuple[ObjectRef]):
    """
    Return a ref to an object ref. (This can't be done with
    `ray.put(obj_ref)`).

    """
    return ref[0]
