"""
This module is higher-level abstraction of storage directly used by
workflows.
"""

import json
import os
from typing import Dict, List, Optional, Any, Callable, Tuple, Union, Set
from dataclasses import dataclass
import logging

import ray
from ray import cloudpickle
from ray._private import signature
from ray.internal import storage
from ray.workflow.common import (
    Workflow,
    StepID,
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
WORKFLOW_ROOT = "workflows"  # The workflow root directory under global Ray storage.
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
WORKFLOW_STATUS_DIR = "__status__"
WORKFLOW_STATUS_DIRTY_DIR = "dirty"
# Without this counter, we're going to scan all steps to get the number of
# steps with a given name. This can be very expensive if there are too
# many duplicates.
DUPLICATE_NAME_COUNTER = "duplicate_name_counter"


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


class WorkflowIndexingStorage:
    """Access and maintenance the indexing of workflow status.

    It runs a protocol that guarantees we can recover from any interrupted
    status updating. This protocol is **not thread-safe** for updating the
    status of the same workflow, currently it is executed by workflow management
    actor with a single thread.

    Here is how the protocol works:

    Update the status of a workflow
    1. Load workflow status from workflow data. If it is the same as the new status,
       return.
    2. Check if the workflow status updating is dirty. If it is, fix the
       workflow status; otherwise, mark the workflow status updating dirty.
    3. Update status in the workflow metadata.
    4. Insert the workflow ID key in the status indexing directory of the new status.
    5. Delete the workflow ID key in the status indexing directory of
       the previous status.
    6. Remove the workflow status updating dirty mark.

    Load a status of a workflow
    1. Read the status of the workflow from the workflow metadata.
    2. Return the status.

    List the status of all workflows
    1. Get status of all workflows by listing workflow ID keys in each workflow
       status indexing directory.
    2. List all workflows with dirty updating status. Get their status from
       workflow data. Override the status of the corresponding workflow.
    3. Return all the status.
    """

    def __init__(self):
        self._storage = storage.get_client(WORKFLOW_ROOT)

    def update_workflow_status(self, workflow_id: str, status: WorkflowStatus):
        """Update the status of the workflow.
        Try fixing indexing if workflow status updating was marked dirty.

        This method is NOT thread-safe. It is handled by the workflow management actor.
        """
        prev_status = self.load_workflow_status(workflow_id)
        if prev_status != status:
            # Try fixing indexing if workflow status updating was marked dirty.
            if (
                self._storage.get_info(self._key_workflow_status_dirty(workflow_id))
                is not None
            ):
                # This means the previous status update failed. Fix it.
                self._storage.put(
                    self._key_workflow_with_status(workflow_id, prev_status), b""
                )
                for s in WorkflowStatus:
                    if s != prev_status:
                        self._storage.delete(
                            self._key_workflow_with_status(workflow_id, s)
                        )
            else:
                self._storage.put(self._key_workflow_status_dirty(workflow_id), b"")
            # Transactional update of workflow status
            self._storage.put(
                self._key_workflow_metadata(workflow_id),
                json.dumps({"status": status.value}).encode(),
            )
            self._storage.put(self._key_workflow_with_status(workflow_id, status), b"")
            if prev_status is not WorkflowStatus.NONE:
                self._storage.delete(
                    self._key_workflow_with_status(workflow_id, prev_status)
                )
            self._storage.delete(self._key_workflow_status_dirty(workflow_id))

    def load_workflow_status(self, workflow_id: str):
        """Load the committed workflow status."""
        raw_data = self._storage.get(self._key_workflow_metadata(workflow_id))
        if raw_data is not None:
            metadata = json.loads(raw_data)
            return WorkflowStatus(metadata["status"])
        return WorkflowStatus.NONE

    def list_workflow(
        self, status_filter: Optional[Set[WorkflowStatus]] = None
    ) -> List[Tuple[str, WorkflowStatus]]:
        """List workflow status. Override status of the workflows whose status updating
        were marked dirty with the workflow status from workflow metadata.

        Args:
            status_filter: If given, only returns workflow with that status. This can
                be a single status or set of statuses.
        """
        if status_filter is None:
            status_filter = set(WorkflowStatus)
            status_filter.discard(WorkflowStatus.NONE)
        elif not isinstance(status_filter, set):
            raise TypeError("'status_filter' should either be 'None' or a set.")
        elif WorkflowStatus.NONE in status_filter:
            raise ValueError("'WorkflowStatus.NONE' is not a valid filter value.")

        results = {}
        for status in status_filter:
            try:
                # empty string points the key to the dir
                for p in self._storage.list(self._key_workflow_with_status("", status)):
                    workflow_id = p.base_name
                    results[workflow_id] = status
            except FileNotFoundError:
                pass
        # Get "correct" status of workflows
        try:
            for p in self._storage.list(self._key_workflow_status_dirty("")):
                workflow_id = p.base_name
                # overwrite status
                results.pop(workflow_id, None)
                status = self.load_workflow_status(workflow_id)
                if status in status_filter:
                    results[workflow_id] = status
        except FileNotFoundError:
            pass
        return list(results.items())

    def delete_workflow_status(self, workflow_id: str):
        """Delete status indexing for the workflow."""
        for status in WorkflowStatus:
            self._storage.delete(self._key_workflow_with_status(workflow_id, status))
        self._storage.delete(self._key_workflow_status_dirty(workflow_id))

    def _key_workflow_with_status(self, workflow_id: str, status: WorkflowStatus):
        """A key whose existence marks the status of the workflow."""
        return os.path.join(WORKFLOW_STATUS_DIR, status.value, workflow_id)

    def _key_workflow_status_dirty(self, workflow_id: str):
        """A key marks the workflow status dirty, because it is under change."""
        return os.path.join(WORKFLOW_STATUS_DIR, WORKFLOW_STATUS_DIRTY_DIR, workflow_id)

    def _key_workflow_metadata(self, workflow_id: str):
        return os.path.join(workflow_id, WORKFLOW_META)


class WorkflowStorage:
    """Access workflow in storage. This is a higher-level abstraction,
    which does not care about the underlining storage implementation."""

    def __init__(self, workflow_id: str):
        from ray.workflow.api import _ensure_workflow_initialized

        _ensure_workflow_initialized()

        self._storage = storage.get_client(os.path.join(WORKFLOW_ROOT, workflow_id))
        self._status_storage = WorkflowIndexingStorage()
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
        (output_ret, output_err), (exception_ret, exception_err) = tasks
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
                    storage=self,
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
                    storage=self,
                )
                tasks.append(promise)
                # tasks.append(
                #     self._put(self._key_step_exception(step_id), exception))

        # Finish checkpointing.
        # TODO(suquark): batching all tasks above.

        # NOTE: if we update the dynamic output before
        # finishing checkpointing, then during recovery, the dynamic could
        # would point to a checkpoint that does not exist.
        if dynamic_output_id is not None:
            self._update_dynamic_output(outer_most_step_id, dynamic_output_id)

    def load_step_func_body(self, step_id: StepID) -> Callable:
        """Load the function body of the workflow step.

        Args:
            step_id: ID of the workflow step.

        Returns:
            A callable function.
        """
        return self._get(self._key_step_function_body(step_id))

    def gen_step_id(self, step_name: str) -> int:
        def _gen_step_id():
            key = self._key_num_steps_with_name(step_name)
            try:
                val = self._get(key, True)
                self._put(key, val + 1, True)
                return val + 1
            except KeyNotFoundError:
                self._put(key, 0, True)
                return 0

        return _gen_step_id()

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
            flattened_args = self._get(self._key_step_args(step_id))
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
        return self._save_object_ref(obj_ref)

    def load_object_ref(self, object_id: str) -> ray.ObjectRef:
        """Load the input object ref.

        Args:
            object_id: The hex ObjectID.

        Returns:
            The object ref.
        """

        def _load_obj_ref() -> ray.ObjectRef:
            data = self._get(self._key_obj_id(object_id))
            ref = _put_obj_ref.remote((data,))
            return ref

        return _load_obj_ref()

    def _update_dynamic_output(
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

        try:
            metadata = self._get(
                self._key_step_output_metadata(outer_most_step_id), True
            )
        except KeyNotFoundError:
            # This is because we skipped checkpointing of the
            # step [id=outer_most_step_id]. Return a dummy
            # metadata instead.
            metadata = {}
        if dynamic_output_step_id != metadata.get(
            "output_step_id"
        ) and dynamic_output_step_id != metadata.get("dynamic_output_step_id"):
            metadata["dynamic_output_step_id"] = dynamic_output_step_id
            self._put(
                self._key_step_output_metadata(outer_most_step_id), metadata, True
            )

    def _locate_output_step_id(self, step_id: StepID) -> str:
        metadata = self._get(self._key_step_output_metadata(step_id), True)
        return metadata.get("dynamic_output_step_id") or metadata["output_step_id"]

    def get_entrypoint_step_id(self) -> StepID:
        """Load the entrypoint step ID of the workflow.

        Returns:
            The ID of the entrypoint step.
        """
        # empty StepID represents the workflow driver
        try:
            return self._locate_output_step_id("")
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
        return self._inspect_step(step_id)

    def _inspect_step(self, step_id: StepID) -> StepInspectResult:
        items = self._scan(self._key_step_prefix(step_id), ignore_errors=True)
        keys = set(items)
        # does this step contains output checkpoint file?
        if STEP_OUTPUT in keys:
            return StepInspectResult(output_object_valid=True)
        # do we know where the output comes from?
        if STEP_OUTPUTS_METADATA in keys:
            output_step_id = self._locate_output_step_id(step_id)
            return StepInspectResult(output_step_id=output_step_id)

        # read inputs metadata
        try:
            metadata = self._get(self._key_step_input_metadata(step_id), True)
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

    def _save_object_ref(self, identifier: str, obj_ref: ray.ObjectRef):
        data = ray.get(obj_ref)
        self._put(self._key_obj_id(identifier), data)

    def load_actor_class_body(self) -> type:
        """Load the class body of the virtual actor.

        Raises:
            DataLoadError: if we fail to load the class body.
        """
        return self._get(self._key_class_body())

    def save_actor_class_body(self, cls: type) -> None:
        """Save the class body of the virtual actor.

        Args:
            cls: The class body used by the virtual actor.

        Raises:
            DataSaveError: if we fail to save the class body.
        """
        self._put(self._key_class_body(), cls)

    def save_step_prerun_metadata(self, step_id: StepID, metadata: Dict[str, Any]):
        """Save pre-run metadata of the current step.

        Args:
            step_id: ID of the workflow step.
            metadata: pre-run metadata of the current step.

        Raises:
            DataSaveError: if we fail to save the pre-run metadata.
        """

        self._put(self._key_step_prerun_metadata(step_id), metadata, True)

    def save_step_postrun_metadata(self, step_id: StepID, metadata: Dict[str, Any]):
        """Save post-run metadata of the current step.

        Args:
            step_id: ID of the workflow step.
            metadata: post-run metadata of the current step.

        Raises:
            DataSaveError: if we fail to save the post-run metadata.
        """

        self._put(self._key_step_postrun_metadata(step_id), metadata, True)

    def save_workflow_user_metadata(self, metadata: Dict[str, Any]):
        """Save user metadata of the current workflow.

        Args:
            metadata: user metadata of the current workflow.

        Raises:
            DataSaveError: if we fail to save the user metadata.
        """

        self._put(self._key_workflow_user_metadata(), metadata, True)

    def save_workflow_prerun_metadata(self, metadata: Dict[str, Any]):
        """Save pre-run metadata of the current workflow.

        Args:
            metadata: pre-run metadata of the current workflow.

        Raises:
            DataSaveError: if we fail to save the pre-run metadata.
        """

        self._put(self._key_workflow_prerun_metadata(), metadata, True)

    def save_workflow_postrun_metadata(self, metadata: Dict[str, Any]):
        """Save post-run metadata of the current workflow.

        Args:
            metadata: post-run metadata of the current workflow.

        Raises:
            DataSaveError: if we fail to save the post-run metadata.
        """

        self._put(self._key_workflow_postrun_metadata(), metadata, True)

    def load_step_metadata(self, step_id: StepID) -> Dict[str, Any]:
        """Load the metadata of the given step.

        Returns:
            The metadata of the given step.
        """

        def _load_step_metadata():
            if not self._scan(self._key_step_prefix(step_id), ignore_errors=True):
                if not self._scan("", ignore_errors=True):
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
            ) = tasks

            input_metadata = input_metadata or {}
            prerun_metadata = prerun_metadata or {}
            postrun_metadata = postrun_metadata or {}

            metadata = input_metadata
            metadata["stats"] = {**prerun_metadata, **postrun_metadata}

            return metadata

        return _load_step_metadata()

    def load_workflow_metadata(self) -> Dict[str, Any]:
        """Load the metadata of the current workflow.

        Returns:
            The metadata of the current workflow.
        """

        def _load_workflow_metadata():
            if not self._scan("", ignore_errors=True):
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
            ) = tasks

            status_metadata = status_metadata or {}
            user_metadata = user_metadata or {}
            prerun_metadata = prerun_metadata or {}
            postrun_metadata = postrun_metadata or {}

            metadata = status_metadata
            metadata["user_metadata"] = user_metadata
            metadata["stats"] = {**prerun_metadata, **postrun_metadata}

            return metadata

        return _load_workflow_metadata()

    def list_workflow(
        self, status_filter: Optional[Set[WorkflowStatus]] = None
    ) -> List[Tuple[str, WorkflowStatus]]:
        """List all workflows matching a given status filter.

        Args:
            status_filter: If given, only returns workflow with that status. This can
                be a single status or set of statuses.
        """
        return self._status_storage.list_workflow(status_filter)

    def advance_progress(self, finished_step_id: "StepID") -> None:
        """Save the latest progress of a workflow. This is used by a
        virtual actor.

        Args:
            finished_step_id: The step that contains the latest output.

        Raises:
            DataSaveError: if we fail to save the progress.
        """
        metadata = {"step_id": finished_step_id}
        self._put(self._key_workflow_progress(), metadata, True)

    def get_latest_progress(self) -> "StepID":
        """Load the latest progress of a workflow. This is used by a
        virtual actor.

        Raises:
            DataLoadError: if we fail to load the progress.

        Returns:
            The step that contains the latest output.
        """
        return self._get(self._key_workflow_progress(), True)["step_id"]

    def delete_workflow(self) -> None:
        # TODO (Alex): There's a race condition here if someone tries to
        # start the workflow between these ops.
        self._status_storage.delete_workflow_status(self._workflow_id)
        found = self._storage.delete_dir("")
        # TODO (Alex): Different file systems seem to have different
        # behavior when deleting a prefix that doesn't exist, so we may
        # need to catch a broader class of exceptions.

        if not found:
            raise WorkflowNotFoundError(self._workflow_id)

    def update_workflow_status(self, status: WorkflowStatus):
        """Update the status of the workflow.
        This method is NOT thread-safe. It is handled by the workflow management actor.
        """
        self._status_storage.update_workflow_status(self._workflow_id, status)

    def load_workflow_status(self):
        """Load workflow status. If we find the previous status updating failed,
        fix it with redo-log transaction recovery."""
        return self._status_storage.load_workflow_status(self._workflow_id)

    def _put(self, key: str, data: Any, is_json: bool = False) -> str:
        """Serialize and put an object in the object store.

        Args:
            key: The key of the object.
            data: The data to be stored.
            is_json: If true, json encode the data, otherwise pickle it.
        """
        # TODO(suquark): Currently put to file is not atomic -- you can get a partial
        # file. This could fail workflow recovery.
        try:
            if not is_json:
                serialization.dump_to_storage(
                    key, data, self._workflow_id, storage=self
                )
            else:
                serialized_data = json.dumps(data).encode()
                self._storage.put(key, serialized_data)
        except Exception as e:
            raise DataSaveError from e

        return key

    def _get(self, key: str, is_json: bool = False, no_exception: bool = False) -> Any:
        err = None
        ret = None
        try:
            unmarshaled = self._storage.get(key)
            if unmarshaled is None:
                raise KeyNotFoundError
            if is_json:
                ret = json.loads(unmarshaled.decode())
            else:
                ret = cloudpickle.loads(unmarshaled)
        except KeyNotFoundError as e:
            err = e
        except Exception as e:
            err = DataLoadError()
            err.__cause__ = e

        if no_exception:
            return (ret, err)
        elif err is None:
            return ret
        else:
            raise err

    def _scan(self, prefix: str, ignore_errors: bool = False) -> List[str]:
        try:
            return [p.base_name for p in self._storage.list(prefix)]
        except Exception as e:
            if ignore_errors:
                return []
            raise e

    def _exists(self, key: str) -> bool:
        return self._storage.get_info(key) is not None

    # The following functions are helper functions to get the key
    # for a specific fields

    def _key_workflow_progress(self):
        return os.path.join(STEPS_DIR, WORKFLOW_PROGRESS)

    def _key_step_input_metadata(self, step_id):
        return os.path.join(STEPS_DIR, step_id, STEP_INPUTS_METADATA)

    def _key_step_user_metadata(self, step_id):
        return os.path.join(STEPS_DIR, step_id, STEP_USER_METADATA)

    def _key_step_prerun_metadata(self, step_id):
        return os.path.join(STEPS_DIR, step_id, STEP_PRERUN_METADATA)

    def _key_step_postrun_metadata(self, step_id):
        return os.path.join(STEPS_DIR, step_id, STEP_POSTRUN_METADATA)

    def _key_step_output(self, step_id):
        return os.path.join(STEPS_DIR, step_id, STEP_OUTPUT)

    def _key_step_exception(self, step_id):
        return os.path.join(STEPS_DIR, step_id, STEP_EXCEPTION)

    def _key_step_output_metadata(self, step_id):
        return os.path.join(STEPS_DIR, step_id, STEP_OUTPUTS_METADATA)

    def _key_step_function_body(self, step_id):
        return os.path.join(STEPS_DIR, step_id, STEP_FUNC_BODY)

    def _key_step_args(self, step_id):
        return os.path.join(STEPS_DIR, step_id, STEP_ARGS)

    def _key_obj_id(self, object_id):
        return os.path.join(OBJECTS_DIR, object_id)

    def _key_step_prefix(self, step_id):
        return os.path.join(STEPS_DIR, step_id, "")

    def _key_class_body(self):
        return os.path.join(CLASS_BODY)

    def _key_workflow_metadata(self):
        return os.path.join(WORKFLOW_META)

    def _key_workflow_user_metadata(self):
        return os.path.join(WORKFLOW_USER_METADATA)

    def _key_workflow_prerun_metadata(self):
        return os.path.join(WORKFLOW_PRERUN_METADATA)

    def _key_workflow_postrun_metadata(self):
        return os.path.join(WORKFLOW_POSTRUN_METADATA)

    def _key_num_steps_with_name(self, name):
        return os.path.join(DUPLICATE_NAME_COUNTER, name)


def get_workflow_storage(workflow_id: Optional[str] = None) -> WorkflowStorage:
    """Get the storage for the workflow.

    Args:
        workflow_id: The ID of the storage.

    Returns:
        A workflow storage.
    """
    if workflow_id is None:
        workflow_id = workflow_context.get_workflow_step_context().workflow_id
    return WorkflowStorage(workflow_id)


def _load_object_ref(paths: List[str], wf_storage: WorkflowStorage) -> ObjectRef:
    @ray.remote(num_cpus=0)
    def load_ref(paths: List[str], wf_storage: WorkflowStorage):
        return wf_storage._get(paths)

    return load_ref.remote(paths, wf_storage)


@ray.remote(num_cpus=0)
def _put_obj_ref(ref: Tuple[ObjectRef]):
    """
    Return a ref to an object ref. (This can't be done with
    `ray.put(obj_ref)`).

    """
    return ref[0]
