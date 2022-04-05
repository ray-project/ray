import functools
from typing import Callable, Dict, Any, Optional, TYPE_CHECKING

from ray._private import signature
from ray.workflow import serialization_context
from ray.workflow.common import (
    Workflow,
    WorkflowData,
    StepType,
    ensure_ray_initialized,
    WorkflowStepRuntimeOptions,
    validate_user_metadata,
)
from ray.workflow import workflow_context
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.workflow.common import CheckpointModeType


def _inherit_checkpoint_option(checkpoint: "Optional[CheckpointModeType]"):
    # If checkpoint option is not specified, inherit checkpoint
    # options from context (i.e. checkpoint options of the outer
    # step). If it is still not specified, it's True by default.
    context = workflow_context.get_workflow_step_context()
    if checkpoint is None:
        if context is not None:
            return context.checkpoint_context.checkpoint
    if checkpoint is None:
        return True
    return checkpoint


class WorkflowStepFunction:
    """This class represents a workflow step."""

    def __init__(
        self,
        func: Callable,
        *,
        step_options: "WorkflowStepRuntimeOptions" = None,
        name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        validate_user_metadata(metadata)
        self._func = func
        self._step_options = step_options
        self._func_signature = signature.extract_signature(func)
        self._name = name or ""
        self._user_metadata = metadata or {}

        # Override signature and docstring
        @functools.wraps(func)
        def _build_workflow(*args, **kwargs) -> Workflow:
            flattened_args = signature.flatten_args(self._func_signature, args, kwargs)

            def prepare_inputs():
                ensure_ray_initialized()
                return serialization_context.make_workflow_inputs(flattened_args)

            nonlocal step_options
            if step_options is None:
                step_options = WorkflowStepRuntimeOptions.make(
                    step_type=StepType.FUNCTION
                )
            # We could have "checkpoint=None" when we use @workflow.step
            # with arguments. Avoid this by updating it here.
            step_options.checkpoint = _inherit_checkpoint_option(
                step_options.checkpoint
            )

            workflow_data = WorkflowData(
                func_body=self._func,
                inputs=None,
                step_options=step_options,
                name=self._name,
                user_metadata=self._user_metadata,
            )
            return Workflow(workflow_data, prepare_inputs)

        self.step = _build_workflow

    def __call__(self, *args, **kwargs):
        raise TypeError(
            "Workflow steps cannot be called directly. Instead "
            f"of running '{self.step.__name__}()', "
            f"try '{self.step.__name__}.step()'."
        )

    @PublicAPI(stability="beta")
    def options(
        self,
        *,
        max_retries: int = None,
        catch_exceptions: bool = None,
        name: str = None,
        metadata: Dict[str, Any] = None,
        allow_inplace: bool = None,
        checkpoint: "Optional[CheckpointModeType]" = None,
        **ray_options,
    ) -> "WorkflowStepFunction":
        """This function set how the step function is going to be executed.

        Args:
            max_retries: num of retries the step for an application
                level error.
            catch_exceptions: Whether the user want to take care of the
                failure mannually.
                If it's set to be true, (Optional[R], Optional[E]) will be
                returned.
                If it's false, the normal result will be returned.
            name: The name of this step, which will be used to
                generate the step_id of the step. The name will be used
                directly as the step id if possible, otherwise deduplicated by
                appending .N suffixes.
            metadata: metadata to add to the step.
            allow_inplace: Execute the workflow step inplace.
            checkpoint: The option for checkpointing.
            **ray_options: All parameters in this fields will be passed
                to ray remote function options.

        Returns:
            The step function itself.
        """
        validate_user_metadata(metadata)
        name = name if name is not None else self._name
        metadata = {**self._user_metadata, **(metadata if metadata is not None else {})}
        step_options = WorkflowStepRuntimeOptions.make(
            step_type=StepType.FUNCTION,
            catch_exceptions=catch_exceptions
            if catch_exceptions is not None
            else self._step_options.catch_exceptions,
            max_retries=max_retries
            if max_retries is not None
            else self._step_options.max_retries,
            allow_inplace=allow_inplace
            if allow_inplace is not None
            else self._step_options.allow_inplace,
            checkpoint=_inherit_checkpoint_option(checkpoint),
            ray_options={
                **self._step_options.ray_options,
                **(ray_options if ray_options is not None else {}),
            },
        )
        return WorkflowStepFunction(
            self._func, step_options=step_options, name=name, metadata=metadata
        )
