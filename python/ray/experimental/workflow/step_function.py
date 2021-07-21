import functools
from typing import Callable

from ray._private import signature
from ray.experimental.workflow import serialization_context
from ray.experimental.workflow.common import Workflow, WorkflowData


class WorkflowStepFunction:
    """This class represents a workflow step."""

    def __init__(self,
                 func: Callable,
                 step_max_retries=1,
                 catch_exceptions=False,
                 ray_options=None):
        if not isinstance(step_max_retries, int) or step_max_retries < 1:
            raise ValueError(
                "step_max_retries should be greater or equal to 1.")
        if ray_options is not None and not isinstance(ray_options, dict):
            raise ValueError("ray_options must be a dict.")

        self._func = func
        self._step_max_retries = step_max_retries
        self._catch_exceptions = catch_exceptions
        self._ray_options = ray_options or {}
        self._func_signature = signature.extract_signature(func)

        # Override signature and docstring
        @functools.wraps(func)
        def _build_workflow(*args, **kwargs) -> Workflow:
            flattened_args = signature.flatten_args(self._func_signature, args,
                                                    kwargs)
            workflow_inputs = serialization_context.make_workflow_inputs(
                flattened_args)
            workflow_data = WorkflowData(
                func_body=self._func,
                inputs=workflow_inputs,
                step_max_retries=self._step_max_retries,
                catch_exceptions=self._catch_exceptions,
                ray_options=self._ray_options,
            )
            return Workflow(workflow_data)

        self.step = _build_workflow

    def __call__(self, *args, **kwargs):
        raise TypeError("Workflow steps cannot be called directly. Instead "
                        f"of running '{self.step.__name__}()', "
                        f"try '{self.step.__name__}.step()'.")

    def options(self,
                *,
                step_max_retries: int = 1,
                catch_exceptions: bool = False,
                **ray_options) -> "WorkflowStepFunction":
        """This function set how the step function is going to be executed.

        Args:
            step_max_retries(int): num of retries the step for an application
                level error
            catch_exceptions(bool): Whether the user want to take care of the
                failure mannually.
                If it's set to be true, (Optional[R], Optional[E]) will be
                returned.
                If it's false, the normal result will be returned.
            **ray_options(dict): All parameters in this fields will be passed
                to ray remote function options.

        Returns:
            The step function itself.
        """
        return WorkflowStepFunction(self._func, step_max_retries,
                                    catch_exceptions, ray_options)
