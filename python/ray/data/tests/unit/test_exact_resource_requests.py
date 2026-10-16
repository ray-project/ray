"""Unit tests for the exact resource requests reported to the cluster autoscaler.

Covers the resource-dict builders and the operator-side plumbing in
``PhysicalOperator.get_resource_requests``.
"""

from unittest.mock import MagicMock

import pytest

from ray.data._internal.execution.interfaces import (
    ExecutionResources,
    PhysicalOperator,
    actor_resource_dict,
    execution_resource_dict,
    task_resource_dict,
)
from ray.data.context import DataContext


def _make_operator(name: str = "test_op") -> PhysicalOperator:
    return PhysicalOperator(name, [], DataContext.get_current())


def _make_task(request=None, bundle=None) -> MagicMock:
    """Build a task double exposing the two resource views an operator reports."""
    task = MagicMock()
    task.get_requested_resource_request.return_value = request
    task.get_requested_resource_bundle.return_value = bundle
    return task


class TestResourceDictBuilders:
    @pytest.mark.parametrize(
        "options,expected",
        [
            ({"num_cpus": 2}, {"CPU": 2}),
            ({"num_gpus": 1, "memory": 1024}, {"GPU": 1, "memory": 1024}),
            # Zero-valued requests carry no demand.
            ({"num_cpus": 0}, {}),
            ({}, {}),
            # Custom resources are the whole point of the exact path.
            ({"resources": {"worker_group_a": 1}}, {"worker_group_a": 1}),
            ({"accelerator_type": "A10G"}, {"accelerator_type:A10G": 0.001}),
            (
                {"num_gpus": 1, "resources": {"worker_group_a": 1}},
                {"GPU": 1, "worker_group_a": 1},
            ),
        ],
    )
    def test_task_resource_dict(self, options, expected):
        assert task_resource_dict(options) == expected

    @pytest.mark.parametrize(
        "options,expected",
        [
            # An actor with any non-memory resource reserves 1 CPU by default.
            ({"num_gpus": 1}, {"GPU": 1, "CPU": 1}),
            ({"num_cpus": 4}, {"CPU": 4}),
            ({"resources": {"worker_group_a": 1}}, {"worker_group_a": 1, "CPU": 1}),
            # A memory-only actor keeps Ray's simple default of 0 CPU, which is
            # then dropped as a zero value.
            ({"memory": 1024}, {"memory": 1024}),
            ({}, {}),
        ],
    )
    def test_actor_resource_dict(self, options, expected):
        assert actor_resource_dict(options) == expected

    def test_execution_resource_dict_only_keeps_standard_dimensions(self):
        """``ExecutionResources`` cannot express custom resources.

        Operators that only carry a logical bundle therefore lose custom
        resources; they must report the request built from the task's remote
        options instead.
        """
        request = execution_resource_dict(
            ExecutionResources(cpu=1, gpu=2, object_store_memory=100)
        )

        assert request == {"CPU": 1, "GPU": 2, "object_store_memory": 100}

    @pytest.mark.parametrize("builder", [task_resource_dict, actor_resource_dict])
    def test_builders_do_not_alias_the_options(self, builder):
        """The returned dict is fresh, so mutating it never touches the options."""
        options = {"resources": {"worker_group_a": 1}}
        request = builder(options)

        request["CPU"] = 99
        assert options == {"resources": {"worker_group_a": 1}}


class TestPhysicalOperatorResourceRequests:
    def test_base_requests_prefer_exact_request_over_bundle(self):
        op = _make_operator()
        exact = {"GPU": 1, "worker_group_a": 1}
        op.get_active_tasks = MagicMock(
            return_value=[
                _make_task(request=exact),
                # No exact request: falls back to the logical bundle.
                _make_task(bundle=ExecutionResources(cpu=2)),
                # A zero bundle carries no demand.
                _make_task(bundle=ExecutionResources.zero()),
                # An empty exact request is skipped too.
                _make_task(request={}),
            ]
        )

        assert op._get_base_resource_requests() == [exact, {"CPU": 2}]

    def test_get_resource_requests_keeps_task_shape_exact(self):
        op = _make_operator()
        op.get_active_tasks = MagicMock(return_value=[_make_task(request={"CPU": 1})])

        assert op.get_resource_requests() == [{"CPU": 1}]

    def test_get_resource_requests_omits_node_level_usage(self):
        """Object store memory is not a scheduling requirement of one task.

        Folding it into a task's request would corrupt the shape and let a
        node-level shortage make every shape look unhostable, so it is left out
        of the requests entirely.
        """
        op = _make_operator()
        op.estimate_object_store_usage = MagicMock(return_value=1000)
        op._metrics = MagicMock(obj_store_mem_pending_task_outputs=0)
        op.get_active_tasks = MagicMock(return_value=[_make_task(request={"GPU": 1})])

        assert op.get_resource_requests() == [{"GPU": 1}]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
