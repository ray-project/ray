"""Unit tests for the exact resource requests reported to the V2 autoscaler.

Covers ``ResourceRequest`` construction and the operator-side plumbing in
``PhysicalOperator.get_resource_requests``.
"""

from unittest.mock import MagicMock

import pytest

from ray.data._internal.execution.interfaces import (
    ExecutionResources,
    PhysicalOperator,
    ResourceRequest,
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


class TestResourceRequestConstruction:
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
    def test_from_task_options(self, options, expected):
        assert ResourceRequest.from_task_options(options).resources == expected

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
    def test_from_actor_options(self, options, expected):
        assert ResourceRequest.from_actor_options(options).resources == expected

    def test_from_execution_resources_only_keeps_standard_dimensions(self):
        """``ExecutionResources`` cannot express custom resources.

        Operators that only carry a logical bundle therefore lose custom
        resources; they must report ``task_resource_request`` instead.
        """
        request = ResourceRequest.from_execution_resources(
            ExecutionResources(cpu=1, gpu=2, object_store_memory=100)
        )

        assert request.resources == {"CPU": 1, "GPU": 2, "object_store_memory": 100}

    def test_resources_dict_is_copied_on_construction(self):
        resources = {"CPU": 1}
        request = ResourceRequest(resources=resources)

        resources["CPU"] = 99
        assert request.resources == {"CPU": 1}


class TestPhysicalOperatorResourceRequests:
    def test_base_requests_prefer_exact_request_over_bundle(self):
        op = _make_operator()
        exact = ResourceRequest(resources={"GPU": 1, "worker_group_a": 1})
        op.get_active_tasks = MagicMock(
            return_value=[
                _make_task(request=exact),
                # No exact request: falls back to the logical bundle.
                _make_task(bundle=ExecutionResources(cpu=2)),
                # A zero bundle carries no demand.
                _make_task(bundle=ExecutionResources.zero()),
                # An empty exact request is skipped too.
                _make_task(request=ResourceRequest(resources={})),
            ]
        )

        assert op._get_base_resource_requests() == [
            exact,
            ResourceRequest(resources={"CPU": 2}),
        ]

    def _stub_extra_usage(self, op, *, object_store_usage=0, extra=None, started=True):
        """Stub the usage hooks ``get_extra_resource_usage`` reads."""
        op.estimate_object_store_usage = MagicMock(return_value=object_store_usage)
        # `metrics` is a read-only property, so stub the underlying object.
        op._metrics = MagicMock(obj_store_mem_pending_task_outputs=0)
        if extra is not None:
            op.extra_resource_usage = MagicMock(return_value=extra)
        # The real hooks are only wired up once the executor starts the operator.
        op._started = started

    def test_node_level_usage_is_not_folded_into_requests(self):
        """Object store usage must not corrupt the task shapes it is reported with.

        Folding it in would both change the shape and let a node-level shortage
        make the shape unhostable, scaling up node types that are not short.
        """
        op = _make_operator()
        base_requests = [
            ResourceRequest(resources={"CPU": 1}),
            ResourceRequest(resources={"CPU": 1}),
        ]
        op._get_base_resource_requests = MagicMock(return_value=base_requests)
        self._stub_extra_usage(op, object_store_usage=1500)

        assert op.get_resource_requests() == base_requests

    def test_extra_usage_is_reported_separately_from_requests(self):
        op = _make_operator()
        op._get_base_resource_requests = MagicMock(
            return_value=[ResourceRequest(resources={"CPU": 1})]
        )
        self._stub_extra_usage(op, object_store_usage=1500)

        assert op.get_extra_resource_usage().object_store_memory == 1500

    def test_extra_usage_excludes_operator_reported_usage(self):
        """`extra_resource_usage` is backpressure accounting, not autoscaler demand.

        It reports aggregate CPU/GPU/memory of tasks Ray Core re-runs, which
        would corrupt task shapes if folded in. Those tasks are reported as
        requests of their own shape instead (see the operator-level tests).
        """
        op = _make_operator()
        op._get_base_resource_requests = MagicMock(
            return_value=[ResourceRequest(resources={"CPU": 1})]
        )
        self._stub_extra_usage(
            op, object_store_usage=1500, extra=ExecutionResources(gpu=1)
        )

        assert op.get_extra_resource_usage().object_store_memory == 1500
        assert op.get_extra_resource_usage().gpu == 0

    def test_no_extra_usage_reports_zero(self):
        op = _make_operator()
        op._get_base_resource_requests = MagicMock(
            return_value=[ResourceRequest(resources={"CPU": 1})]
        )
        self._stub_extra_usage(op)

        assert op.get_extra_resource_usage().is_zero()

    def test_extra_usage_without_active_requests(self):
        """Node-level usage is independent of how many requests are in flight."""
        op = _make_operator()
        op._get_base_resource_requests = MagicMock(return_value=[])
        self._stub_extra_usage(op, object_store_usage=1000)

        assert op.get_resource_requests() == []
        assert op.get_extra_resource_usage().object_store_memory == 1000

    def test_unstarted_operator_reports_no_extra_usage(self):
        """The block counter only exists once the executor starts the operator.

        The autoscaler walks every operator in the topology, so this must not
        raise for an operator that has not started yet.
        """
        op = _make_operator()
        self._stub_extra_usage(op, object_store_usage=1000, started=False)

        assert op.get_extra_resource_usage().is_zero()

    def test_get_resource_requests_keeps_task_shape_exact(self):
        op = _make_operator()
        op.get_active_tasks = MagicMock(
            return_value=[_make_task(request=ResourceRequest(resources={"CPU": 1}))]
        )
        self._stub_extra_usage(op, object_store_usage=100)

        assert op.get_resource_requests() == [ResourceRequest(resources={"CPU": 1})]
        assert op.get_extra_resource_usage().object_store_memory == 100


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
