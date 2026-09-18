import logging
from typing import TYPE_CHECKING, cast

import ray
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from ray.actor import ActorHandle
    from ray.data._internal.stats import DatasetStatsSummary

logger = logging.getLogger(__name__)

STATS_SUMMARY_SERVER_NAME = "dataset_stats_summary_server"
STATS_SUMMARY_SERVER_NAMESPACE = "_dataset_stats_summary_server"

REPORT_STATS_SUMMARY_TIMEOUT_S = 30


class _StatsSummaryServer:
    """Retains the stats summaries of finished executions."""

    def __init__(self):
        self._summaries: "list[DatasetStatsSummary]" = []

    def report_stats_summary(self, stats_summary: "DatasetStatsSummary") -> None:
        self._summaries.append(stats_summary)

    def list_stats_summaries(self) -> "list[DatasetStatsSummary]":
        return self._summaries

    def clear_stats_summaries(self) -> None:
        self._summaries.clear()


def _get_or_create_stats_summary_server() -> "ActorHandle[_StatsSummaryServer]":
    """Return the server for this cluster, creating it if it doesn't exist yet."""
    # Pin to the caller's node so the server fate-shares with the driver, matching
    # `_StatsActor`.
    label_selector = {
        # pyrefly: ignore[missing-attribute]  # constant lives in the Cython ext
        ray._raylet.RAY_NODE_ID_KEY: ray.get_runtime_context().get_node_id()
    }
    # ``ray.remote``'s overloads widen the inferred type to include the undecorated
    # class, so name the handle type we know we get back.
    return cast(
        "ActorHandle[_StatsSummaryServer]",
        ray.remote(num_cpus=0)(_StatsSummaryServer)
        .options(
            name=STATS_SUMMARY_SERVER_NAME,
            namespace=STATS_SUMMARY_SERVER_NAMESPACE,
            get_if_exists=True,
            lifetime="detached",
            label_selector=label_selector,
        )
        .remote(),
    )


def report_stats_summary(stats_summary: "DatasetStatsSummary") -> None:
    """Record a finished execution's summary on the server.

    Blocks until the server acknowledges the summary, so that it is queryable as soon
    as the execution that produced it returns.
    """
    server = _get_or_create_stats_summary_server()
    ray.get(
        server.report_stats_summary.remote(stats_summary),
        timeout=REPORT_STATS_SUMMARY_TIMEOUT_S,
    )


@DeveloperAPI
def list_stats_summaries() -> "list[DatasetStatsSummary]":
    """List the stats summaries of executions that have finished on this cluster.

    The motivation for this API is to observe execution statistics in our benchmark
    utilities without adding too much boilerplate to the benchmark scripts.

    To enable this API, set ``DataContext.enable_stats_summary_collection = True``.
    It's disabled by default because reporting stats summaries adds overhead at the
    end of an execution.

    Examples:

        .. testcode::

            import tempfile

            import ray
            from ray.data import DataContext

            DataContext.get_current().enable_stats_summary_collection = True

            with tempfile.TemporaryDirectory() as path:
                ray.data.range(1).write_parquet(path)

            print(len(ray.data.list_stats_summaries()))

        .. testoutput::

            1

    Returns:
        One :class:`~ray.data._internal.stats.DatasetStatsSummary` per execution, in
        completion order.
    """
    server = _get_or_create_stats_summary_server()
    return ray.get(server.list_stats_summaries.remote())


def clear_stats_summaries() -> None:
    """Discard every summary the server is holding.

    Intended for tests, which would otherwise see summaries reported by earlier tests
    sharing the same cluster.
    """
    server = _get_or_create_stats_summary_server()
    ray.get(server.clear_stats_summaries.remote())
