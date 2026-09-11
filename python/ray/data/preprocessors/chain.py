import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from ray.data.preprocessor import Preprocessor, SerializablePreprocessorBase
from ray.data.preprocessors.dag import _AggregationNode, _build_aggregation_dag
from ray.data.preprocessors.utils import (
    _PublicField,
    execute_aggregate_specs,
    migrate_private_fields,
)
from ray.data.preprocessors.version_support import SerializablePreprocessor
from ray.data.util.data_batch_conversion import BatchFormat

if TYPE_CHECKING:
    from ray.air.data_batch_type import DataBatchType
    from ray.data.dataset import Dataset

logger = logging.getLogger(__name__)


@SerializablePreprocessor(version=1, identifier="io.ray.preprocessors.chain")
class Chain(SerializablePreprocessorBase):
    """Combine multiple preprocessors into a single :py:class:`Preprocessor`.

    When you call ``fit``, each preprocessor is fit on the dataset produced by the
    preceeding preprocessor's ``fit_transform``.

    When :attr:`DataContext.enable_aggregation_based_preprocessors
    <ray.data.context.DataContext.enable_aggregation_based_preprocessors>` is
    set and every fittable member supports it, ``fit`` instead only registers
    each member's statistics as aggregation queries; the first ``transform()``
    (or ``transform_batch()``, or serialization) computes them over the fit
    dataset, batching independent members' aggregations into a single dataset
    scan per dependency level. The fitted statistics are identical either way.

    Example:
        >>> import pandas as pd
        >>> import ray
        >>> from ray.data.preprocessors import *
        >>>
        >>> df = pd.DataFrame({
        ...     "X0": [0, 1, 2],
        ...     "X1": [3, 4, 5],
        ...     "Y": ["orange", "blue", "orange"],
        ... })
        >>> ds = ray.data.from_pandas(df)  # doctest: +SKIP
        >>>
        >>> preprocessor = Chain(
        ...     StandardScaler(columns=["X0", "X1"]),
        ...     Concatenator(columns=["X0", "X1"], output_column_name="X"),
        ...     LabelEncoder(label_column="Y")
        ... )
        >>> preprocessor.fit_transform(ds).to_pandas()  # doctest: +SKIP
           Y                                         X
        0  1  [-1.224744871391589, -1.224744871391589]
        1  0                                [0.0, 0.0]
        2  1    [1.224744871391589, 1.224744871391589]

    Args:
        *preprocessors: The preprocessors to sequentially compose.
    """

    def fit_status(self):
        fittable_count = 0
        fitted_count = 0

        for p in self._preprocessors:
            if p.fit_status() == Preprocessor.FitStatus.FITTED:
                fittable_count += 1
                fitted_count += 1
            elif p.fit_status() in (
                Preprocessor.FitStatus.NOT_FITTED,
                Preprocessor.FitStatus.PARTIALLY_FITTED,
            ):
                fittable_count += 1
            else:
                assert p.fit_status() == Preprocessor.FitStatus.NOT_FITTABLE
        if fittable_count > 0:
            if fitted_count == fittable_count:
                return Preprocessor.FitStatus.FITTED
            elif fitted_count > 0:
                return Preprocessor.FitStatus.PARTIALLY_FITTED
            else:
                return Preprocessor.FitStatus.NOT_FITTED
        else:
            return Preprocessor.FitStatus.NOT_FITTABLE

    def __init__(self, *preprocessors: SerializablePreprocessorBase):
        super().__init__()
        self._preprocessors = preprocessors
        self._fit_ds: Optional["Dataset"] = None
        self._deferred_fit = False
        self._stats_materialized = False

    @property
    def preprocessors(self) -> Tuple[SerializablePreprocessorBase, ...]:
        return self._preprocessors

    @property
    def _supports_deferred_fit(self) -> bool:
        # A chain can defer only when every fittable member can; a nested
        # Chain recurses into its own members.
        return all(
            not p._is_fittable or p._supports_deferred_fit for p in self._preprocessors
        )

    def get_input_columns(self) -> List[str]:
        columns = []
        for p in self._preprocessors:
            columns.extend(p.get_input_columns())
        return columns

    def get_output_columns(self) -> List[str]:
        columns = []
        for p in self._preprocessors:
            columns.extend(p.get_output_columns())
        return columns

    def _uses_deferred_fit(self) -> bool:
        from ray.data.context import DataContext

        return (
            DataContext.get_current().enable_aggregation_based_preprocessors
            and self._supports_deferred_fit
        )

    def _fit(self, ds: "Dataset") -> SerializablePreprocessorBase:
        self._fit_ds = None
        self._deferred_fit = False
        self._stats_materialized = False

        if self._uses_deferred_fit():
            # Every member's `_fit` only registers aggregations, so no member
            # needs the previous members' transformed output to fit: register
            # everything now and defer the scans to `_materialize_deferred_stats`,
            # which computes them level-by-level over the fit dataset.
            for preprocessor in self._preprocessors:
                if not preprocessor._is_fittable:
                    continue
                preprocessor._defer_fit_execute = True
                try:
                    preprocessor.fit(ds)
                finally:
                    preprocessor._defer_fit_execute = False
            self._fit_ds = ds
            self._deferred_fit = True
            return self

        for preprocessor in self._preprocessors[:-1]:
            ds = preprocessor.fit_transform(ds)
        self._preprocessors[-1].fit(ds)
        return self

    def fit_transform(self, ds: "Dataset") -> "Dataset":
        if self._uses_deferred_fit():
            self.fit(ds)
            return self.transform(ds)

        for preprocessor in self._preprocessors:
            ds = preprocessor.fit_transform(ds)
        return ds

    def _needs_stats_materialization(self) -> bool:
        # getattr: instances deserialized from older versions lack these fields.
        return (
            getattr(self, "_deferred_fit", False)
            and not getattr(self, "_stats_materialized", False)
            and getattr(self, "_fit_ds", None) is not None
        )

    @staticmethod
    def _sync_nested_chain_fit_ds(preprocessor: Preprocessor, ds: "Dataset") -> None:
        """Point a nested deferred Chain's fit dataset at the data actually
        flowing into it at its chain position (its members were registered on
        the outer chain's raw fit dataset)."""
        if (
            isinstance(preprocessor, Chain)
            and preprocessor._needs_stats_materialization()
        ):
            preprocessor._fit_ds = ds

    def _materialize_deferred_stats(self) -> None:
        """Compute every member's deferred statistics over the fit dataset.

        Builds a column-dependency DAG with one node per (member, aggregation)
        pair and runs one aggregation query per dependency level: all
        aggregations whose dependencies are complete run as a single
        ``Dataset.aggregate()`` call, results are routed into each owning
        member's ``stats_`` by aggregator alias, and each fully-fitted member
        then transforms the (lazy) fit dataset so the next level aggregates
        over transformed data - exactly what sequential fitting would have
        produced, in as few scans as there are dependency levels.
        """
        ds = self._fit_ds

        if any(
            p._stat_computation_plan.has_custom_stat_fn() for p in self._preprocessors
        ):
            self._fallback_to_serial_execution(ds)
            self._stats_materialized = True
            return

        all_nodes = _build_aggregation_dag(self._preprocessors)
        pending_nodes = list(all_nodes)
        transformed = set()

        while pending_nodes:
            ready = [node for node in pending_nodes if node.is_ready()]
            if not ready:
                raise RuntimeError("Circular dependency detected in aggregation DAG.")

            # One aggregation query for every ready aggregation node. Nodes
            # sharing an alias (e.g. two members needing mean of the same
            # column) can't share one query's result columns, so run them in
            # alias-unique batches.
            agg_nodes = [n for n in ready if isinstance(n, _AggregationNode)]
            while agg_nodes:
                batch = []
                rest = []
                seen_aliases = set()
                for node in agg_nodes:
                    alias = node.agg_fn.name
                    if alias in seen_aliases:
                        rest.append(node)
                    else:
                        seen_aliases.add(alias)
                        batch.append(node)

                results = execute_aggregate_specs(ds, [node.spec for node in batch])
                for node in batch:
                    node.preprocessor.stats_[node.agg_fn.name] = results[
                        node.agg_fn.name
                    ]
                agg_nodes = rest

            for node in ready:
                node.completed = True
            ready_set = set(ready)
            pending_nodes = [n for n in pending_nodes if n not in ready_set]

            # Apply the transform of every member whose nodes are all complete
            # (in chain order), so the next level aggregates over transformed
            # data.
            for preprocessor in self._preprocessors:
                if preprocessor in transformed:
                    continue
                if all(
                    node.completed
                    for node in all_nodes
                    if node.preprocessor is preprocessor
                ):
                    self._sync_nested_chain_fit_ds(preprocessor, ds)
                    ds = preprocessor.transform(ds)
                    transformed.add(preprocessor)

        self._stats_materialized = True

    def _fallback_to_serial_execution(self, ds: "Dataset") -> None:
        """Fit members one by one, each on the previous members' transformed
        output - the pre-deferral behavior. Used when a member registered a
        custom (callable) stat, which can't run inside ``Dataset.aggregate``.
        """
        logger.warning(
            "Falling back to serial statistics computation because one or "
            "more preprocessors use custom stat functions (e.g., "
            "add_callable_stat). Rewriting those stats as AggregateFnV2-based "
            "aggregators (add_aggregator) would allow batching them into a "
            "single pass over the dataset."
        )
        for preprocessor in self._preprocessors:
            if preprocessor._is_fittable and not preprocessor.has_stats():
                preprocessor._fit_execute(ds)
            self._sync_nested_chain_fit_ds(preprocessor, ds)
            ds = preprocessor.transform(ds)

    def _transform(
        self,
        ds: "Dataset",
        batch_size: Optional[int],
        num_cpus: Optional[float] = None,
        memory: Optional[float] = None,
        concurrency: Optional[int] = None,
    ) -> "Dataset":
        if self._needs_stats_materialization():
            self._materialize_deferred_stats()

        for preprocessor in self._preprocessors:
            ds = preprocessor.transform(
                ds,
                batch_size=batch_size,
                num_cpus=num_cpus,
                memory=memory,
                concurrency=concurrency,
            )
        return ds

    def _transform_batch(self, df: "DataBatchType") -> "DataBatchType":
        if self._needs_stats_materialization():
            self._materialize_deferred_stats()

        for preprocessor in self._preprocessors:
            df = preprocessor.transform_batch(df)
        return df

    def __repr__(self):
        arguments = ", ".join(
            repr(preprocessor) for preprocessor in self._preprocessors
        )
        return f"{self.__class__.__name__}({arguments})"

    def _determine_transform_to_use(self) -> BatchFormat:
        # This is relevant for BatchPrediction.
        # For Chain preprocessor, we picked the first one as entry point.
        # TODO (jiaodong): We should revisit if our Chain preprocessor is
        # still optimal with context of lazy execution.
        return self._preprocessors[0]._determine_transform_to_use()

    def _get_serializable_fields(self) -> Dict[str, Any]:
        # A deferred fit computes stats at the first transform; serializing a
        # fitted-but-never-transformed chain must materialize them first (in
        # dependency order), or the serialized members would have no stats.
        if self._needs_stats_materialization():
            self._materialize_deferred_stats()
        return {
            "preprocessors": self._preprocessors,
        }

    def _set_serializable_fields(self, fields: Dict[str, Any], version: int):
        # required fields
        self._preprocessors = fields["preprocessors"]
        # Serialized stats are always materialized (see
        # `_get_serializable_fields`), so a deserialized chain never defers.
        self._fit_ds = None
        self._deferred_fit = False
        self._stats_materialized = False

    def __getstate__(self) -> Dict[str, Any]:
        # Same reason as `_get_serializable_fields`: plain pickling must not
        # lose the deferred stats.
        if self._needs_stats_materialization():
            self._materialize_deferred_stats()
        state = super().__getstate__()
        # The fit dataset is only needed to materialize stats; never pickle it.
        state.pop("_fit_ds", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        """Handle backwards compatibility for old pickled objects."""
        super().__setstate__(state)
        self._fit_ds = None
        self.__dict__.setdefault("_deferred_fit", False)
        self.__dict__.setdefault("_stats_materialized", False)
        migrate_private_fields(
            self,
            fields={
                "_preprocessors": _PublicField(public_field="preprocessors"),
            },
        )
