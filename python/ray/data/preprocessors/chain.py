import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from ray.data.preprocessor import Preprocessor, SerializablePreprocessorBase
from ray.data.preprocessors.dag import _fit_levels, _fit_order
from ray.data.preprocessors.utils import (
    AggregateStatSpec,
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

    When ``DataContext.enable_aggregation_based_preprocessors`` is set and every
    fittable member supports it, ``fit`` instead only registers
    each member's statistics as aggregation queries; the first ``transform()``
    (or ``transform_batch()``, or serialization) computes them over the fit
    dataset, batching independent members' aggregations into a single dataset
    scan per dependency level. The fitted statistics are identical either way.

    Deferred fitting relies on each member's declared input and output
    columns: a member that declares both is assumed to change only those
    columns, row by row. A member that adds or drops rows (for example, a
    user-defined preprocessor that filters out nulls) must not declare both;
    such members are treated as reading and writing every column, so the
    members around them fit in chain order.

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
        # Columns an earlier member produces aren't inputs of the chain.
        columns = {}
        written = set()
        for p in self._preprocessors:
            for column in p.get_input_columns():
                if column not in written:
                    columns[column] = None
            written.update(p.get_output_columns())
        return list(columns)

    def get_output_columns(self) -> List[str]:
        columns = {}
        for p in self._preprocessors:
            columns.update(dict.fromkeys(p.get_output_columns()))
        return list(columns)

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
        return (
            self._deferred_fit
            and not self._stats_materialized
            and self._fit_ds is not None
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

        Preprocessors are fitted in the order given by ``_fit_order`` - a
        permutation that lets preprocessors touching disjoint columns share
        the earliest scan possible. Only this fit-time pipeline is reordered;
        the stored chain and inference-time transform order are untouched.

        ``_fit_levels`` then assigns each preprocessor a level - a layered
        topological sort of the column-dependency graph. Two preprocessors on
        the same level never read each other's output columns, so their
        statistics are valid over the same version of the fit dataset and can
        be computed in the same pass.

        The levels are then processed in order. For each level:

        1. Compute the statistics: run every aggregation registered by that
           level's preprocessors as one ``Dataset.aggregate()`` call, and
           write each result into the owning preprocessor's ``stats_`` under
           the aggregator's alias.
        2. Advance the data: apply the transforms of the now fully-fitted
           preprocessors, in fit order, so the next level's statistics are
           computed over the data those preprocessors will actually see.

        The fitted statistics are identical to sequential fitting; the cost
        drops to ``max(levels) + 1`` dataset scans instead of one scan per
        fittable preprocessor.
        """
        ds = self._fit_ds

        # NOTE: Only fittable members are checked; a non-fittable user-defined
        #       member may not call `Preprocessor.__init__` and so have no plan.
        if any(
            p._is_fittable and p._stat_computation_plan.has_custom_stat_fn()
            for p in self._preprocessors
        ):
            self._fallback_to_serial_execution(ds)
            self._stats_materialized = True
            self._fit_ds = None
            return

        fit_sequence = [self._preprocessors[i] for i in _fit_order(self._preprocessors)]
        levels = _fit_levels(fit_sequence)

        num_transformed = 0
        for level in range(max(levels, default=-1) + 1):
            # Every aggregation scheduled at this level. NOTE: Every
            # registered aggregation runs, even when `stats_` is already
            # partly filled (e.g. `Categorizer` copies the dtypes it was
            # given before registering the rest).
            ready: List[Tuple[Preprocessor, AggregateStatSpec]] = [
                (p, spec)
                for p, p_level in zip(fit_sequence, levels)
                if p_level == level and p._is_fittable
                for spec in p._stat_computation_plan
            ]

            # One aggregation query for all of them. Specs sharing an alias
            # (e.g. two members needing mean of the same column) can't share
            # one query's result columns, so run them in alias-unique batches.
            while ready:
                batch = []
                rest = []
                seen_aliases = set()
                for preprocessor, spec in ready:
                    # NOTE: A plan's aggregate specs always hold an aggregation
                    #       with an alias (callable stats are separate specs).
                    if spec.stat_fn.name in seen_aliases:
                        rest.append((preprocessor, spec))
                    else:
                        seen_aliases.add(spec.stat_fn.name)
                        batch.append((preprocessor, spec))

                results = execute_aggregate_specs(ds, [spec for _, spec in batch])
                for preprocessor, spec in batch:
                    preprocessor.stats_[spec.stat_fn.name] = results[spec.stat_fn.name]
                ready = rest

            # Apply transforms in fit order, stopping at the first member that
            # hasn't fitted yet, so no member rewrites data that an earlier
            # conflicting member still needs to fit on.
            while num_transformed < len(levels) and levels[num_transformed] <= level:
                preprocessor = fit_sequence[num_transformed]
                self._sync_nested_chain_fit_ds(preprocessor, ds)
                ds = preprocessor.transform(ds)
                num_transformed += 1

        self._stats_materialized = True
        # The fit dataset is only needed to compute the statistics; don't keep
        # it (and possibly its blocks) alive for the chain's lifetime.
        self._fit_ds = None

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
            # NOTE: Every fittable member deferred its statistics, even if its
            #       `stats_` is already partly filled (e.g. `Categorizer`).
            if preprocessor._is_fittable:
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
