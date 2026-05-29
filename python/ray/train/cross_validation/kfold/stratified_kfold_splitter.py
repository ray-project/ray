from typing import List, Optional, Tuple

import pandas as pd

from ray.data import Dataset
from ray.train.cross_validation.splitter import (
    Splitter,
    _build_folds_from_column,
    _get_noncolliding_column,
)
from ray.util.annotations import PublicAPI


def _assign_fold_ids_to_group(
    group_df: pd.DataFrame,
    stratify_column: str,
    n_splits: int,
    seed: Optional[int],
    fold_column: str,
) -> pd.DataFrame:
    """Distribute all rows of one stratification class across folds.

    Each fold gets roughly the same number of rows from that class.
    Fold ids are assigned positionally (row 0 → fold 0, row 1 → fold 1, ...),
    so a stable order must be established first.
    """
    if seed is not None:
        # Shuffle with the seed: reproducible but randomized ordering.
        group_df = group_df.sample(frac=1, random_state=seed).reset_index(drop=True)
    else:
        # No seed: sort deterministically instead.
        # Prefer non-float columns because float-to-string conversion is
        # platform-dependent and could cause inconsistent ordering.
        sort_cols = [
            c
            for c in group_df.columns
            if c != stratify_column and group_df[c].dtype.kind != "f"
        ]
        if sort_cols:
            group_df = group_df.sort_values(sort_cols).reset_index(drop=True)
        else:
            # No non-float columns available: fall back to float columns
            # but round to 6 decimal places to normalize platform differences.
            float_cols = [
                c
                for c in group_df.columns
                if c != stratify_column and group_df[c].dtype.kind == "f"
            ]
            if float_cols:
                group_df = group_df.sort_values(
                    by=float_cols,
                    key=lambda col: col.round(6).fillna(0.0),
                ).reset_index(drop=True)

    group_df[fold_column] = (pd.RangeIndex(len(group_df)) % n_splits).astype("int32")
    return group_df


@PublicAPI
class StratifiedKFoldSplitter(Splitter):
    """Stratified KFold splitter that ensures each fold has approximately
    the same class distribution as the original dataset with respect to a specified
    stratification column.

    Within each class, rows are shuffled (when ``seed`` is provided)
    or sorted by non-float non-stratify columns for determinism, then assigned to folds
    round-robin (``fold_id = rank_within_class % n_splits``).

    This guarantees at most 1 row difference per class per fold — the same
    guarantee as ``sklearn.model_selection.StratifiedKFold``.

    Unlike :class:`KFoldSplitter` and :class:`GroupedKFoldSplitter`, this
    splitter **materializes** the fold-assignment result once before filtering.
    This avoids recomputing the groupby and rank assignment 2 × n_splits times.
    """

    def __init__(
        self,
        n_splits: int,
        stratify_column: str,
        seed: Optional[int] = None,
    ) -> None:
        """Create a ``StratifiedKFoldSplitter``.

        Args:
            n_splits: Number of folds.
            stratify_column: Name of the column whose class distribution
                should be preserved across folds (e.g. a label or target
                column).
            seed: Optional int seed. When provided, rows within each class
                are shuffled with this seed before fold assignment, giving
                different but reproducible splits across runs.
        """
        super().__init__(n_splits)
        if not stratify_column:
            raise ValueError("stratify_column must be a non-empty string.")
        self._stratify_column = stratify_column
        self._seed = seed

    def split(self, dataset: Dataset) -> List[Tuple[Dataset, Dataset]]:
        schema_cols = set(dataset.schema().names)
        if self._stratify_column not in schema_cols:
            raise ValueError(
                f"stratify_column '{self._stratify_column}' not found in dataset. "
                f"Available columns: {sorted(schema_cols)}"
            )

        # Pre-check class counts on the driver and collect the small
        # aggregated summary so users receive an immediate, clear error when
        # some classes have fewer examples than `n_splits`.
        class_counts_ds = dataset.groupby(self._stratify_column).count()
        class_counts_df = class_counts_ds.to_pandas()
        # Identify the count column (any column other than the group key).
        count_cols = [c for c in class_counts_df.columns if c != self._stratify_column]
        if count_cols:
            count_col = count_cols[0]
            small = class_counts_df[class_counts_df[count_col] < self._n_splits]
            if not small.empty:
                items = ", ".join(
                    f"{repr(row[self._stratify_column])}: {int(row[count_col])}"
                    for _, row in small.iterrows()
                )
                raise ValueError(
                    f"StratifiedKFoldSplitter requires at least n_splits={self._n_splits} "
                    f"examples per class in '{self._stratify_column}'. Found classes with too few examples: {items}"
                )

        # Use a collision-free column name in case the dataset already has a "fold_id" column.
        fold_column = _get_noncolliding_column(schema_cols, "fold_id")

        stratify_column = self._stratify_column
        n_splits = self._n_splits
        seed = self._seed

        # Materialize once to avoid recomputing the groupby/map_groups pipeline
        # for each of the 2 * n_splits filter passes below.
        dataset_with_fold_id = (
            dataset.groupby(stratify_column)
            .map_groups(
                lambda group_df: _assign_fold_ids_to_group(
                    group_df, stratify_column, n_splits, seed, fold_column
                ),
                batch_format="pandas",
            )
            .materialize()
        )

        return _build_folds_from_column(
            dataset_with_fold_id, fold_column, self._n_splits
        )
