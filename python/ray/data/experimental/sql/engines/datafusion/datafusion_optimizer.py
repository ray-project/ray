"""DataFusion query optimizer integration for Ray Data SQL API.

This module provides integration with Apache Arrow DataFusion's advanced
query optimizer while preserving Ray Data's distributed execution, resource
management, and backpressure control.

Apache DataFusion documentation:
https://datafusion.apache.org/

Architecture:
    SQL Query
      → DataFusion: Parsing + Logical optimization (CBO, join reordering)
      → Translation: Extract optimization decisions
      → Ray Data: Distributed execution + resource + backpressure management
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from ray.data import Dataset
from ray.data.experimental.sql.utils import setup_logger

try:
    import datafusion as df

    DATAFUSION_AVAILABLE = True
except ImportError:
    DATAFUSION_AVAILABLE = False
    df = None

from .config import DataFusionSamplingConfig, get_sampling_config


@dataclass
class DataFusionOptimizations:
    """Optimization decisions extracted from DataFusion's query planner.

    These optimizations guide Ray Data execution while preserving
    Ray Data's distributed execution and resource management.
    """

    join_order: List[Tuple[str, str]]  # Optimized join sequence
    join_algorithms: Dict[str, str]  # Join type for each join
    filter_placement: List[Dict[str, Any]]  # Where to apply filters
    projection_columns: List[str]  # Column pruning decisions
    aggregation_strategy: Optional[str] = None  # Hash vs sort aggregation
    sort_keys: Optional[List[str]] = None  # Sort operations
    parallelism_hint: Optional[int] = None  # Suggested parallelism
    predicate_pushdown: bool = True  # Whether predicates were pushed down
    projection_pushdown: bool = True  # Whether projections were pushed down


class DataFusionOptimizer:
    """Apache DataFusion query optimizer for Ray Data SQL.

    Uses DataFusion's advanced cost-based optimizer (CBO) for query planning
    and optimization, then executes queries using Ray Data's distributed
    execution engine with resource and backpressure management.

    Apache DataFusion: https://datafusion.apache.org/

    Key Benefits:
    - DataFusion: Advanced query optimization (join ordering, predicate placement)
    - Ray Data: Distributed execution, fault tolerance, backpressure control

    Examples:
        >>> optimizer = DataFusionOptimizer()
        >>> if optimizer.is_available():
        ...     optimizations = optimizer.optimize_query(query, datasets)
        ...     result = execute_with_ray_data(optimizations)
    """

    def __init__(self, config: Optional[DataFusionSamplingConfig] = None):
        """Initialize DataFusion optimizer.

        Args:
            config: Sampling configuration. Uses defaults if not provided.
        """
        self.available = DATAFUSION_AVAILABLE
        self._logger = setup_logger("DataFusionOptimizer")
        self.sampling_config = config or get_sampling_config()

        if self.available:
            try:
                self.df_ctx = df.SessionContext()
                # Successfully initialized
            except Exception as e:
                self._logger.warning(f"DataFusion SessionContext init failed: {e}")
                self.available = False
                self.df_ctx = None
        else:
            self.df_ctx = None

    def is_available(self) -> bool:
        """Check if DataFusion optimizer is available.

        Returns:
            True if DataFusion is installed and initialized successfully.
        """
        return self.available

    def optimize_query(
        self, query: str, datasets: Dict[str, Dataset], dialect: str = "duckdb"
    ) -> Optional[DataFusionOptimizations]:
        """Optimize SQL query using DataFusion's cost-based optimizer."""
        if not self.available:
            return None

        try:
            translated_query = self._translate_to_postgres(query, dialect)
            self._register_datasets(datasets)
            df_result = self.df_ctx.sql(translated_query)
            optimizations = self._extract_optimizations(df_result.optimized_logical_plan(), query, datasets)
            self._logger.info(
                f"DataFusion optimization extracted: "
                f"{len(optimizations.filter_placement)} filters, "
                f"{len(optimizations.projection_columns)} projections, "
                f"{len(optimizations.join_order)} joins"
            )
            return optimizations
        except Exception as e:
            self._logger.warning(f"DataFusion optimization failed: {e}, will fallback to SQLGlot")
            return None

    def _translate_to_postgres(self, query: str, source_dialect: str) -> str:
        """Translate SQL query from user's dialect to PostgreSQL for DataFusion."""
        if source_dialect.lower() in {"postgres", "postgresql", "duckdb"}:
            return query
        try:
            import sqlglot
            postgres_query = sqlglot.parse_one(query, read=source_dialect).sql(dialect="postgres")
            self._logger.debug(f"Translated query from {source_dialect} to PostgreSQL for DataFusion")
            return postgres_query
        except Exception as e:
            self._logger.warning(f"Could not translate {source_dialect} to PostgreSQL: {e}, using original query")
            return query

    def _register_datasets(self, datasets: Dict[str, Dataset]) -> None:
        """Register Ray Datasets with DataFusion for optimization planning."""
        table_size_estimates = {name: self._estimate_dataset_size(ray_ds, name) for name, ray_ds in datasets.items()}
        if not table_size_estimates:
            return
        max_table_size = max(table_size_estimates.values())

        # Determine global sampling strategy that maintains proportions
        # If largest table is 1M rows, we sample it at 10K (1%)
        # Then smaller tables get proportionally smaller samples
        cfg = self.sampling_config
        base_sample_fraction = (
            cfg.tiny_sample_fraction if max_table_size <= cfg.tiny_threshold
            else cfg.small_sample_fraction if max_table_size <= cfg.small_threshold
            else cfg.medium_sample_fraction if max_table_size <= cfg.medium_threshold
            else cfg.large_sample_fraction if max_table_size <= cfg.large_threshold
            else cfg.max_sample_size / max_table_size
        )

        self._logger.info(
            f"Sampling {len(datasets)} tables with {base_sample_fraction:.2%} fraction "
            f"(largest table: ~{max_table_size} rows)"
        )

        for name, ray_ds in datasets.items():
            try:
                estimated_rows = table_size_estimates.get(name, cfg.default_sample_size)
                sample_size = max(cfg.min_sample_size, min(cfg.max_sample_size, int(estimated_rows * base_sample_fraction)))
                sample_arrow = ray_ds.limit(sample_size).to_arrow()
                self.df_ctx.register_table(name, sample_arrow)
                self._logger.debug(
                    f"Registered table '{name}' with DataFusion: "
                    f"{len(sample_arrow)} rows ({sample_size}/{estimated_rows} = "
                    f"{sample_size/max(estimated_rows,1):.1%}), "
                    f"{len(sample_arrow.schema)} columns"
                )
            except Exception as e:
                self._logger.warning(f"Failed to register table '{name}' with DataFusion: {e}")

    def _estimate_dataset_size(self, dataset: Dataset, table_name: str) -> int:
        """Estimate dataset size WITHOUT materialization."""
        try:
            if hasattr(dataset, "_plan") and hasattr(dataset._plan, "_dataset_stats_summary"):
                try:
                    stats = dataset._plan._dataset_stats_summary
                    if hasattr(stats, "num_rows") and stats.num_rows:
                        self._logger.debug(f"Table '{table_name}': Estimated {stats.num_rows} rows from metadata")
                        return stats.num_rows
                except Exception:
                    pass

            cfg = self.sampling_config
            dataset_str = str(type(dataset))
            if "read_parquet" in dataset_str:
                return cfg.medium_threshold
            elif "read_csv" in dataset_str:
                return cfg.small_threshold
            elif "from_items" in dataset_str:
                return cfg.tiny_threshold
            self._logger.debug(f"Table '{table_name}': No metadata, assuming {cfg.default_sample_size} rows")
            return cfg.default_sample_size
        except Exception:
            return self.sampling_config.default_sample_size

    def _extract_optimizations(
        self, optimized_plan: Any, query: str, datasets: Dict[str, Dataset]
    ) -> DataFusionOptimizations:
        """Extract optimization decisions from DataFusion's optimized logical plan."""
        try:
            plan_lines = str(optimized_plan).split("\n")
            filters = self._extract_filters_from_plan(plan_lines)
            projections = self._extract_projections_from_plan(plan_lines)
            join_order, join_algorithms = self._extract_joins_from_plan(plan_lines)
            agg_strategy = self._extract_aggregation_from_plan(plan_lines)
            self._logger.info(
                f"Extracted optimizations: {len(filters)} filters, "
                f"{len(projections)} projections, {len(join_order)} joins"
            )

            return DataFusionOptimizations(
                join_order=join_order,
                join_algorithms=join_algorithms,
                filter_placement=filters,
                projection_columns=projections,
                aggregation_strategy=agg_strategy,
                predicate_pushdown=True,
                projection_pushdown=True,
            )

        except Exception as e:
            self._logger.debug(f"Could not extract detailed optimizations: {e}")
            # Return basic optimizations on error
            return DataFusionOptimizations(
                join_order=[],
                join_algorithms={},
                filter_placement=[],
                projection_columns=[],
                predicate_pushdown=True,
                projection_pushdown=True,
            )

    def _extract_from_plan(self, plan_lines: List[str], keyword: str, extractor, wrapper=None):
        """Generic method to extract information from plan lines."""
        results = []
        keyword_lower = keyword.lower()
        for i, line in enumerate(plan_lines):
            if keyword in line or keyword_lower in line.lower():
                extracted = extractor(line)
                if extracted:
                    item = wrapper(i, extracted) if wrapper else extracted
                    results.extend(item if isinstance(item, list) else [item])
        return results

    def _extract_filters_from_plan(self, plan_lines: List[str]) -> List[Dict[str, Any]]:
        """Extract filter placements from DataFusion plan lines."""
        filters = self._extract_from_plan(plan_lines, "Filter:", self._extract_filter_from_line, lambda i, f: {"expression": f, "position": i, "type": "filter"})
        self._logger.debug(f"Extracted {len(filters)} filter placements from DataFusion plan")
        return filters

    def _extract_projections_from_plan(self, plan_lines: List[str]) -> List[str]:
        """Extract projection columns from DataFusion plan lines."""
        projections = self._extract_from_plan(plan_lines, "Projection:", self._extract_columns_from_line)
        self._logger.debug(f"Extracted {len(projections)} projection columns from DataFusion plan")
        return projections

    def _extract_joins_from_plan(
        self, plan_lines: List[str]
    ) -> Tuple[List[Tuple[str, str]], Dict[str, str]]:
        """Extract join order and algorithms from DataFusion plan lines."""
        joins = self._extract_from_plan(plan_lines, "Join:", self._extract_join_from_line)
        if joins:
            join_order = [(j.get("left", ""), j.get("right", "")) for j in joins]
            join_algorithms = {f"{j.get('left', '')}_{j.get('right', '')}": j.get("type", "inner") for j in joins}
            return join_order, join_algorithms
        return [], {}

    def _extract_aggregation_from_plan(self, plan_lines: List[str]) -> Optional[str]:
        """Extract aggregation strategy from DataFusion plan lines."""
        strategies = self._extract_from_plan(plan_lines, "Aggregate:", self._extract_aggregation_strategy)
        if strategies:
            self._logger.debug(f"Extracted aggregation strategy: {strategies[0]}")
            return strategies[0]
        return None

    def _extract_from_line(self, line: str, delimiter: str = ":") -> Optional[str]:
        """Extract value from plan line after delimiter."""
        if delimiter in line:
            parts = line.split(delimiter, 1)
            if len(parts) == 2:
                return parts[1].strip()
        return None

    def _extract_filter_from_line(self, line: str) -> Optional[str]:
        """Extract filter expression from plan line."""
        return self._extract_from_line(line)

    def _extract_columns_from_line(self, line: str) -> List[str]:
        """Extract column names from projection line."""
        col_part = self._extract_from_line(line)
        return [c.strip() for c in col_part.split(",")] if col_part and "," in col_part else []

    def _extract_join_from_line(self, line: str) -> Optional[Dict[str, str]]:
        """Extract join information from plan line."""
        line_lower = line.lower()
        join_type = "inner"
        if "left" in line_lower:
            join_type = "left"
        elif "right" in line_lower:
            join_type = "right"
        return {"type": join_type}

    def _extract_aggregation_strategy(self, line: str) -> Optional[str]:
        """Extract aggregation strategy from plan line."""
        return "sort_aggregate" if "sort" in line.lower() else "hash_aggregate"


def get_datafusion_optimizer() -> Optional[DataFusionOptimizer]:
    """Get DataFusion optimizer instance if available."""
    if not DATAFUSION_AVAILABLE:
        return None
    try:
        return DataFusionOptimizer()
    except Exception:
        return None

def is_datafusion_available() -> bool:
    """Check if DataFusion is available for use."""
    return DATAFUSION_AVAILABLE
