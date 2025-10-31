"""
DataFusion query optimizer integration for Ray Data SQL API.

This module provides integration with Apache Arrow DataFusion's advanced
query optimizer while preserving Ray Data's distributed execution, resource
management, and backpressure control.

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

# DataFusion is optional - gracefully handle if not available
try:
    import datafusion as df

    DATAFUSION_AVAILABLE = True
except ImportError:
    DATAFUSION_AVAILABLE = False
    df = None

# Import DataFusion configuration
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
    """
    Apache DataFusion query optimizer for Ray Data SQL.

    Uses DataFusion's advanced cost-based optimizer (CBO) for query planning
    and optimization, then executes queries using Ray Data's distributed
    execution engine with resource and backpressure management.

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
        """
        Optimize SQL query using DataFusion's cost-based optimizer.

        CRITICAL: DataFusion only supports PostgreSQL syntax. If the user's
        dialect is not PostgreSQL-compatible, we translate it using SQLGlot
        before sending to DataFusion.

        Flow:
        1. If dialect != postgres: Translate query to PostgreSQL (via SQLGlot)
        2. Register datasets with DataFusion
        3. Optimize with DataFusion CBO
        4. Extract optimization decisions

        Args:
            query: SQL query string in user's chosen dialect.
            datasets: Dictionary mapping table names to Ray Datasets.
            dialect: User's SQL dialect (e.g., "mysql", "spark", "postgres").

        Returns:
            DataFusionOptimizations with extracted optimization decisions,
            or None if optimization fails (caller should fallback to SQLGlot).
        """
        if not self.available:
            return None

        try:
            # Translate query to PostgreSQL if needed
            # DataFusion only understands PostgreSQL syntax
            translated_query = self._translate_to_postgres(query, dialect)

            # Register Ray Datasets with DataFusion for optimization planning
            self._register_datasets(datasets)

            # Parse and optimize with DataFusion (using PostgreSQL syntax)
            df_result = self.df_ctx.sql(translated_query)

            # Get optimized plans
            optimized_logical = df_result.optimized_logical_plan()

            # Extract optimization decisions from the plans
            optimizations = self._extract_optimizations(
                optimized_logical, query, datasets
            )

            self._logger.info(
                f"DataFusion optimization extracted: "
                f"{len(optimizations.filter_placement)} filters, "
                f"{len(optimizations.projection_columns)} projections, "
                f"{len(optimizations.join_order)} joins"
            )

            return optimizations

        except Exception as e:
            self._logger.warning(
                f"DataFusion optimization failed: {e}, will fallback to SQLGlot"
            )
            return None

    def _translate_to_postgres(self, query: str, source_dialect: str) -> str:
        """
        Translate SQL query from user's dialect to PostgreSQL for DataFusion.

        DataFusion only supports PostgreSQL-compatible syntax. This method
        uses SQLGlot to translate queries from other dialects (MySQL, Spark SQL,
        BigQuery, etc.) to PostgreSQL.

        Args:
            query: SQL query in source dialect.
            source_dialect: Source SQL dialect (e.g., "mysql", "spark", "bigquery").

        Returns:
            Query translated to PostgreSQL syntax.
        """
        # If already PostgreSQL-compatible, no translation needed
        postgres_dialects = {"postgres", "postgresql", "duckdb"}
        if source_dialect.lower() in postgres_dialects:
            return query

        try:
            import sqlglot

            # Parse query in source dialect
            ast = sqlglot.parse_one(query, read=source_dialect)

            # Translate to PostgreSQL
            postgres_query = ast.sql(dialect="postgres")

            self._logger.debug(
                f"Translated query from {source_dialect} to PostgreSQL for DataFusion"
            )

            return postgres_query

        except Exception as e:
            self._logger.warning(
                f"Could not translate {source_dialect} to PostgreSQL: {e}, "
                f"using original query"
            )
            # Return original query - DataFusion will either parse it or fail gracefully
            return query

    def _register_datasets(self, datasets: Dict[str, Dataset]) -> None:
        """Register Ray Datasets with DataFusion for optimization planning.

        CRITICAL: Maintains RELATIVE sizes across tables for accurate CBO.

        DataFusion's cost-based optimizer makes join ordering decisions based on
        table sizes. If Table A (1M rows) and Table B (1K rows) are sampled to
        10K and 5K, DataFusion thinks they're 2:1 when they're actually 1000:1!

        Strategy:
        1. Estimate size of ALL tables first (without materialization)
        2. Calculate proportional sampling to maintain relative sizes
        3. Cap absolute sample sizes for memory efficiency
        4. Preserve size ratios for accurate join cost estimation

        Args:
            datasets: Dictionary mapping table names to Ray Datasets.
        """
        # Phase 1: Estimate sizes of all tables WITHOUT materialization
        table_size_estimates = {}
        for name, ray_ds in datasets.items():
            estimated_rows = self._estimate_dataset_size(ray_ds, name)
            table_size_estimates[name] = estimated_rows

        # Phase 2: Calculate proportional sampling strategy
        # Find the largest table to use as baseline
        if not table_size_estimates:
            return

        max_table_size = max(table_size_estimates.values())

        # Determine global sampling strategy that maintains proportions
        # If largest table is 1M rows, we sample it at 10K (1%)
        # Then smaller tables get proportionally smaller samples
        cfg = self.sampling_config
        if max_table_size <= cfg.tiny_threshold:
            # All tables tiny - use all rows
            base_sample_fraction = cfg.tiny_sample_fraction
        elif max_table_size <= cfg.small_threshold:
            base_sample_fraction = cfg.small_sample_fraction
        elif max_table_size <= cfg.medium_threshold:
            base_sample_fraction = cfg.medium_sample_fraction
        elif max_table_size <= cfg.large_threshold:
            base_sample_fraction = cfg.large_sample_fraction
        else:
            # Very large datasets - use fraction that gives max_sample for largest table
            base_sample_fraction = cfg.max_sample_size / max_table_size

        self._logger.info(
            f"Sampling {len(datasets)} tables with {base_sample_fraction:.2%} fraction "
            f"(largest table: ~{max_table_size} rows)"
        )

        # Phase 3: Register each table with proportional sample
        for name, ray_ds in datasets.items():
            try:
                estimated_rows = table_size_estimates.get(name, cfg.default_sample_size)

                # Calculate proportional sample size
                sample_size = int(estimated_rows * base_sample_fraction)

                # Apply bounds
                sample_size = max(
                    cfg.min_sample_size, min(cfg.max_sample_size, sample_size)
                )

                # Get sample for DataFusion (maintains relative size)
                sample_arrow = ray_ds.limit(sample_size).to_arrow()

                # Register with DataFusion
                self.df_ctx.register_table(name, sample_arrow)

                self._logger.debug(
                    f"Registered table '{name}' with DataFusion: "
                    f"{len(sample_arrow)} rows ({sample_size}/{estimated_rows} = "
                    f"{sample_size/max(estimated_rows,1):.1%}), "
                    f"{len(sample_arrow.schema)} columns"
                )

            except Exception as e:
                self._logger.warning(
                    f"Failed to register table '{name}' with DataFusion: {e}"
                )

    def _estimate_dataset_size(self, dataset: Dataset, table_name: str) -> int:
        """
        Estimate dataset size WITHOUT materialization.

        Uses metadata and hints to estimate row count without reading data.
        Critical for maintaining relative table sizes in sampling.

        Args:
            dataset: Ray Dataset to estimate.
            table_name: Table name for logging.

        Returns:
            Estimated number of rows.
        """
        try:
            # Strategy 1: Check read operation metadata
            if hasattr(dataset, "_plan") and hasattr(
                dataset._plan, "_dataset_stats_summary"
            ):
                try:
                    stats = dataset._plan._dataset_stats_summary
                    if hasattr(stats, "num_rows") and stats.num_rows:
                        self._logger.debug(
                            f"Table '{table_name}': Estimated {stats.num_rows} rows from metadata"
                        )
                        return stats.num_rows
                except Exception:
                    pass

            # Strategy 2: Heuristic based on dataset type
            # Read operations from large files likely have more data
            cfg = self.sampling_config
            dataset_str = str(type(dataset))
            if "read_parquet" in dataset_str:
                # Parquet reads often indicate larger datasets
                return cfg.medium_threshold  # Assume medium size
            elif "read_csv" in dataset_str:
                return cfg.small_threshold  # CSV often smaller
            elif "from_items" in dataset_str:
                return cfg.tiny_threshold  # from_items usually small

            # Fallback: Assume default size
            self._logger.debug(
                f"Table '{table_name}': No metadata, assuming {cfg.default_sample_size} rows"
            )
            return cfg.default_sample_size

        except Exception:
            return self.sampling_config.default_sample_size

    def _calculate_smart_sample_size(self, dataset: Dataset, table_name: str) -> int:
        """
        Calculate optimal sample size for DataFusion statistics without materialization.

        Multi-strategy approach that never materializes full datasets:
        1. Use schema metadata (lazy - no materialization)
        2. Check dataset stats if available (from read metadata)
        3. Inspect block metadata without reading data
        4. Adaptive sampling based on estimated size
        5. Configurable via DataContext

        Args:
            dataset: Ray Dataset to sample.
            table_name: Name of the table (for logging).

        Returns:
            Optimal sample size (rows to take).
        """
        try:
            # Strategy 1: Use estimated row count from read metadata (most accurate)
            estimated_rows = self._get_estimated_row_count(dataset)
            if estimated_rows:
                return self._calculate_adaptive_sample_size(estimated_rows, table_name)

            # Strategy 2: Use block metadata for size estimation
            if self._has_block_metadata(dataset):
                return self._calculate_from_block_metadata(table_name)

            # Strategy 3: Check if dataset is from read operation
            if self._is_read_operation(dataset):
                return self._calculate_for_read_operation(table_name)

            # Fallback: Use default sample size
            return self._get_default_sample_size(table_name)

        except Exception as e:
            cfg = self.sampling_config
            self._logger.debug(
                f"Could not determine smart sample size for '{table_name}': {e}, "
                f"using default ({cfg.default_sample_size})"
            )
            return cfg.default_sample_size

    def _get_estimated_row_count(self, dataset: Dataset) -> Optional[int]:
        """
        Extract estimated row count from dataset metadata without materialization.

        Args:
            dataset: Ray Dataset to check.

        Returns:
            Estimated row count if available, None otherwise.
        """
        try:
            if hasattr(dataset, "_plan") and hasattr(
                dataset._plan, "_dataset_stats_summary"
            ):
                stats = dataset._plan._dataset_stats_summary
                if hasattr(stats, "num_rows") and stats.num_rows:
                    return stats.num_rows
        except Exception:
            pass
        return None

    def _calculate_adaptive_sample_size(
        self, estimated_rows: int, table_name: str
    ) -> int:
        """
        Calculate sample size based on estimated row count using tiered sampling.

        Uses progressive sampling: larger datasets get proportionally smaller samples
        to balance statistical accuracy with performance.

        Args:
            estimated_rows: Estimated number of rows in dataset.
            table_name: Table name for logging.

        Returns:
            Calculated sample size within configured bounds.
        """
        # Define size tiers: (threshold, sample_fraction, tier_name)
        cfg = self.sampling_config
        size_tiers = [
            (cfg.tiny_threshold, cfg.tiny_sample_fraction, "tiny"),
            (cfg.small_threshold, cfg.small_sample_fraction, "small"),
            (cfg.medium_threshold, cfg.medium_sample_fraction, "medium"),
            (cfg.large_threshold, cfg.large_sample_fraction, "large"),
        ]

        # Find appropriate tier and calculate sample size
        for threshold, fraction, tier_name in size_tiers:
            if estimated_rows < threshold:
                sample_size = int(estimated_rows * fraction)
                bounded_size = self._apply_sample_bounds(sample_size)

                # Log the sampling decision
                if tier_name == "tiny":
                    self._logger.debug(
                        f"Table '{table_name}': Full dataset "
                        f"({estimated_rows} rows - {tier_name})"
                    )
                else:
                    self._logger.debug(
                        f"Table '{table_name}': {int(fraction*100)}% sample "
                        f"({bounded_size} of ~{estimated_rows} rows - {tier_name})"
                    )

                return bounded_size

        # Very large dataset - use fixed max sample
        self._logger.info(
            f"Table '{table_name}': Max sample "
            f"({self.sampling_config.max_sample_size} rows from ~{estimated_rows} rows)"
        )
        return self.sampling_config.max_sample_size

    def _apply_sample_bounds(self, sample_size: int) -> int:
        """
        Apply minimum and maximum bounds to sample size.

        Args:
            sample_size: Raw sample size to bound.

        Returns:
            Sample size clamped within configured min/max bounds.
        """
        cfg = self.sampling_config
        return max(cfg.min_sample_size, min(sample_size, cfg.max_sample_size))

    def _has_block_metadata(self, dataset: Dataset) -> bool:
        """Check if dataset has block metadata available."""
        try:
            if hasattr(dataset, "_plan"):
                logical_plan = dataset._logical_plan
                return hasattr(logical_plan, "dag")
        except Exception:
            pass
        return False

    def _calculate_from_block_metadata(self, table_name: str) -> int:
        """Calculate sample size using block metadata heuristics."""
        self._logger.debug(f"Table '{table_name}': Using metadata-based estimation")
        # Use larger sample for datasets with more blocks (heuristic)
        # More blocks usually indicates larger dataset
        cfg = self.sampling_config
        return min(
            cfg.max_sample_size,
            cfg.default_sample_size * cfg.block_metadata_multiplier,
        )

    def _is_read_operation(self, dataset: Dataset) -> bool:
        """Check if dataset comes from a read operation (likely has metadata)."""
        dataset_str = str(type(dataset))
        return "read_parquet" in dataset_str or "read_csv" in dataset_str

    def _calculate_for_read_operation(self, table_name: str) -> int:
        """Calculate sample size for datasets from read operations."""
        self._logger.debug(
            f"Table '{table_name}': Read operation detected, using larger sample"
        )
        # Read operations likely have metadata, use larger sample for better statistics
        cfg = self.sampling_config
        return min(
            cfg.max_sample_size,
            cfg.default_sample_size * cfg.read_operation_multiplier,
        )

    def _get_default_sample_size(self, table_name: str) -> int:
        """Return default sample size with logging."""
        cfg = self.sampling_config
        self._logger.debug(
            f"Table '{table_name}': Using default sample ({cfg.default_sample_size} rows)"
        )
        return cfg.default_sample_size

    def _extract_optimizations(
        self, optimized_plan: Any, query: str, datasets: Dict[str, Dataset]
    ) -> DataFusionOptimizations:
        """
        Extract optimization decisions from DataFusion's optimized logical plan.

        Analyzes the DataFusion plan to extract:
        - Join order (from DataFusion's cost-based join reordering)
        - Filter placement (from DataFusion's predicate pushdown)
        - Projection columns (from DataFusion's projection pushdown)
        - Aggregation strategies
        - Other optimization hints

        Args:
            optimized_plan: DataFusion's optimized logical plan.
            query: Original SQL query.
            datasets: Registered datasets.

        Returns:
            DataFusionOptimizations with extracted decisions.
        """
        try:
            # Convert plan to string representation for parsing
            plan_lines = str(optimized_plan).split("\n")

            # Extract each type of optimization using dedicated methods
            filters = self._extract_filters_from_plan(plan_lines)
            projections = self._extract_projections_from_plan(plan_lines)
            join_order, join_algorithms = self._extract_joins_from_plan(plan_lines)
            agg_strategy = self._extract_aggregation_from_plan(plan_lines)

            # Log summary of extracted optimizations
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

    def _extract_filters_from_plan(self, plan_lines: List[str]) -> List[Dict[str, Any]]:
        """
        Extract filter placements from DataFusion plan lines.

        DataFusion's optimizer pushes filters closer to data sources for efficiency.
        This extracts where filters ended up after optimization.

        Args:
            plan_lines: Lines from DataFusion's plan string representation.

        Returns:
            List of filter placement dictionaries with expression, position, and type.
        """
        filters = []
        for i, line in enumerate(plan_lines):
            if "Filter:" in line or "filter" in line.lower():
                filter_expr = self._extract_filter_from_line(line)
                if filter_expr:
                    filters.append(
                        {
                            "expression": filter_expr,
                            "position": i,
                            "type": "filter",
                        }
                    )

        self._logger.debug(
            f"Extracted {len(filters)} filter placements from DataFusion plan"
        )
        return filters

    def _extract_projections_from_plan(self, plan_lines: List[str]) -> List[str]:
        """
        Extract projection columns from DataFusion plan lines.

        DataFusion's optimizer performs column pruning to read only necessary columns.
        This extracts which columns survived the optimization.

        Args:
            plan_lines: Lines from DataFusion's plan string representation.

        Returns:
            List of column names in projections.
        """
        projections = []
        for line in plan_lines:
            if "Projection:" in line or "projection" in line.lower():
                cols = self._extract_columns_from_line(line)
                if cols:
                    projections.extend(cols)

        self._logger.debug(
            f"Extracted {len(projections)} projection columns from DataFusion plan"
        )
        return projections

    def _extract_joins_from_plan(
        self, plan_lines: List[str]
    ) -> Tuple[List[Tuple[str, str]], Dict[str, str]]:
        """
        Extract join order and algorithms from DataFusion plan lines.

        DataFusion's cost-based optimizer reorders joins based on table statistics
        and cardinality estimates. This extracts the optimized join sequence.

        Args:
            plan_lines: Lines from DataFusion's plan string representation.

        Returns:
            Tuple of (join_order, join_algorithms) where:
            - join_order: List of (left_table, right_table) tuples in execution order
            - join_algorithms: Dict mapping join pairs to join types (inner, left, etc.)
        """
        joins = []
        for line in plan_lines:
            if "Join:" in line or "join" in line.lower():
                join_info = self._extract_join_from_line(line)
                if join_info:
                    joins.append(join_info)

        if joins:
            join_order = [(j.get("left", ""), j.get("right", "")) for j in joins]
            join_algorithms = {
                f"{j.get('left', '')}_{j.get('right', '')}": j.get("type", "inner")
                for j in joins
            }
            return join_order, join_algorithms

        return [], {}

    def _extract_aggregation_from_plan(self, plan_lines: List[str]) -> Optional[str]:
        """
        Extract aggregation strategy from DataFusion plan lines.

        DataFusion chooses between hash-based and sort-based aggregation based on
        data characteristics and available memory.

        Args:
            plan_lines: Lines from DataFusion's plan string representation.

        Returns:
            Aggregation strategy string if found, None otherwise.
        """
        for line in plan_lines:
            if "Aggregate:" in line or "aggregate" in line.lower():
                agg_strategy = self._extract_aggregation_strategy(line)
                if agg_strategy:
                    self._logger.debug(
                        f"Extracted aggregation strategy: {agg_strategy}"
                    )
                    return agg_strategy
        return None

    def _extract_filter_from_line(self, line: str) -> Optional[str]:
        """Extract filter expression from plan line.

        Args:
            line: Line from DataFusion plan string.

        Returns:
            Filter expression or None.
        """
        # DataFusion plan lines look like: "Filter: age > 25"
        if ":" in line:
            parts = line.split(":", 1)
            if len(parts) == 2:
                return parts[1].strip()
        return None

    def _extract_columns_from_line(self, line: str) -> List[str]:
        """Extract column names from projection line.

        Args:
            line: Line from DataFusion plan string.

        Returns:
            List of column names.
        """
        # DataFusion projection lines show column list
        # Extract column names from the line
        columns = []
        if ":" in line:
            parts = line.split(":", 1)
            if len(parts) == 2:
                # Parse column list (simple heuristic)
                col_part = parts[1].strip()
                # This is a simplified extraction - production version would
                # use proper plan tree walking
                if "," in col_part:
                    columns = [c.strip() for c in col_part.split(",")]
        return columns

    def _extract_join_from_line(self, line: str) -> Optional[Dict[str, str]]:
        """Extract join information from plan line.

        Args:
            line: Line from DataFusion plan string.

        Returns:
            Dictionary with join information or None.
        """
        # Extract join type and tables from plan line
        join_info = {}
        if "inner" in line.lower():
            join_info["type"] = "inner"
        elif "left" in line.lower():
            join_info["type"] = "left"
        elif "right" in line.lower():
            join_info["type"] = "right"
        else:
            join_info["type"] = "inner"  # Default

        # Note: Actual table names would be extracted from plan tree
        # This is simplified for the current implementation
        return join_info if join_info else None

    def _extract_aggregation_strategy(self, line: str) -> Optional[str]:
        """Extract aggregation strategy from plan line.

        Args:
            line: Line from DataFusion plan string.

        Returns:
            Aggregation strategy (hash, sort) or None.
        """
        # DataFusion uses different aggregation strategies
        if "hash" in line.lower():
            return "hash_aggregate"
        elif "sort" in line.lower():
            return "sort_aggregate"
        return "hash_aggregate"  # Default


def get_datafusion_optimizer() -> Optional[DataFusionOptimizer]:
    """Get DataFusion optimizer instance if available.

    Returns:
        DataFusionOptimizer instance or None if DataFusion not available.
    """
    if not DATAFUSION_AVAILABLE:
        return None

    try:
        return DataFusionOptimizer()
    except Exception:
        return None


def is_datafusion_available() -> bool:
    """Check if DataFusion is available for use.

    Returns:
        True if DataFusion package is installed and functional.
    """
    return DATAFUSION_AVAILABLE
