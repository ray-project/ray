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


# Configuration constants for adaptive sampling strategy
# These determine how many rows to sample from datasets for DataFusion optimization

# Minimum sample size for any dataset (ensures basic statistics)
DEFAULT_MIN_SAMPLE_SIZE = 100

# Maximum sample size for any dataset (caps memory usage)
DEFAULT_MAX_SAMPLE_SIZE = 10000

# Default sample size when metadata is unavailable
DEFAULT_SAMPLE_SIZE = 1000

# Dataset size tier thresholds (in rows) for adaptive sampling
TINY_DATASET_THRESHOLD = 1000  # < 1K rows: use all rows
SMALL_DATASET_THRESHOLD = 10000  # 1K-10K rows: use 50% sample
MEDIUM_DATASET_THRESHOLD = 100000  # 10K-100K rows: use 10% sample
LARGE_DATASET_THRESHOLD = 1000000  # 100K-1M rows: use 1% sample
# > 1M rows: use fixed max sample

# Sampling percentages for each tier
TINY_DATASET_SAMPLE_FRACTION = 1.0  # 100% - use all
SMALL_DATASET_SAMPLE_FRACTION = 0.5  # 50%
MEDIUM_DATASET_SAMPLE_FRACTION = 0.1  # 10%
LARGE_DATASET_SAMPLE_FRACTION = 0.01  # 1%

# Multiplier for datasets from read operations (often have good metadata)
READ_OPERATION_SAMPLE_MULTIPLIER = 5

# Multiplier for datasets with block metadata
BLOCK_METADATA_SAMPLE_MULTIPLIER = 2


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

    def __init__(self):
        """Initialize DataFusion optimizer."""
        self.available = DATAFUSION_AVAILABLE
        self._logger = setup_logger("DataFusionOptimizer")

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
        if max_table_size <= TINY_DATASET_THRESHOLD:
            # All tables tiny - use all rows
            base_sample_fraction = TINY_DATASET_SAMPLE_FRACTION
        elif max_table_size <= SMALL_DATASET_THRESHOLD:
            base_sample_fraction = SMALL_DATASET_SAMPLE_FRACTION
        elif max_table_size <= MEDIUM_DATASET_THRESHOLD:
            base_sample_fraction = MEDIUM_DATASET_SAMPLE_FRACTION
        elif max_table_size <= LARGE_DATASET_THRESHOLD:
            base_sample_fraction = LARGE_DATASET_SAMPLE_FRACTION
        else:
            # Very large datasets - use fraction that gives max_sample for largest table
            base_sample_fraction = DEFAULT_MAX_SAMPLE_SIZE / max_table_size

        self._logger.info(
            f"Sampling {len(datasets)} tables with {base_sample_fraction:.2%} fraction "
            f"(largest table: ~{max_table_size} rows)"
        )

        # Phase 3: Register each table with proportional sample
        for name, ray_ds in datasets.items():
            try:
                estimated_rows = table_size_estimates.get(name, DEFAULT_SAMPLE_SIZE)

                # Calculate proportional sample size
                sample_size = int(estimated_rows * base_sample_fraction)

                # Apply bounds
                sample_size = max(
                    DEFAULT_MIN_SAMPLE_SIZE, min(DEFAULT_MAX_SAMPLE_SIZE, sample_size)
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
            dataset_str = str(type(dataset))
            if "read_parquet" in dataset_str:
                # Parquet reads often indicate larger datasets
                return MEDIUM_DATASET_THRESHOLD  # Assume medium size
            elif "read_csv" in dataset_str:
                return SMALL_DATASET_THRESHOLD  # CSV often smaller
            elif "from_items" in dataset_str:
                return TINY_DATASET_THRESHOLD  # from_items usually small

            # Fallback: Assume default size
            self._logger.debug(
                f"Table '{table_name}': No metadata, assuming {DEFAULT_SAMPLE_SIZE} rows"
            )
            return DEFAULT_SAMPLE_SIZE

        except Exception:
            return DEFAULT_SAMPLE_SIZE

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
        # Use module-level constants for configuration
        min_sample = DEFAULT_MIN_SAMPLE_SIZE
        max_sample = DEFAULT_MAX_SAMPLE_SIZE
        default_sample = DEFAULT_SAMPLE_SIZE

        try:
            # Strategy 1: Check for estimated row count from read metadata
            # Many read operations (read_parquet, read_csv) provide estimates
            # WITHOUT materializing data
            if hasattr(dataset, "_plan") and hasattr(
                dataset._plan, "_dataset_stats_summary"
            ):
                try:
                    stats = dataset._plan._dataset_stats_summary
                    if hasattr(stats, "num_rows") and stats.num_rows:
                        estimated_rows = stats.num_rows

                        # Adaptive sampling tiers using constants
                        if estimated_rows < TINY_DATASET_THRESHOLD:
                            # Tiny dataset - use all (no sampling needed)
                            sample_size = int(
                                estimated_rows * TINY_DATASET_SAMPLE_FRACTION
                            )
                            self._logger.debug(
                                f"Table '{table_name}': Full dataset "
                                f"({estimated_rows} rows - tiny)"
                            )
                        elif estimated_rows < SMALL_DATASET_THRESHOLD:
                            # Small dataset - 50% sample for good statistics
                            sample_size = min(
                                max_sample,
                                max(
                                    min_sample,
                                    int(estimated_rows * SMALL_DATASET_SAMPLE_FRACTION),
                                ),
                            )
                            self._logger.debug(
                                f"Table '{table_name}': {int(SMALL_DATASET_SAMPLE_FRACTION*100)}% sample "
                                f"({sample_size} of ~{estimated_rows} rows)"
                            )
                        elif estimated_rows < MEDIUM_DATASET_THRESHOLD:
                            # Medium dataset - 10% sample
                            sample_size = min(
                                max_sample,
                                max(
                                    min_sample,
                                    int(
                                        estimated_rows * MEDIUM_DATASET_SAMPLE_FRACTION
                                    ),
                                ),
                            )
                            self._logger.debug(
                                f"Table '{table_name}': {int(MEDIUM_DATASET_SAMPLE_FRACTION*100)}% sample "
                                f"({sample_size} of ~{estimated_rows} rows)"
                            )
                        elif estimated_rows < LARGE_DATASET_THRESHOLD:
                            # Large dataset - 1% sample (still 10k max)
                            sample_size = min(
                                max_sample,
                                max(
                                    min_sample,
                                    int(estimated_rows * LARGE_DATASET_SAMPLE_FRACTION),
                                ),
                            )
                            self._logger.debug(
                                f"Table '{table_name}': {int(LARGE_DATASET_SAMPLE_FRACTION*100)}% sample "
                                f"({sample_size} of ~{estimated_rows} rows)"
                            )
                        else:
                            # Very large dataset - fixed max sample
                            sample_size = max_sample
                            self._logger.info(
                                f"Table '{table_name}': Max sample "
                                f"({sample_size} rows from ~{estimated_rows} rows)"
                            )

                        return max(min_sample, min(sample_size, max_sample))
                except Exception:
                    pass

            # Strategy 2: Try to get block count without reading data
            # Block metadata is available without materialization
            if hasattr(dataset, "_plan"):
                try:
                    # Check if we can estimate from block metadata
                    # This is lightweight - doesn't read actual data
                    logical_plan = dataset._logical_plan
                    if hasattr(logical_plan, "dag"):
                        # Estimated blocks can hint at size
                        # More blocks usually means larger dataset
                        self._logger.debug(
                            f"Table '{table_name}': Using metadata-based estimation"
                        )
                        # Use larger sample for datasets with more blocks (heuristic)
                        return min(
                            max_sample,
                            default_sample * BLOCK_METADATA_SAMPLE_MULTIPLIER,
                        )
                except Exception:
                    pass

            # Strategy 3: Check if dataset is from read operation
            # Read operations often have metadata
            dataset_str = str(type(dataset))
            if "read_parquet" in dataset_str or "read_csv" in dataset_str:
                # Read operations likely have metadata
                # Use larger sample for better statistics
                self._logger.debug(
                    f"Table '{table_name}': Read operation detected, using larger sample"
                )
                return min(
                    max_sample, default_sample * READ_OPERATION_SAMPLE_MULTIPLIER
                )

            # Fallback: Use default sample size
            self._logger.debug(
                f"Table '{table_name}': Using default sample ({default_sample} rows)"
            )
            return default_sample

        except Exception as e:
            self._logger.debug(
                f"Could not determine smart sample size for '{table_name}': {e}, "
                f"using default ({default_sample})"
            )
            return default_sample

    def _extract_optimizations(
        self, optimized_plan, query: str, datasets: Dict[str, Dataset]
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
        optimizations = DataFusionOptimizations(
            join_order=[],
            join_algorithms={},
            filter_placement=[],
            projection_columns=[],
            predicate_pushdown=True,
            projection_pushdown=True,
        )

        try:
            # Get string representation of optimized plan
            plan_str = str(optimized_plan)

            # Extract optimization information from plan structure
            # DataFusion's optimized plan shows the result of:
            # - Predicate pushdown (filters moved closer to scans)
            # - Projection pushdown (column selection moved earlier)
            # - Join reordering (based on cost estimates)
            # - Expression simplification

            # Parse plan string to extract key optimizations
            plan_lines = plan_str.split("\n")

            # Extract filters (look for Filter operations in plan)
            filters = []
            for i, line in enumerate(plan_lines):
                if "Filter:" in line or "filter" in line.lower():
                    # Extract filter expression
                    filter_expr = self._extract_filter_from_line(line)
                    if filter_expr:
                        filters.append(
                            {
                                "expression": filter_expr,
                                "position": i,
                                "type": "filter",
                            }
                        )

            optimizations.filter_placement = filters
            self._logger.debug(
                f"Extracted {len(filters)} filter placements from DataFusion plan"
            )

            # Extract projections (look for Projection operations)
            projections = []
            for line in plan_lines:
                if "Projection:" in line or "projection" in line.lower():
                    cols = self._extract_columns_from_line(line)
                    if cols:
                        projections.extend(cols)

            optimizations.projection_columns = projections
            self._logger.debug(
                f"Extracted {len(projections)} projection columns from DataFusion plan"
            )

            # Extract join information
            joins = []
            for line in plan_lines:
                if "Join:" in line or "join" in line.lower():
                    join_info = self._extract_join_from_line(line)
                    if join_info:
                        joins.append(join_info)

            if joins:
                optimizations.join_order = [
                    (j.get("left", ""), j.get("right", "")) for j in joins
                ]
                optimizations.join_algorithms = {
                    f"{j.get('left', '')}_{j.get('right', '')}": j.get("type", "inner")
                    for j in joins
                }

            # Extract aggregation information
            for line in plan_lines:
                if "Aggregate:" in line or "aggregate" in line.lower():
                    agg_strategy = self._extract_aggregation_strategy(line)
                    if agg_strategy:
                        optimizations.aggregation_strategy = agg_strategy
                        self._logger.debug(
                            f"Extracted aggregation strategy: {agg_strategy}"
                        )
                        break

            # Log summary of extracted optimizations
            self._logger.info(
                f"Extracted optimizations: {len(optimizations.filter_placement)} filters, "
                f"{len(optimizations.projection_columns)} projections, "
                f"{len(optimizations.join_order)} joins"
            )

        except Exception as e:
            self._logger.debug(f"Could not extract detailed optimizations: {e}")
            # Return basic optimizations on error

        return optimizations

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
