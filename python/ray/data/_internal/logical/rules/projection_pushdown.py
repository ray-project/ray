from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Set, Tuple

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsProjectionPassThrough,
    LogicalOperatorSupportsProjectionPushdown,
    LogicalPlan,
    Rule,
)
from ray.data._internal.logical.operators.map_operator import Project
from ray.data._internal.planner.plan_expression.expression_visitors import (
    _ColumnReferenceCollector,
    _ColumnSubstitutionVisitor,
    _is_col_expr,
)
from ray.data.expressions import (
    AliasExpr,
    ColumnExpr,
    Expr,
    StarExpr,
)

if TYPE_CHECKING:
    from ray.data.block import Schema


def _collect_referenced_columns(exprs: List[Expr]) -> Optional[List[str]]:
    """
    Extract all column names referenced by the given expressions.

    Recursively traverses expression trees to find all ColumnExpr nodes
    and collects their names.

    Example: For expression "col1 + col2", returns {"col1", "col2"}
    """
    # If any expression is star(), we need all columns
    if any(isinstance(expr, StarExpr) for expr in exprs):
        # TODO (goutam): Instead of using None to refer to All columns, resolve the AST against the schema.
        # https://github.com/ray-project/ray/issues/57720
        return None

    collector = _ColumnReferenceCollector()
    for expr in exprs or []:
        collector.visit(expr)

    return collector.get_column_refs()


def _analyze_upstream_project(
    upstream_project: Project,
) -> Tuple[Set[str], dict[str, Expr], Set[str]]:
    """
    Analyze what the upstream project produces and identifies removed columns.

    Example: Upstream exprs [col("x").alias("y")] → removed_by_renames = {"x"} if "x" not in output
    """
    output_column_names = {
        expr.name for expr in upstream_project.exprs if not isinstance(expr, StarExpr)
    }

    # Compose column definitions in the form of a mapping of
    #   - Target column name
    #   - Target expression
    output_column_defs = {
        expr.name: expr for expr in _filter_out_star(upstream_project.exprs)
    }

    # Identify upstream input columns removed by renaming (ie not propagated into
    # its output)
    upstream_column_renaming_map = _extract_input_columns_renaming_mapping(
        upstream_project.exprs
    )

    return (
        output_column_names,
        output_column_defs,
        set(upstream_column_renaming_map.keys()),
    )


def _validate_fusion(
    downstream_project: Project,
    upstream_has_all: bool,
    upstream_output_columns: Set[str],
    removed_by_renames: Set[str],
) -> Tuple[bool, Set[str]]:
    """
    Validate if fusion is possible without rewriting expressions.

    Args:
        downstream_project: The downstream Project operator
        upstream_has_all: True if the upstream Project has all columns, False otherwise
        upstream_output_columns: Set of column names that are available in the upstream Project
        removed_by_renames: Set of column names that are removed by renames in the upstream Project

    Returns:
        Tuple of (is_valid, missing_columns)
        - is_valid: True if all expressions can be fused, False otherwise
        - missing_columns: Set of column names that are referenced but not available

    Example: Downstream refs "x" but upstream renamed "x" to "y" and dropped "x"
             → (False, {"x"})
    """
    missing_columns = set()

    for expr in downstream_project.exprs:
        if isinstance(expr, StarExpr):
            continue

        column_refs = _collect_referenced_columns([expr])
        column_refs_set = set(column_refs or [])

        columns_from_original = column_refs_set - (
            column_refs_set & upstream_output_columns
        )

        # Validate accessibility
        if not upstream_has_all and columns_from_original:
            # Example: Upstream selects ["a", "b"], Downstream refs "c" → can't fuse
            missing_columns.update(columns_from_original)

        if any(col in removed_by_renames for col in columns_from_original):
            # Example: Upstream renames "x" to "y" (dropping "x"), Downstream refs "x" → can't fuse
            removed_cols = {
                col for col in columns_from_original if col in removed_by_renames
            }
            missing_columns.update(removed_cols)

    is_valid = len(missing_columns) == 0
    return is_valid, missing_columns


def _try_fuse(upstream_project: Project, downstream_project: Project) -> Project:
    """
    Attempt to merge two consecutive Project operations into one.

    Example: Upstream: [star(), col("x").alias("y")], Downstream: [star(), (col("y") + 1).alias("z")] → Fused: [star(), (col("x") + 1).alias("z")]
    """
    upstream_has_star: bool = upstream_project.has_star_expr()

    # TODO add validations that
    #   - exprs only depend on input attrs (ie no dep on output of other exprs)

    # Analyze upstream
    (
        upstream_output_cols,
        upstream_column_defs,
        upstream_input_cols_removed,
    ) = _analyze_upstream_project(upstream_project)

    # Validate fusion possibility
    is_valid, missing_columns = _validate_fusion(
        downstream_project,
        upstream_has_star,
        upstream_output_cols,
        upstream_input_cols_removed,
    )

    if not is_valid:
        # Raise KeyError to match expected error type in tests
        raise KeyError(
            f"Column(s) {sorted(missing_columns)} not found. "
            f"Available columns: {sorted(upstream_output_cols) if not upstream_has_star else 'all columns (has star)'}"
        )

    # Following invariants are upheld for each ``Project`` logical op:
    #
    #   1. ``Project``s list of expressions are bound to op's input columns **only**
    #   (ie there could be no inter-dependency b/w expressions themselves)
    #
    #   2. `Each of expressions on the `Project``s list constitutes an output
    #   column definition, where column's name is derived from ``expr.name`` and
    #   column itself is derived by executing that expression against the op's
    #   input block.
    #
    # Therefore to abide by and satisfy aforementioned invariants, when fusing
    # 2 ``Project`` operators, following scenarios are considered:
    #
    #   1. Composition: downstream including (and potentially renaming) upstream
    #      output columns (this is the case when downstream holds ``StarExpr``).
    #
    #   2. Projection: downstream projecting upstream output columns (by for ex,
    #      only selecting & transforming some of the upstream output columns).
    #

    # Upstream output column refs inside downstream expressions need to be bound
    # to upstream output column definitions to satisfy invariant #1 (common for both
    # composition/projection cases)
    v = _ColumnSubstitutionVisitor(upstream_column_defs)

    rebound_downstream_exprs = [
        v.visit(e) for e in _filter_out_star(downstream_project.exprs)
    ]

    if not downstream_project.has_star_expr():
        # Projection case: this is when downstream is a *selection* (ie, not including
        # the upstream columns with ``StarExpr``)
        #
        # Example:
        #   Upstream: Project([col("a").alias("b")])
        #   Downstream: Project([col("b").alias("c")])
        #
        #   Result: Project([col("a").alias("c")])
        new_exprs = rebound_downstream_exprs
    else:
        # Composition case: downstream has ``StarExpr`` (entailing that downstream
        # output will be including all of the upstream output columns)
        #
        # Example 1:
        #   Upstream: [star(), col("a").alias("b")],
        #   Downstream: [star(), col("b").alias("c")]
        #
        #   Result: [star(), col("a").alias("b"), col("a").alias("c")]
        #
        # Example 2:
        #   Input (columns): ["a", "b"]
        #   Upstream: [star({"b": "z"}), col("a").alias("x")],
        #   Downstream: [star({"x": "y"}), col("z")]
        #
        #   Result: [star(), col("a").alias("y"), col("b").alias("z")]

        # Extract downstream's input column rename map (downstream inputs are
        # upstream's outputs)
        downstream_input_column_rename_map = _extract_input_columns_renaming_mapping(
            downstream_project.exprs
        )
        # Collect upstream output column expression "projected" to become
        # downstream expressions
        projected_upstream_output_col_exprs = []

        # When fusing 2 projections
        for e in upstream_project.exprs:
            # NOTE: We have to filter out upstream output columns that are
            #       being *renamed* by downstream expression
            if e.name not in downstream_input_column_rename_map:
                projected_upstream_output_col_exprs.append(e)

        new_exprs = projected_upstream_output_col_exprs + rebound_downstream_exprs

    return Project(
        upstream_project.input_dependency,
        exprs=new_exprs,
        ray_remote_args=downstream_project._ray_remote_args,
    )


def _filter_out_star(exprs: List[Expr]) -> List[Expr]:
    return [e for e in exprs if not isinstance(e, StarExpr)]


def _create_upstream_alias_projection(
    columns_to_rename: Iterable[str],
    column_rename_map: Dict[str, str],
    input_op: LogicalOperator,
) -> Project:
    """Create an upstream Project operator that renames columns.
    This is used for ProjectionPassThrough."""
    from ray.data.expressions import col

    new_exprs: List[Expr] = []
    for old_col in columns_to_rename:
        if old_col in column_rename_map:
            new_col = column_rename_map[old_col]
            new_exprs.append(col(old_col).alias(new_col))
        else:
            new_exprs.append(col(old_col))

    return Project(
        input_op=input_op,
        exprs=new_exprs,
        compute=None,
        ray_remote_args=None,
    )


def _create_downstream_selection_project(
    column_rename_map: Dict[str, str],
    input_op: LogicalOperator,
) -> Project:
    """Create a downstream Project operator that selects only the renamed columns.
    This is used for ProjectionPassThrough."""
    from ray.data.expressions import col

    new_exprs = []
    for new_col in column_rename_map.values():
        new_exprs.append(col(new_col))

    return Project(
        input_op=input_op,
        exprs=new_exprs,
        compute=None,
        ray_remote_args=None,
    )


def _get_columns_to_rename_for_input(
    use_schema_analysis: bool,
    schema: Optional["Schema"],
    column_rename_map: Dict[str, str],
    input_op_referenced_keys: List[str],
) -> Optional[List[str]]:
    """Determine which columns to rename for a specific input.

    Returns:
        List of columns to include in the upstream project, or None if no project needed.
    """
    match (use_schema_analysis, schema is None):
        case (True, True):
            # Schema-based analysis required but schema unavailable
            return None

        case (True, False):
            # Schema-based: Only push columns that exist in this input (Join, Zip)
            input_schema_cols: Set[str] = set(schema.names)
            projected_cols_in_input: Set[str] = (
                set(column_rename_map.keys()) & input_schema_cols
            )
            return list(projected_cols_in_input | set(input_op_referenced_keys))

        case (False, _):
            # Non-schema-based: Push all columns to all branches
            return list(set(column_rename_map.keys()) | set(input_op_referenced_keys))


class ProjectionPushdown(Rule):
    """
    Optimization rule that pushes projections (column selections) down the query plan.

    This rule performs two optimizations:
    1. Fuses consecutive Project operations to eliminate redundant projections
    2. Pushes projections into data sources (e.g., Read operations) to enable
       column pruning at the storage layer
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        """Apply projection pushdown optimization to the entire plan."""
        dag = plan.dag
        new_dag = dag._apply_transform(self._try_fuse_projects)
        new_dag = new_dag._apply_transform(self._push_projection_into_read_op)
        new_dag = new_dag._apply_transform(self._pass_projection_through_op)
        return LogicalPlan(new_dag, plan.context) if dag is not new_dag else plan

    @classmethod
    def _try_fuse_projects(cls, op: LogicalOperator) -> LogicalOperator:
        """
        Optimize a single Project operator.

        Steps:
        1. Iteratively fuse with upstream Project operations
        2. Push the resulting projection into the data source if possible
        """
        if not isinstance(op, Project):
            return op

        # Step 1: Iteratively fuse with upstream Project operations
        current_project: Project = op

        if not isinstance(current_project.input_dependency, Project):
            return op

        upstream_project: Project = current_project.input_dependency  # type: ignore[assignment]

        fused = _try_fuse(upstream_project, current_project)

        return fused

    @classmethod
    def _push_projection_into_read_op(cls, op: LogicalOperator) -> LogicalOperator:

        if not isinstance(op, Project):
            return op

        current_project: Project = op

        # Step 2: Push projection into the data source if supported
        input_op = current_project.input_dependency
        if (
            isinstance(input_op, LogicalOperatorSupportsProjectionPushdown)
            and input_op.supports_projection_pushdown()
        ):
            if current_project.has_star_expr():
                # If project has a star, then projection is not feasible
                required_columns = None
            else:
                # Otherwise, collect required columns to push projection down
                # into the reader
                required_columns = _collect_referenced_columns(current_project.exprs)

            # Check if it's a simple projection that could be pushed into
            # read as a whole
            is_projection = all(
                _is_col_expr(expr) for expr in _filter_out_star(current_project.exprs)
            )

            if is_projection:
                # NOTE: We only can rename output columns when it's a simple
                #       projection and Project operator is discarded (otherwise
                #       it might be holding expression referencing attributes
                #       by original their names prior to renaming)
                #
                # TODO fix by instead rewriting exprs
                output_column_rename_map = _extract_input_columns_renaming_mapping(
                    current_project.exprs
                )

                # Determine columns to project
                if required_columns is None:
                    # All columns case - need to determine available columns
                    if not output_column_rename_map:
                        # No renames and all columns - pass through as None
                        projection_map = None
                    else:
                        # Has renames - get the list of columns to apply renames to
                        current_projection = input_op.get_projection_map()

                        if current_projection is not None:
                            # Use output column names from existing projection (for chained renames)
                            columns = list(current_projection.values())
                        else:
                            # No existing projection - get all columns from schema
                            schema = input_op._cached_output_metadata.schema
                            if schema is not None:
                                columns = schema.names
                            else:
                                # Cannot determine available columns - this shouldn't happen in practice
                                # for properly implemented datasources. Rather than guessing, raise an error.
                                raise RuntimeError(
                                    f"Cannot apply rename operation: schema unavailable for input operator "
                                    f"{input_op}. This may indicate a legacy datasource that doesn't properly "
                                    f"expose schema information."
                                )

                        # Build projection_map: apply renames to all columns
                        projection_map = {
                            col: output_column_rename_map.get(col, col)
                            for col in columns
                        }
                else:
                    # Specific columns selected - build projection_map with renames applied
                    projection_map = {
                        col: output_column_rename_map[col] for col in required_columns
                    }

                # Apply projection to the read op
                return input_op.apply_projection(projection_map)
            else:
                # Complex expressions - apply projection without full rename
                projection_map = (
                    None
                    if required_columns is None
                    else {col: col for col in required_columns}
                )
                projected_input_op = input_op.apply_projection(projection_map)

                # Has transformations: Keep Project on top of optimized Read
                return Project(
                    projected_input_op,
                    exprs=current_project.exprs,
                    ray_remote_args=current_project._ray_remote_args,
                )

        return current_project

    @classmethod
    def _pass_projection_through_op(cls, op: LogicalOperator) -> LogicalOperator:
        """Pass projections through operators that support it. This can optimize shuffle
        performance by only selecting the keys/columns that the user chose.
        """
        if not isinstance(op, Project):
            return op

        curr_project: Project = op

        # Simple projections are those that use col() or alias()
        # no star() exprs, no binary(), etc...
        is_simple_projection = all(
            _is_col_expr(expr=expr) for expr in curr_project.exprs
        )

        # This is the candidate op to pass through
        op_to_pass_through = curr_project.input_dependency

        if not (
            isinstance(op_to_pass_through, LogicalOperatorSupportsProjectionPassThrough)
            and is_simple_projection
        ):
            return op

        # Collect input columns for current projection
        curr_project_referenced_cols = _collect_referenced_columns(
            exprs=curr_project.exprs
        )
        assert (
            curr_project_referenced_cols is not None
        ), "Projection pass through is not supported for star(*) expressions"

        # Create a mapping from old name -> new name
        old_to_new_name_map: Dict[str, str] = _extract_input_columns_renaming_mapping(
            projection_exprs=curr_project.exprs
        )

        # Filter out any columns not referenced by the project.
        filtered_old_to_new_name_map: Dict[str, str] = {
            # col(a).alias(b) or col(a)
            col: old_to_new_name_map.get(col, col)
            for col in curr_project_referenced_cols
        }

        # Apply unified projection pass-through logic
        return cls._apply_projection_pass_through(
            op=op_to_pass_through,
            column_rename_map=filtered_old_to_new_name_map,
        )

    @classmethod
    def _apply_projection_pass_through(
        cls,
        op: LogicalOperatorSupportsProjectionPassThrough,
        column_rename_map: Dict[str, str],
    ) -> LogicalOperator:
        """Unified projection pass-through logic for all operators.

        This method implements the 4 key steps:
        1. Conditionally use schema analysis (if requires_schema_based_branch_selection() is True)
        2. Always create upstream project(s)
        3. Conditionally rename keys (if get_referenced_keys() is not None)
        4. Conditionally create downstream project (if keys not in output)

        Example: Join with keys NOT in output (most complex case)

        Before:
            Project[select left.name, right.value]
                |
            Join[left.id = right.id]
               / \
            Left                Right
            [id=1, name="A"]    [id=1, value=100]
            [id=2, name="B"]    [id=2, value=200]

        After:
            Project[select name, value]  <-- Step 4: downstream (removes join keys)
                |
            Join[left.id = right.id]
               / \
              /   \
        Project   Project  <-- Step 2: upstream (schema-based selection + keys)
        [id,name] [id,value]
           |         |
        Left      Right

        Step-by-step breakdown:
        1. Schema analysis: Left has [id,name], Right has [id,value]
                          Project needs [name] from Left, [value] from Right
        2. Upstream projects: Left gets [id,name] (adds join key)
                            Right gets [id,value] (adds join key)
        3. Rename keys: left.id -> id, right.id -> id
        4. Downstream project: Remove id columns, keep only [name, value]
        """
        use_schema_analysis: bool = op.requires_schema_based_branch_selection()
        referenced_keys_per_input: List[List[str]] = op.get_referenced_keys()

        upstream_projects: List[Project] = []
        renamed_keys_per_input: List[List[str]] = []

        assert len(referenced_keys_per_input) == len(op.input_dependencies), (
            f"ProjectionPushdown rule expected number of inputs to be equal to "
            f"the number of input keys, but found mismatch: "
            f"len(input_keys)={len(referenced_keys_per_input)} vs "
            f"len(input_operators)={len(op.input_dependencies)}. "
            f"This is a bug in the Ray Data."
        )

        for input_op, input_op_referenced_keys in zip(
            op.input_dependencies, referenced_keys_per_input
        ):
            if op in input_op.output_dependencies:
                input_op.output_dependencies.remove(op)

            schema: Optional["Schema"] = input_op.infer_schema()

            # Step 1: Determine which columns to include in the upstream project
            columns_to_rename: Optional[Set[str]] = _get_columns_to_rename_for_input(
                use_schema_analysis=use_schema_analysis,
                schema=schema,
                column_rename_map=column_rename_map,
                input_op_referenced_keys=input_op_referenced_keys,
            )

            if columns_to_rename is None:
                # Short-circuit: Schema unavailable, use original input
                upstream_projects.append(input_op)
                # No rename needed
                renamed_keys_per_input.append(input_op_referenced_keys)
                continue

            # Step 2: Create upstream project
            upstream_project = _create_upstream_alias_projection(
                columns_to_rename=columns_to_rename,
                column_rename_map=column_rename_map,
                input_op=input_op,
            )
            upstream_projects.append(upstream_project)

            # Step 3: Rename keys for this input
            input_op_renamed_keys = [
                column_rename_map.get(old_key, old_key)
                for old_key in input_op_referenced_keys
            ]
            renamed_keys_per_input.append(input_op_renamed_keys)

        # Recreate operator with new inputs and renamed keys
        new_op: LogicalOperator = op.apply_projection_pass_through(
            renamed_keys=renamed_keys_per_input,
            upstream_projects=upstream_projects,
        )

        # Step 4: Create downstream project if keys aren't all in output
        # Flatten all keys from all inputs
        all_keys: Set[str] = set()
        for keys in referenced_keys_per_input:
            all_keys.update(keys)

        # Check if all keys are in the output projection
        if all_keys and not all(key in column_rename_map.keys() for key in all_keys):
            return _create_downstream_selection_project(
                column_rename_map=column_rename_map,
                input_op=new_op,
            )

        return new_op


def _extract_input_columns_renaming_mapping(
    projection_exprs: List[Expr],
) -> Dict[str, str]:
    """Fetches renaming mapping of all input columns names being renamed (replaced)
    in alias() expressions. The format is source column name -> new column name.
    """

    return dict(
        [
            _get_renaming_mapping(expr)
            for expr in _filter_out_star(projection_exprs)
            if _is_renaming_expr(expr)
        ]
    )


def _get_renaming_mapping(expr: Expr) -> Tuple[str, str]:
    assert _is_renaming_expr(expr)

    alias: AliasExpr = expr

    return alias.expr.name, alias.name


def _is_renaming_expr(expr: Expr) -> bool:
    is_renaming = isinstance(expr, AliasExpr) and expr._is_rename

    assert not is_renaming or isinstance(
        expr.expr, ColumnExpr
    ), f"Renaming expression expected to be of the shape alias(col('source'), 'target') (got {expr})"

    return is_renaming
