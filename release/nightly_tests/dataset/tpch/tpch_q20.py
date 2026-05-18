import ray
from ray.data.aggregate import Sum
from ray.data.expressions import col
from common import parse_tpch_args, load_table, to_f64, run_tpch_benchmark


def main(args):
    def benchmark_fn():
        from datetime import datetime

        # Q20: Potential Part Promotion Query
        # Identify suppliers in a given nation having parts with excess
        # inventory (available quantity exceeds 50% of the quantity shipped
        # in a given year for forest-colored parts).
        #
        # Equivalent SQL:
        #   SELECT s_name, s_address
        #   FROM supplier, nation
        #   WHERE s_suppkey IN (
        #       SELECT ps_suppkey
        #       FROM partsupp
        #       WHERE ps_partkey IN (
        #           SELECT p_partkey
        #           FROM part
        #           WHERE p_name LIKE 'forest%'
        #       )
        #       AND ps_availqty > (
        #           SELECT 0.5 * SUM(l_quantity)
        #           FROM lineitem
        #           WHERE l_partkey = ps_partkey
        #             AND l_suppkey = ps_suppkey
        #             AND l_shipdate >= DATE '1994-01-01'
        #             AND l_shipdate < DATE '1995-01-01'
        #       )
        #   )
        #   AND s_nationkey = n_nationkey
        #   AND n_name = 'CANADA'
        #   ORDER BY s_name;
        #
        # Note:
        # The innermost IN subquery is a simple part filter turned into a
        # left_semi join. The correlated scalar subquery on lineitem is
        # decorrelated by pre-aggregating SUM(l_quantity) grouped by
        # (l_partkey, l_suppkey), then joining with partsupp on the same
        # composite key and applying the threshold filter.

        # Load tables with early projection.
        supplier = load_table("supplier", args.sf).select_columns(
            ["s_suppkey", "s_name", "s_address", "s_nationkey"]
        )
        nation = load_table("nation", args.sf).select_columns(["n_nationkey", "n_name"])
        part = load_table("part", args.sf).select_columns(["p_partkey", "p_name"])
        partsupp = load_table("partsupp", args.sf).select_columns(
            ["ps_partkey", "ps_suppkey", "ps_availqty"]
        )
        lineitem = load_table("lineitem", args.sf).select_columns(
            ["l_partkey", "l_suppkey", "l_quantity", "l_shipdate"]
        )

        # Q20 parameters
        color = "forest"
        nation_name = "CANADA"
        date_start = datetime(1994, 1, 1)
        date_end = datetime(1995, 1, 1)

        # ── Innermost subquery: forest parts ────────────────────────────
        forest_parts = part.filter(
            expr=col("p_name").str.starts_with(color)
        ).select_columns(["p_partkey"])

        # ── Restrict partsupp to forest parts (IN subquery) ─────────────
        ps_forest = partsupp.join(
            forest_parts,
            join_type="left_semi",
            num_partitions=16,
            on=("ps_partkey",),
            right_on=("p_partkey",),
        )

        # ── Decorrelate scalar subquery on lineitem ─────────────────────
        # Pre-aggregate: SUM(l_quantity) grouped by (l_partkey, l_suppkey)
        # for lineitems in the target date range.
        li_filtered = lineitem.filter(
            expr=(col("l_shipdate") >= date_start) & (col("l_shipdate") < date_end)
        )
        li_agg = li_filtered.groupby(["l_partkey", "l_suppkey"]).aggregate(
            Sum(on="l_quantity", alias_name="sum_qty")
        )
        li_agg = li_agg.with_column("sum_qty_f", to_f64(col("sum_qty"))).select_columns(
            ["l_partkey", "l_suppkey", "sum_qty_f"]
        )

        # ── Join partsupp with lineitem aggregate and apply threshold ───
        ps_forest = ps_forest.with_column("ps_availqty_f", to_f64(col("ps_availqty")))
        ps_li = ps_forest.join(
            li_agg,
            join_type="inner",
            num_partitions=16,
            on=("ps_partkey", "ps_suppkey"),
            right_on=("l_partkey", "l_suppkey"),
        )
        qualified_ps = ps_li.filter(
            expr=col("ps_availqty_f") > 0.5 * col("sum_qty_f")
        ).select_columns(["ps_suppkey"])

        # ── Main pipeline: Canadian suppliers with qualifying parts ─────
        nation_filtered = nation.filter(expr=col("n_name") == nation_name)
        canadian_suppliers = supplier.join(
            nation_filtered,
            join_type="inner",
            num_partitions=16,
            on=("s_nationkey",),
            right_on=("n_nationkey",),
        ).select_columns(["s_suppkey", "s_name", "s_address"])

        result = canadian_suppliers.join(
            qualified_ps,
            join_type="left_semi",
            num_partitions=16,
            on=("s_suppkey",),
            right_on=("ps_suppkey",),
        )

        _ = (
            result.select_columns(["s_name", "s_address"])
            .sort(key="s_name")
            .materialize()
        )

        # Report arguments for the benchmark.
        return vars(args)

    run_tpch_benchmark("tpch_q20", benchmark_fn)


if __name__ == "__main__":
    ray.init()
    args = parse_tpch_args()
    main(args)
