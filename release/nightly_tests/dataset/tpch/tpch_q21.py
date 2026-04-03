import ray
from ray.data.aggregate import Count, CountDistinct
from ray.data.expressions import col
from common import parse_tpch_args, load_table, run_tpch_benchmark


def main(args):
    def benchmark_fn():
        # Q21: Suppliers Who Kept Orders Waiting Query
        # Identify suppliers in a given nation whose shipments were received
        # late, where at least one other supplier also filled the same order
        # but none of those other suppliers delivered late.
        #
        # Equivalent SQL:
        #   SELECT s_name, COUNT(*) AS numwait
        #   FROM supplier, lineitem l1, orders, nation
        #   WHERE s_suppkey = l1.l_suppkey
        #     AND o_orderkey = l1.l_orderkey
        #     AND o_orderstatus = 'F'
        #     AND l1.l_receiptdate > l1.l_commitdate
        #     AND EXISTS (
        #         SELECT * FROM lineitem l2
        #         WHERE l2.l_orderkey = l1.l_orderkey
        #           AND l2.l_suppkey <> l1.l_suppkey
        #     )
        #     AND NOT EXISTS (
        #         SELECT * FROM lineitem l3
        #         WHERE l3.l_orderkey = l1.l_orderkey
        #           AND l3.l_suppkey <> l1.l_suppkey
        #           AND l3.l_receiptdate > l3.l_commitdate
        #     )
        #     AND s_nationkey = n_nationkey
        #     AND n_name = 'SAUDI ARABIA'
        #   GROUP BY s_name
        #   ORDER BY numwait DESC, s_name
        #   LIMIT 100;
        #
        # Note:
        # The EXISTS and NOT EXISTS subqueries both use inequality predicates
        # (l_suppkey <> l1.l_suppkey) which cannot be expressed as equi-join
        # conditions. Instead we decorrelate them using pre-aggregated counts:
        #   - EXISTS (another supplier for the same order)
        #       ⟺ COUNT(DISTINCT l_suppkey) per order > 1
        #   - NOT EXISTS (no other LATE supplier for the same order)
        #       ⟺ COUNT(DISTINCT l_suppkey) among late lineitems per order == 1
        #     (since l1 itself is the only late supplier)

        # Load tables with early projection.
        supplier = load_table("supplier", args.sf).select_columns(
            ["s_suppkey", "s_name", "s_nationkey"]
        )
        lineitem = load_table("lineitem", args.sf).select_columns(
            ["l_orderkey", "l_suppkey", "l_receiptdate", "l_commitdate"]
        )
        orders = load_table("orders", args.sf).select_columns(
            ["o_orderkey", "o_orderstatus"]
        )
        nation = load_table("nation", args.sf).select_columns(["n_nationkey", "n_name"])

        # Q21 parameters
        nation_name = "SAUDI ARABIA"

        # ── Pre-aggregate: distinct suppliers per order (EXISTS) ────────
        # If an order has > 1 distinct supplier, there exists "another"
        # supplier for any given supplier on that order.
        # Filter early to reduce the right-side dataset size before join.
        suppliers_per_order = (
            lineitem.select_columns(["l_orderkey", "l_suppkey"])
            .groupby("l_orderkey")
            .aggregate(CountDistinct(on="l_suppkey", alias_name="num_suppliers"))
            .filter(expr=col("num_suppliers") > 1)
        )

        # ── Pre-aggregate: distinct LATE suppliers per order (NOT EXISTS) ─
        # Late lineitem: l_receiptdate > l_commitdate.
        # Materialize to avoid recomputing the filter in both the
        # late_suppliers_per_order branch and the main pipeline
        # (Ray Data has no CSE).
        late_lineitem = (
            lineitem.filter(expr=col("l_receiptdate") > col("l_commitdate"))
            .select_columns(["l_orderkey", "l_suppkey"])
            .materialize()
        )

        late_suppliers_per_order = (
            late_lineitem.groupby("l_orderkey")
            .aggregate(CountDistinct(on="l_suppkey", alias_name="num_late_suppliers"))
            .filter(expr=col("num_late_suppliers") == 1)
        )

        # ── Build main pipeline ─────────────────────────────────────────
        # Saudi suppliers
        saudi_nation = nation.filter(expr=col("n_name") == nation_name)
        saudi_suppliers = supplier.join(
            saudi_nation,
            join_type="inner",
            num_partitions=16,
            on=("s_nationkey",),
            right_on=("n_nationkey",),
        ).select_columns(["s_suppkey", "s_name"])

        # Failed orders
        failed_orders = orders.filter(expr=col("o_orderstatus") == "F").select_columns(
            ["o_orderkey"]
        )

        # Late lineitem joined with failed orders (l1 base rows)
        ds = late_lineitem.join(
            failed_orders,
            join_type="left_semi",
            num_partitions=16,
            on=("l_orderkey",),
            right_on=("o_orderkey",),
        )

        # Join with Saudi suppliers
        ds = ds.join(
            saudi_suppliers,
            join_type="inner",
            num_partitions=16,
            on=("l_suppkey",),
            right_on=("s_suppkey",),
        )

        # EXISTS: another supplier exists for this order (num_suppliers > 1)
        # Filter already pushed down to suppliers_per_order.
        ds = ds.join(
            suppliers_per_order,
            join_type="inner",
            num_partitions=16,
            on=("l_orderkey",),
        )

        # NOT EXISTS: no other late supplier (num_late_suppliers == 1)
        # Filter already pushed down to late_suppliers_per_order.
        ds = ds.join(
            late_suppliers_per_order,
            join_type="inner",
            num_partitions=16,
            on=("l_orderkey",),
        )

        # Group by supplier name, count, sort, and limit.
        _ = (
            ds.groupby("s_name")
            .aggregate(Count(alias_name="numwait"))
            .sort(key=["numwait", "s_name"], descending=[True, False])
            .limit(100)
            .materialize()
        )

        # Report arguments for the benchmark.
        return vars(args)

    run_tpch_benchmark("tpch_q21", benchmark_fn)


if __name__ == "__main__":
    ray.init()
    args = parse_tpch_args()
    main(args)
