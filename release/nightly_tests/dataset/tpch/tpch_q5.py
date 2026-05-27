import ray
from ray.data.aggregate import Sum
from ray.data.expressions import col
from common import load_table, parse_tpch_args, run_tpch_benchmark, to_f64


def main(args):
    def benchmark_fn():
        from datetime import datetime

        # Q5: Local Supplier Volume Query
        # Revenue by nation for customers in a target region and order-date window,
        # restricted to suppliers from the same nation as the customer.
        #
        # Equivalent SQL:
        #   SELECT n_name,
        #          SUM(l_extendedprice * (1 - l_discount)) AS revenue
        #   FROM customer, orders, lineitem, supplier, nation, region
        #   WHERE c_custkey = o_custkey
        #     AND l_orderkey = o_orderkey
        #     AND l_suppkey = s_suppkey
        #     AND c_nationkey = s_nationkey
        #     AND s_nationkey = n_nationkey
        #     AND n_regionkey = r_regionkey
        #     AND r_name = 'ASIA'
        #     AND o_orderdate >= DATE '1994-01-01'
        #     AND o_orderdate <  DATE '1995-01-01'
        #   GROUP BY n_name
        #   ORDER BY revenue DESC;
        #
        # Note:
        # The pipeline stays linear:
        # (region->nation->customer)->orders->lineitem->supplier.

        region = load_table("region", args.sf).select_columns(["r_regionkey", "r_name"])
        nation = load_table("nation", args.sf).select_columns(
            ["n_nationkey", "n_name", "n_regionkey"]
        )
        customer = load_table("customer", args.sf).select_columns(
            ["c_custkey", "c_nationkey"]
        )
        orders = load_table("orders", args.sf).select_columns(
            ["o_orderkey", "o_custkey", "o_orderdate"]
        )
        lineitem = load_table("lineitem", args.sf).select_columns(
            ["l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"]
        )
        supplier = load_table("supplier", args.sf).select_columns(
            ["s_suppkey", "s_nationkey"]
        )

        region_name = "ASIA"
        date_start = datetime(1994, 1, 1)
        date_end = datetime(1995, 1, 1)

        region_filtered = region.filter(
            expr=col("r_name") == region_name
        ).select_columns(["r_regionkey"])

        nation_region = region_filtered.join(
            nation,
            num_partitions=16,
            join_type="inner",
            on=("r_regionkey",),
            right_on=("n_regionkey",),
        ).select_columns(["n_nationkey", "n_name"])

        # TODO: manual rename n_nationkey to c_nationkey as workaround, the join planner or operator should be able to infer the correct join column to keep. See https://github.com/ray-project/ray/issues/62846
        customer_nation = (
            nation_region.join(
                customer,
                num_partitions=16,
                join_type="inner",
                on=("n_nationkey",),
                right_on=("c_nationkey",),
            )
            .select_columns(["c_custkey", "n_nationkey", "n_name"])
            .rename_columns({"n_nationkey": "c_nationkey"})
        )

        orders_filtered = orders.filter(
            expr=((col("o_orderdate") >= date_start) & (col("o_orderdate") < date_end))
        )
        orders_customer = orders_filtered.join(
            customer_nation,
            num_partitions=16,
            join_type="inner",
            on=("o_custkey",),
            right_on=("c_custkey",),
        ).select_columns(["o_orderkey", "c_nationkey", "n_name"])

        lineitem_orders = lineitem.join(
            orders_customer,
            num_partitions=16,
            join_type="inner",
            on=("l_orderkey",),
            right_on=("o_orderkey",),
        ).select_columns(
            ["l_suppkey", "l_extendedprice", "l_discount", "c_nationkey", "n_name"]
        )

        ds = lineitem_orders.join(
            supplier,
            num_partitions=16,
            join_type="inner",
            on=("l_suppkey",),
            right_on=("s_suppkey",),
        )
        ds = ds.filter(expr=col("c_nationkey") == col("s_nationkey")).select_columns(
            ["n_name", "l_extendedprice", "l_discount"]
        )

        ds = ds.with_column(
            "revenue",
            to_f64(col("l_extendedprice")) * (1 - to_f64(col("l_discount"))),
        )

        _ = (
            ds.groupby("n_name")
            .aggregate(Sum(on="revenue", alias_name="revenue"))
            .sort(key="revenue", descending=True)
            .materialize()
        )

        return vars(args)

    run_tpch_benchmark("tpch_q5", benchmark_fn)


if __name__ == "__main__":
    ray.init()
    args = parse_tpch_args()
    main(args)
