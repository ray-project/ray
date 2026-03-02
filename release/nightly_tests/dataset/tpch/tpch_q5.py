import ray
from ray.data.aggregate import Sum
from ray.data.expressions import col
from common import parse_tpch_args, load_table, to_f64, run_tpch_benchmark


def main(args):
    def benchmark_fn():
        from datetime import datetime

        # Q5: Local Supplier Volume Query
        # Revenue by nation for orders in a one-year window and a target region.
        # Keep rows where customer nation == supplier nation, then aggregate revenue.
        #
        # Equivalent SQL:
        #   SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue
        #   FROM customer
        #   JOIN orders   ON c_custkey = o_custkey
        #   JOIN lineitem ON l_orderkey = o_orderkey
        #   JOIN supplier ON l_suppkey = s_suppkey
        #   JOIN nation   ON c_nationkey = n_nationkey
        #   JOIN region   ON n_regionkey = r_regionkey
        #   WHERE r_name = 'ASIA'
        #     AND o_orderdate >= DATE '1994-01-01'
        #     AND o_orderdate <  DATE '1995-01-01'
        #     AND c_nationkey = s_nationkey
        #   GROUP BY n_name
        #   ORDER BY revenue DESC;
        #
        # Note:
        # The physical join order below is a linear chain optimized for Ray Data:
        # Region -> Nation -> Customer -> Orders -> Lineitem -> Supplier.

        # Load all required tables with early column pruning to reduce
        # intermediate data size (projection pushes down to Parquet reader)
        # TODO: Remove manual projection once we support proper projection derivation
        region = load_table("region", args.sf).select_columns(["r_regionkey", "r_name"])
        nation = load_table("nation", args.sf).select_columns(
            ["n_nationkey", "n_name", "n_regionkey"]
        )
        supplier = load_table("supplier", args.sf).select_columns(
            ["s_suppkey", "s_nationkey"]
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

        # Q5 parameters
        date = datetime(1994, 1, 1)

        # Filter region by name
        region_filtered = region.filter(expr=col("r_name") == "ASIA")

        # 1) Region -> Nation: keep nations in target region
        nation_region = region_filtered.join(
            nation,
            num_partitions=16,  # Empirical value to balance parallelism and shuffle overhead
            join_type="inner",
            on=("r_regionkey",),
            right_on=("n_regionkey",),
        )
        nation_region = nation_region.select_columns(["n_nationkey", "n_name"])

        # 2) Nation -> Customer: keep customers in target region nations
        customer_region = customer.join(
            nation_region,
            num_partitions=16,
            join_type="inner",
            on=("c_nationkey",),
            right_on=("n_nationkey",),
        )
        customer_region = customer_region.select_columns(
            ["c_custkey", "c_nationkey", "n_name"]
        )

        # 3) Customer -> Orders: keep only target-year orders from those customers
        orders_filtered = orders.filter(
            expr=(
                (col("o_orderdate") >= date)
                & (col("o_orderdate") < datetime(date.year + 1, date.month, date.day))
            )
        )
        orders_customer = orders_filtered.join(
            customer_region,
            num_partitions=16,
            join_type="inner",
            on=("o_custkey",),
            right_on=("c_custkey",),
        )
        orders_customer = orders_customer.select_columns(
            ["o_orderkey", "c_nationkey", "n_name"]
        )

        # 4) Orders -> Lineitem
        lineitem_orders = lineitem.join(
            orders_customer,
            num_partitions=16,
            join_type="inner",
            on=("l_orderkey",),
            right_on=("o_orderkey",),
        )
        lineitem_orders = lineitem_orders.select_columns(
            ["l_suppkey", "l_extendedprice", "l_discount", "c_nationkey", "n_name"]
        )

        # 5) Lineitem -> Supplier
        ds = lineitem_orders.join(
            supplier,
            num_partitions=16,
            join_type="inner",
            on=("l_suppkey",),
            right_on=("s_suppkey",),
        )
        # Apply local supplier constraint after supplier columns become available
        ds = ds.filter(expr=col("c_nationkey") == col("s_nationkey"))

        # Calculate revenue
        ds = ds.with_column(
            "revenue",
            to_f64(col("l_extendedprice")) * (1 - to_f64(col("l_discount"))),
        )

        # Aggregate by nation name
        _ = (
            ds.groupby("n_name")
            .aggregate(Sum(on="revenue", alias_name="revenue"))
            .sort(key="revenue", descending=True)
            .materialize()
        )

        # Report arguments for the benchmark.
        return vars(args)

    run_tpch_benchmark("tpch_q5", benchmark_fn)


if __name__ == "__main__":
    ray.init()
    args = parse_tpch_args()
    main(args)
