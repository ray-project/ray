import ray
from ray.data.aggregate import Sum
from ray.data.expressions import col
from common import parse_tpch_args, load_table, to_f64, run_tpch_benchmark


def main(args):
    def benchmark_fn():
        from datetime import datetime

        # Load all required tables
        supplier = load_table("supplier", args.sf)
        lineitem = load_table("lineitem", args.sf)
        orders = load_table("orders", args.sf)
        customer = load_table("customer", args.sf)
        nation = load_table("nation", args.sf)

        # Q7 parameters
        date1 = datetime(1995, 1, 1)
        date2 = datetime(1997, 1, 1)
        nation1 = "FRANCE"
        nation2 = "GERMANY"

        nations_of_interest = nation.filter(
            expr=(col("n_name") == nation1) | (col("n_name") == nation2)
        )

        supplier_nation = supplier.join(
            nations_of_interest,
            num_partitions=32,  # Empirical value to balance parallelism and shuffle overhead
            join_type="inner",
            on=("s_nationkey",),
            right_on=("n_nationkey",),
        ).rename_columns({"n_name": "n_name_supp"})

        customer_nation = customer.join(
            nations_of_interest,
            num_partitions=32,
            join_type="inner",
            on=("c_nationkey",),
            right_on=("n_nationkey",),
        ).rename_columns({"n_name": "n_name_cust"})

        orders_customer = orders.join(
            customer_nation,
            num_partitions=32,
            join_type="inner",
            on=("o_custkey",),
            right_on=("c_custkey",),
            left_suffix="",
        )

        # Join lineitem with orders and filter by date
        lineitem_filtered = lineitem.filter(
            expr=((col("l_shipdate") >= date1) & (col("l_shipdate") < date2))
        )
        lineitem_orders = lineitem_filtered.join(
            orders_customer,
            num_partitions=32,
            join_type="inner",
            on=("l_orderkey",),
            right_on=("o_orderkey",),
        )

        # Join with supplier (use suffix to avoid conflicts with customer nation columns)
        ds = lineitem_orders.join(
            supplier_nation,
            num_partitions=32,
            join_type="inner",
            on=("l_suppkey",),
            right_on=("s_suppkey",),
            left_suffix="",
            right_suffix="_supp",
        )

        # Filter to ensure we only include shipments between the two nations
        # (exclude shipments within the same nation)
        ds = ds.filter(expr=(col("n_name_supp") != col("n_name_cust")))

        # Calculate revenue
        ds = ds.with_column(
            "revenue",
            to_f64(col("l_extendedprice")) * (1 - to_f64(col("l_discount"))),
        )

        # Extract year from shipdate
        ds = ds.with_column(
            "l_year",
            col("l_shipdate").dt.year(),
        )

        # Aggregate by supplier nation, customer nation, and year
        _ = (
            ds.groupby(["n_name_supp", "n_name_cust", "l_year"])
            .aggregate(Sum(on="revenue", alias_name="revenue"))
            .sort(key=["n_name_supp", "n_name_cust", "l_year"])
            .materialize()
        )

        # Report arguments for the benchmark.
        return vars(args)

    run_tpch_benchmark("tpch_q7", benchmark_fn)


if __name__ == "__main__":
    ray.init()
    args = parse_tpch_args()
    main(args)
