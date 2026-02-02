import ray
from ray.data.aggregate import Sum
from ray.data.expressions import col
from common import parse_tpch_args, load_table, to_f64, run_tpch_benchmark


def main(args):
    def benchmark_fn():
        # Load all required tables
        part = load_table("part", args.sf)
        supplier = load_table("supplier", args.sf)
        partsupp = load_table("partsupp", args.sf)
        orders = load_table("orders", args.sf)
        lineitem = load_table("lineitem", args.sf)
        nation = load_table("nation", args.sf)

        # Q9 parameters
        part_name_pattern = "GREEN"

        # Filter part by name pattern
        part_filtered = part.filter(expr=col("p_name").str.contains(part_name_pattern))

        # Join partsupp with supplier
        partsupp_supplier = partsupp.join(
            supplier,
            join_type="inner",
            num_partitions=100,
            on=("ps_suppkey",),
            right_on=("s_suppkey",),
        )

        # Join with part
        partsupp_part = partsupp_supplier.join(
            part_filtered,
            join_type="inner",
            num_partitions=100,
            on=("ps_partkey",),
            right_on=("p_partkey",),
        )

        # Join supplier with nation
        partsupp_nation = partsupp_part.join(
            nation,
            join_type="inner",
            num_partitions=100,
            on=("s_nationkey",),
            right_on=("n_nationkey",),
        )

        # Join orders with lineitem
        # Note: TPC-H Q9 processes all orders and groups by year, no date filter
        lineitem_orders = lineitem.join(
            orders,
            join_type="inner",
            num_partitions=100,
            on=("l_orderkey",),
            right_on=("o_orderkey",),
        )

        # Join lineitem with partsupp on part key and supplier key
        # Using multi-key join is more efficient than join + filter
        ds = lineitem_orders.join(
            partsupp_nation,
            join_type="inner",
            num_partitions=100,
            on=("l_partkey", "l_suppkey"),
            right_on=("ps_partkey", "ps_suppkey"),
        )

        # Calculate profit
        ds = ds.with_column(
            "profit",
            to_f64(col("l_extendedprice")) * (1 - to_f64(col("l_discount")))
            - to_f64(col("ps_supplycost")) * to_f64(col("l_quantity")),
        )

        # Extract year from orderdate
        ds = ds.with_column(
            "o_year",
            col("o_orderdate").dt.year(),
        )

        # Aggregate by nation and year
        _ = (
            ds.groupby(["n_name", "o_year"])
            .aggregate(Sum(on="profit", alias_name="profit"))
            .sort(key=["n_name", "o_year"], descending=[False, True])
            .materialize()
        )

        # Report arguments for the benchmark.
        return vars(args)

    run_tpch_benchmark("tpch_q9", benchmark_fn)


if __name__ == "__main__":
    ray.init()
    args = parse_tpch_args()
    main(args)
