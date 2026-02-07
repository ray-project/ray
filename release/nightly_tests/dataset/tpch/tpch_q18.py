import ray
from ray.data.aggregate import Sum
from ray.data.expressions import col
from common import parse_tpch_args, load_table, run_tpch_benchmark


def main(args):
    def benchmark_fn():

        # Load all required tables
        customer = load_table("customer", args.sf)
        orders = load_table("orders", args.sf)
        lineitem = load_table("lineitem", args.sf)

        # Q18 parameters (spec: [312..315])
        quantity = 312

        # Calculate total quantity per order
        lineitem_quantity = lineitem.groupby("l_orderkey").aggregate(
            Sum(on="l_quantity", alias_name="total_quantity")
        )

        # Filter orders with total quantity > threshold
        large_orders = lineitem_quantity.filter(expr=col("total_quantity") > quantity)

        # Join lineitem with large orders
        lineitem_large = lineitem.join(
            large_orders,
            join_type="inner",
            num_partitions=100,
            on=("l_orderkey",),
        )

        # Join with orders
        lineitem_orders = lineitem_large.join(
            orders,
            join_type="inner",
            num_partitions=100,
            on=("l_orderkey",),
            right_on=("o_orderkey",),
        )

        # Join with customer
        ds = lineitem_orders.join(
            customer,
            join_type="inner",
            num_partitions=100,
            on=("o_custkey",),
            right_on=("c_custkey",),
        )

        # Aggregate by customer name, customer key, order key, and order date
        _ = (
            ds.groupby(
                ["c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice"]
            )
            .aggregate(Sum(on="l_quantity", alias_name="sum_quantity"))
            .sort(key=["o_totalprice", "o_orderdate"], descending=[True, False])
            .materialize()
        )

        # Report arguments for the benchmark.
        return vars(args)

    run_tpch_benchmark("tpch_q18", benchmark_fn)


if __name__ == "__main__":
    ray.init()
    args = parse_tpch_args()
    main(args)
