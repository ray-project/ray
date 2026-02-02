import ray
from ray.data.aggregate import Sum
from ray.data.expressions import col
from common import parse_tpch_args, load_table, to_f64, run_tpch_benchmark


def main(args):
    def benchmark_fn():
        from datetime import datetime

        # Load all required tables
        region = load_table("region", args.sf)
        nation = load_table("nation", args.sf)
        supplier = load_table("supplier", args.sf)
        customer = load_table("customer", args.sf)
        orders = load_table("orders", args.sf)
        lineitem = load_table("lineitem", args.sf)
        part = load_table("part", args.sf)

        # Q8 parameters
        date1 = datetime(1995, 1, 1)
        date2 = datetime(1997, 1, 1)
        region_name = "AMERICA"
        part_type = "ECONOMY ANODIZED STEEL"

        # Filter region
        region_filtered = region.filter(expr=col("r_name") == region_name)

        # Join region with nation
        nation_region = region_filtered.join(
            nation,
            join_type="inner",
            num_partitions=100,
            on=("r_regionkey",),
            right_on=("n_regionkey",),
        )

        # Join customer with nation
        customer_nation = customer.join(
            nation_region,
            join_type="inner",
            num_partitions=100,
            on=("c_nationkey",),
            right_on=("n_nationkey",),
        )

        # Join supplier with nation (for supplier nation)
        supplier_nation = supplier.join(
            nation,
            join_type="inner",
            num_partitions=100,
            on=("s_nationkey",),
            right_on=("n_nationkey",),
            left_suffix="",
            right_suffix="_supp",
        )

        # Filter part by type
        part_filtered = part.filter(expr=col("p_type") == part_type)

        # Join orders with customer and filter by date
        orders_filtered = orders.filter(
            expr=(
                (col("o_orderdate") >= date1)
                & (col("o_orderdate") < date2)
            )
        )
        orders_customer = orders_filtered.join(
            customer_nation,
            join_type="inner",
            num_partitions=100,
            on=("o_custkey",),
            right_on=("c_custkey",),
        )

        # Join lineitem with orders
        lineitem_orders = lineitem.join(
            orders_customer,
            join_type="inner",
            num_partitions=100,
            on=("l_orderkey",),
            right_on=("o_orderkey",),
        )

        # Join with part
        lineitem_part = lineitem_orders.join(
            part_filtered,
            join_type="inner",
            num_partitions=100,
            on=("l_partkey",),
            right_on=("p_partkey",),
        )

        # Join with supplier
        ds = lineitem_part.join(
            supplier_nation,
            join_type="inner",
            num_partitions=100,
            on=("l_suppkey",),
            right_on=("s_suppkey",),
        )

        # Calculate volume
        ds = ds.with_column(
            "volume",
            to_f64(col("l_extendedprice")) * (1 - to_f64(col("l_discount"))),
        )

        # Extract year from orderdate
        ds = ds.with_column(
            "o_year",
            col("o_orderdate").dt.year(),
        )

        # Aggregate by year and supplier nation
        result = (
            ds.groupby(["o_year", "n_name_supp"])
            .aggregate(Sum(on="volume", alias_name="volume"))
            .sort(key=["o_year", "n_name_supp"])
        )

        return result

    run_tpch_benchmark("tpch_q8", benchmark_fn)


if __name__ == "__main__":
    ray.init()
    args = parse_tpch_args()
    main(args)
