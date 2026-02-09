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
        nation_name = "BRAZIL"  # Specific nation for market share calculation

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
        # Note: For Q8, suppliers can be from ALL nations, not just the region
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
            expr=((col("o_orderdate") >= date1) & (col("o_orderdate") < date2))
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

        # For Q8, we need to calculate market share for a specific nation (e.g., BRAZIL)
        # Market share = (volume from specific nation suppliers) / (total volume in region)

        # Create a conditional column for volume from the specific nation
        # Use conditional expression: if n_name_supp == nation_name then volume else 0
        # Convert boolean to numeric: True -> 1, False -> 0, then multiply by volume
        ds = ds.with_column(
            "is_nation",
            to_f64((col("n_name_supp") == nation_name)),
        )
        ds = ds.with_column(
            "nation_volume",
            col("is_nation") * col("volume"),
        )

        # Aggregate total volume and nation volume per year in a single groupby
        result = ds.groupby("o_year").aggregate(
            Sum(on="volume", alias_name="total_volume"),
            Sum(on="nation_volume", alias_name="nation_volume"),
        )

        # Calculate market share for the specific nation
        result = result.with_column(
            "mkt_share",
            col("nation_volume") / col("total_volume"),
        )

        # Select and sort by year
        _ = (
            result.select_columns(["o_year", "mkt_share"])
            .sort(key="o_year")
            .materialize()
        )

        # Report arguments for the benchmark.
        return vars(args)

    run_tpch_benchmark("tpch_q8", benchmark_fn)


if __name__ == "__main__":
    ray.init()
    args = parse_tpch_args()
    main(args)
