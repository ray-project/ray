import ray
from ray.data.aggregate import Sum
from ray.data.expressions import col
from common import parse_tpch_args, load_table, to_f64, run_tpch_benchmark


def main(args):
    def benchmark_fn():
        from datetime import datetime

        # Q8: National Market Share Query
        # For each year, compute a nation's market share within a target region and part type.
        #
        # Equivalent SQL:
        #   SELECT o_year,
        #          SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / SUM(volume)
        #              AS mkt_share
        #   FROM (
        #     SELECT EXTRACT(YEAR FROM o_orderdate) AS o_year,
        #            l_extendedprice * (1 - l_discount) AS volume,
        #            n2.n_name AS nation
        #     FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
        #     WHERE p_partkey = l_partkey
        #       AND s_suppkey = l_suppkey
        #       AND l_orderkey = o_orderkey
        #       AND o_custkey = c_custkey
        #       AND c_nationkey = n1.n_nationkey
        #       AND n1.n_regionkey = r_regionkey
        #       AND r_name = 'AMERICA'
        #       AND s_nationkey = n2.n_nationkey
        #       AND o_orderdate >= DATE '1995-01-01'
        #       AND o_orderdate <  DATE '1997-01-01'
        #       AND p_type = 'ECONOMY ANODIZED STEEL'
        #   ) AS all_nations
        #   GROUP BY o_year
        #   ORDER BY o_year;
        #
        # Note:
        # The pipeline is kept mostly linear:
        # (region->nation->customer)->orders->lineitem->part->supplier->nation.

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
            ["l_orderkey", "l_partkey", "l_suppkey", "l_extendedprice", "l_discount"]
        )
        part = load_table("part", args.sf).select_columns(["p_partkey", "p_type"])

        # Q8 parameters
        date1 = datetime(1995, 1, 1)
        date2 = datetime(1997, 1, 1)
        region_name = "AMERICA"
        part_type = "ECONOMY ANODIZED STEEL"
        nation_name = "BRAZIL"

        # Filter region
        region_filtered = region.filter(expr=col("r_name") == region_name)

        # Join region with nation
        nation_region = region_filtered.join(
            nation,
            num_partitions=16,  # Empirical value to balance parallelism and shuffle overhead
            join_type="inner",
            on=("r_regionkey",),
            right_on=("n_regionkey",),
        )

        # Join customer with nation in the region.
        customer_nation = nation_region.join(
            customer,
            num_partitions=16,
            join_type="inner",
            on=("n_nationkey",),
            right_on=("c_nationkey",),
        )
        customer_nation = customer_nation.select_columns(["c_custkey"])

        # Filter part by type
        part_filtered = part.filter(expr=col("p_type") == part_type)

        # Join orders with customer and filter by date
        orders_filtered = orders.filter(
            expr=((col("o_orderdate") >= date1) & (col("o_orderdate") < date2))
        )
        orders_customer = orders_filtered.join(
            customer_nation,
            num_partitions=16,
            join_type="inner",
            on=("o_custkey",),
            right_on=("c_custkey",),
        ).select_columns(["o_orderkey", "o_orderdate"])

        # Join lineitem with orders
        lineitem_orders = lineitem.join(
            orders_customer,
            num_partitions=16,
            join_type="inner",
            on=("l_orderkey",),
            right_on=("o_orderkey",),
        ).select_columns(
            [
                "l_orderkey",
                "l_partkey",
                "l_suppkey",
                "l_extendedprice",
                "l_discount",
                "o_orderdate",
            ]
        )

        # Join with part
        lineitem_part = lineitem_orders.join(
            part_filtered,
            num_partitions=16,
            join_type="inner",
            on=("l_partkey",),
            right_on=("p_partkey",),
        ).select_columns(
            ["l_suppkey", "l_extendedprice", "l_discount", "o_orderdate"]
        )

        # Keep supplier->nation on the main path.
        lineitem_supplier = lineitem_part.join(
            supplier,
            num_partitions=16,
            join_type="inner",
            on=("l_suppkey",),
            right_on=("s_suppkey",),
        ).select_columns(["l_extendedprice", "l_discount", "o_orderdate", "s_nationkey"])

        ds = lineitem_supplier.join(
            nation,
            num_partitions=16,
            join_type="inner",
            on=("s_nationkey",),
            right_on=("n_nationkey",),
        ).rename_columns({"n_name": "n_name_supp"})

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
