import ray
from ray.data.aggregate import Sum
from ray.data.expressions import col
from common import parse_tpch_args, load_table, to_f64, run_tpch_benchmark


def main(args):
    def benchmark_fn():
        # Q9: Product Type Profit Measure Query
        # Profit by nation and order year for parts whose names contain "green".
        #
        # Equivalent SQL:
        #   SELECT nation, o_year, SUM(amount) AS sum_profit
        #   FROM (
        #     SELECT n_name AS nation,
        #            EXTRACT(YEAR FROM o_orderdate) AS o_year,
        #            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity
        #                AS amount
        #     FROM part, supplier, lineitem, partsupp, orders, nation
        #     WHERE s_suppkey = l_suppkey
        #       AND ps_suppkey = l_suppkey
        #       AND ps_partkey = l_partkey
        #       AND p_partkey = l_partkey
        #       AND o_orderkey = l_orderkey
        #       AND s_nationkey = n_nationkey
        #       AND p_name LIKE '%green%'
        #   ) AS profit
        #   GROUP BY nation, o_year
        #   ORDER BY nation, o_year DESC;
        #
        # Note:
        # The pipeline is kept linear:
        # part->lineitem->partsupp->supplier->nation->orders.

        # Load all required tables with early column pruning to reduce
        # intermediate data size (projection pushes down to Parquet reader)
        # TODO: Remove manual projection once we support proper projection derivation
        part = load_table("part", args.sf).select_columns(["p_partkey", "p_name"])
        supplier = load_table("supplier", args.sf).select_columns(
            ["s_suppkey", "s_nationkey"]
        )
        partsupp = load_table("partsupp", args.sf).select_columns(
            ["ps_partkey", "ps_suppkey", "ps_supplycost"]
        )
        orders = load_table("orders", args.sf).select_columns(
            ["o_orderkey", "o_orderdate"]
        )
        lineitem = load_table("lineitem", args.sf).select_columns(
            [
                "l_orderkey",
                "l_partkey",
                "l_suppkey",
                "l_quantity",
                "l_extendedprice",
                "l_discount",
            ]
        )
        nation = load_table("nation", args.sf).select_columns(["n_nationkey", "n_name"])

        # Q9 parameters
        part_name_pattern = "green"

        lineitem_part = lineitem.join(
            part,
            num_partitions=16,
            # Empirical value to balance parallelism and shuffle overhead
            join_type="inner",
            on=("l_partkey",),
            right_on=("p_partkey",),
        )
        # Keep contains() filter after a join to avoid pushing a UDF expression
        # into parquet read predicate conversion.
        lineitem_part = lineitem_part.filter(
            expr=col("p_name").str.contains(part_name_pattern)
        ).select_columns(
            [
                "l_orderkey",
                "l_partkey",
                "l_suppkey",
                "l_quantity",
                "l_extendedprice",
                "l_discount",
            ]
        )

        # Join lineitem with partsupp on part key and supplier key
        lineitem_partsupp = lineitem_part.join(
            partsupp,
            num_partitions=16,
            join_type="inner",
            on=("l_partkey", "l_suppkey"),
            right_on=("ps_partkey", "ps_suppkey"),
        ).select_columns(
            [
                "l_orderkey",
                "l_quantity",
                "l_extendedprice",
                "l_discount",
                "l_suppkey",
                "ps_supplycost",
            ]
        )

        lineitem_supplier = lineitem_partsupp.join(
            supplier,
            num_partitions=16,
            join_type="inner",
            on=("l_suppkey",),
            right_on=("s_suppkey",),
        ).select_columns(
            [
                "l_orderkey",
                "l_quantity",
                "l_extendedprice",
                "l_discount",
                "ps_supplycost",
                "s_nationkey",
            ]
        )

        lineitem_nation = lineitem_supplier.join(
            nation,
            num_partitions=16,
            join_type="inner",
            on=("s_nationkey",),
            right_on=("n_nationkey",),
        ).select_columns(
            [
                "l_orderkey",
                "l_quantity",
                "l_extendedprice",
                "l_discount",
                "ps_supplycost",
                "n_name",
            ]
        )

        ds = lineitem_nation.join(
            orders,
            num_partitions=16,
            join_type="inner",
            on=("l_orderkey",),
            right_on=("o_orderkey",),
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
