import ray
from ray.data.aggregate import Min
from ray.data.expressions import col
from common import parse_tpch_args, load_table, to_f64, run_tpch_benchmark


def main(args):
    def benchmark_fn():
        # Q2: Minimum Cost Supplier Query
        # Find the cheapest supplier in a given region for parts of a given size and type.
        #
        # Equivalent SQL:
        #   SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr,
        #          s_address, s_phone, s_comment
        #   FROM part, supplier, partsupp, nation, region
        #   WHERE p_partkey = ps_partkey
        #     AND s_suppkey = ps_suppkey
        #     AND p_size = 15
        #     AND p_type LIKE '%BRASS'
        #     AND s_nationkey = n_nationkey
        #     AND n_regionkey = r_regionkey
        #     AND r_name = 'EUROPE'
        #     AND ps_supplycost = (
        #       SELECT MIN(ps_supplycost)
        #       FROM partsupp, supplier, nation, region
        #       WHERE p_partkey = ps_partkey
        #         AND s_suppkey = ps_suppkey
        #         AND s_nationkey = n_nationkey
        #         AND n_regionkey = r_regionkey
        #         AND r_name = 'EUROPE'
        #     )
        #   ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
        #   LIMIT 100;
        #
        # Note:
        # The correlated subquery is decorrelated into two branches sharing
        # a "regional suppliers" chain:
        #   Branch A: region -> nation -> supplier (regional_suppliers)
        #   Branch B: partsupp join regional_suppliers -> groupby Min(supplycost)
        # The main pipeline joins part -> partsupp -> regional_suppliers -> min_cost.

        # Load tables. Projection pushdown derives the columns each read needs.
        region = load_table("region", args.sf)
        nation = load_table("nation", args.sf)
        supplier = load_table("supplier", args.sf)
        part = load_table("part", args.sf)
        partsupp = load_table("partsupp", args.sf)

        # Q2 parameters
        region_name = "EUROPE"
        part_size = 15
        part_type_suffix = "BRASS"

        # ── Branch A: build regional suppliers ──────────────────────────
        region_filtered = region.filter(expr=col("r_name") == region_name)

        nation_region = region_filtered.join(
            nation,
            num_partitions=200,
            join_type="inner",
            on=("r_regionkey",),
            right_on=("n_regionkey",),
        )

        # Materialize to avoid recomputing the region->nation->supplier chain
        # in both Branch B and the main pipeline (Ray Data has no CSE).
        # Projection pushdown stops at materialize(), so prune by hand first.
        regional_suppliers = (
            nation_region.join(
                supplier,
                num_partitions=200,
                join_type="inner",
                on=("n_nationkey",),
                right_on=("s_nationkey",),
            )
            .select_columns(
                [
                    "s_suppkey",
                    "s_name",
                    "s_address",
                    "s_phone",
                    "s_acctbal",
                    "s_comment",
                    "n_name",
                ]
            )
            .materialize()
        )

        # ── Branch B: min supply cost per part from regional suppliers ──
        regional_partsupp = partsupp.join(
            regional_suppliers,
            num_partitions=200,
            join_type="inner",
            on=("ps_suppkey",),
            right_on=("s_suppkey",),
        )
        regional_partsupp = regional_partsupp.with_column(
            "ps_supplycost_f", to_f64(col("ps_supplycost"))
        )

        min_cost = regional_partsupp.groupby("ps_partkey").aggregate(
            Min(on="ps_supplycost_f", alias_name="min_supplycost")
        )

        # ── Main pipeline ───────────────────────────────────────────────
        # Filter part by size (pushes down to Parquet) and type suffix.
        # Keep ends_with() filter after load_table to avoid pushing a UDF
        # expression into parquet read predicate conversion.
        part_filtered = part.filter(expr=col("p_size") == part_size)
        part_filtered = part_filtered.filter(
            expr=col("p_type").str.ends_with(part_type_suffix)
        )

        # Join part with partsupp
        part_partsupp = part_filtered.join(
            partsupp,
            num_partitions=200,
            join_type="inner",
            on=("p_partkey",),
            right_on=("ps_partkey",),
        )

        part_partsupp = part_partsupp.with_column(
            "ps_supplycost_f", to_f64(col("ps_supplycost"))
        )

        # Join with regional suppliers to enforce region constraint and get details
        part_regional = part_partsupp.join(
            regional_suppliers,
            num_partitions=200,
            join_type="inner",
            on=("ps_suppkey",),
            right_on=("s_suppkey",),
        )

        # Join with min cost, keeping only minimum-cost suppliers. The
        # ``condition`` equality becomes a second hash key, so the hash
        # partitioning enforces it instead of a filter after the join.
        # Float equality is safe: both sides are cast from the same Decimal
        # source via to_f64, and Min preserves the exact float64 value.
        ds = part_regional.join(
            min_cost,
            num_partitions=200,
            join_type="inner",
            on=("p_partkey",),
            right_on=("ps_partkey",),
            condition=col("ps_supplycost_f") == col("min_supplycost"),
        )

        # Cast Decimal s_acctbal to float64 for sort compatibility
        ds = ds.with_column("s_acctbal", to_f64(col("s_acctbal")))

        # Select output columns, sort, and limit
        _ = (
            ds.select_columns(
                [
                    "s_acctbal",
                    "s_name",
                    "n_name",
                    "p_partkey",
                    "p_mfgr",
                    "s_address",
                    "s_phone",
                    "s_comment",
                ]
            )
            .sort(
                key=["s_acctbal", "n_name", "s_name", "p_partkey"],
                descending=[True, False, False, False],
            )
            .limit(100)
            .materialize()
        )

        # Report arguments for the benchmark.
        return vars(args)

    run_tpch_benchmark("tpch_q2", benchmark_fn)


if __name__ == "__main__":
    ray.init()
    args = parse_tpch_args()
    main(args)
