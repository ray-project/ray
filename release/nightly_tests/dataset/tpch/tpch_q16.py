import ray
from ray.data.aggregate import Count
from ray.data.expressions import col
from common import load_table, parse_tpch_args, run_tpch_benchmark


def main(args):
    def benchmark_fn():
        # Q16: Parts/Supplier Relationship Query
        # Count distinct suppliers able to supply parts of given sizes,
        # excluding a brand, a type prefix, and suppliers with complaints.
        #
        # Equivalent SQL:
        #   SELECT p_brand, p_type, p_size,
        #          COUNT(DISTINCT ps_suppkey) AS supplier_cnt
        #   FROM partsupp, part
        #   WHERE p_partkey = ps_partkey
        #     AND p_brand <> 'Brand#45'
        #     AND p_type NOT LIKE 'MEDIUM POLISHED%'
        #     AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
        #     AND ps_suppkey NOT IN (
        #         SELECT s_suppkey FROM supplier
        #         WHERE s_comment LIKE '%Customer%Complaints%')
        #   GROUP BY p_brand, p_type, p_size
        #   ORDER BY supplier_cnt DESC, p_brand, p_type, p_size;
        #
        # Note:
        # The NOT IN subquery is a left_anti join (as in Q22). COUNT(DISTINCT)
        # is expressed as two groupbys: dedupe on (brand, type, size, suppkey),
        # then count rows per (brand, type, size).

        part = load_table("part", args.sf).select_columns(
            ["p_partkey", "p_brand", "p_type", "p_size"]
        )
        partsupp = load_table("partsupp", args.sf).select_columns(
            ["ps_partkey", "ps_suppkey"]
        )
        supplier = load_table("supplier", args.sf).select_columns(
            ["s_suppkey", "s_comment"]
        )

        # Q16 parameters
        excluded_brand = "Brand#45"
        excluded_type_prefix = "MEDIUM POLISHED"
        sizes = [49, 14, 23, 45, 19, 3, 36, 9]

        part_filtered = part.filter(
            expr=(col("p_brand") != excluded_brand) & col("p_size").is_in(sizes)
        )
        # NOT LIKE 'MEDIUM POLISHED%'. Kept after load_table to avoid pushing
        # a UDF expression into parquet predicate conversion (see Q2/Q9).
        part_filtered = part_filtered.filter(
            expr=~col("p_type").str.starts_with(excluded_type_prefix)
        )

        # Suppliers with complaints: LIKE '%Customer%Complaints%'.
        complainers = supplier.filter(
            expr=col("s_comment").str.match_regex("Customer.*Complaints")
        ).select_columns(["s_suppkey"])

        # Inner join with the selective part filter first so the anti join
        # below shuffles the reduced dataset instead of all of partsupp.
        ps_filtered = partsupp.join(
            part_filtered,
            join_type="inner",
            num_partitions=200,
            on=("ps_partkey",),
            right_on=("p_partkey",),
        )

        # NOT IN -> anti join.
        joined = ps_filtered.join(
            complainers,
            join_type="left_anti",
            num_partitions=200,
            on=("ps_suppkey",),
            right_on=("s_suppkey",),
        ).select_columns(["p_brand", "p_type", "p_size", "ps_suppkey"])

        # COUNT(DISTINCT ps_suppkey): dedupe first, then count.
        distinct_suppliers = (
            joined.groupby(["p_brand", "p_type", "p_size", "ps_suppkey"])
            .aggregate(Count(alias_name="_dedupe"))
            .select_columns(["p_brand", "p_type", "p_size", "ps_suppkey"])
        )

        _ = (
            distinct_suppliers.groupby(["p_brand", "p_type", "p_size"])
            .aggregate(Count(alias_name="supplier_cnt"))
            .sort(
                key=["supplier_cnt", "p_brand", "p_type", "p_size"],
                descending=[True, False, False, False],
            )
            .materialize()
        )

        # Report arguments for the benchmark.
        return vars(args)

    run_tpch_benchmark("tpch_q16", benchmark_fn)


if __name__ == "__main__":
    ray.init()
    args = parse_tpch_args()
    main(args)
