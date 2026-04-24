import ray
from ray.data.aggregate import Sum
from ray.data.expressions import col
from common import parse_tpch_args, load_table, to_f64, run_tpch_benchmark


def main(args):
    def benchmark_fn():
        # Q11: Important Stock Identification Query
        # Parts whose national stock value exceeds a fraction of the total
        # national stock value.
        #
        # Equivalent SQL:
        #   SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) AS value
        #   FROM partsupp, supplier, nation
        #   WHERE ps_suppkey = s_suppkey
        #     AND s_nationkey = n_nationkey
        #     AND n_name = 'GERMANY'
        #   GROUP BY ps_partkey
        #   HAVING SUM(ps_supplycost * ps_availqty) > (
        #       SELECT SUM(ps_supplycost * ps_availqty) * 0.0001
        #       FROM partsupp, supplier, nation
        #       WHERE ps_suppkey = s_suppkey
        #         AND s_nationkey = n_nationkey
        #         AND n_name = 'GERMANY'
        #   )
        #   ORDER BY value DESC;
        #
        # Note:
        # Outer query and subquery share the nation -> supplier -> partsupp
        # chain. Materialize the intermediate (partsupp_germany) once and
        # derive both the per-part aggregate and the scalar threshold from it.

        # Q11 parameters
        nation_name = "GERMANY"
        fraction = 0.0001

        nation = load_table("nation", args.sf).select_columns(["n_nationkey", "n_name"])
        supplier = load_table("supplier", args.sf).select_columns(
            ["s_suppkey", "s_nationkey"]
        )
        partsupp = load_table("partsupp", args.sf).select_columns(
            ["ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost"]
        )

        # Filter nation to the target country, then join to suppliers.
        nation_filtered = nation.filter(expr=col("n_name") == nation_name)

        nation_supplier = nation_filtered.join(
            supplier,
            join_type="inner",
            num_partitions=16,
            on=("n_nationkey",),
            right_on=("s_nationkey",),
        ).select_columns(["s_suppkey"])

        # partsupp restricted to national suppliers, with stock value.
        # Materialize so the scalar total and the per-part aggregate both
        # read from the same intermediate (Ray Data has no CSE).
        partsupp_germany = (
            partsupp.join(
                nation_supplier,
                join_type="inner",
                num_partitions=16,
                on=("ps_suppkey",),
                right_on=("s_suppkey",),
            )
            .with_column(
                "value", to_f64(col("ps_supplycost")) * to_f64(col("ps_availqty"))
            )
            .select_columns(["ps_partkey", "value"])
            .materialize()
        )

        # Scalar threshold = SUM(value) * fraction. aggregate() returns a dict.
        total = partsupp_germany.aggregate(Sum(on="value", alias_name="total"))["total"]
        threshold = total * fraction

        _ = (
            partsupp_germany.groupby("ps_partkey")
            .aggregate(Sum(on="value", alias_name="value"))
            .filter(expr=col("value") > threshold)
            .sort(key="value", descending=True)
            .materialize()
        )

        return vars(args)

    run_tpch_benchmark("tpch_q11", benchmark_fn)


if __name__ == "__main__":
    ray.init()
    args = parse_tpch_args()
    main(args)
