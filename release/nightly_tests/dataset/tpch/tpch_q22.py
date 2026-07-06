import ray
from ray.data.aggregate import Count, Sum
from ray.data.expressions import col
from common import parse_tpch_args, load_table, to_f64, run_tpch_benchmark


def main(args):
    def benchmark_fn():
        # Q22: Global Sales Opportunity Query
        # Identify geographic areas where there are customers who may be
        # likely to make a purchase (above-average balance, no existing orders).
        #
        # Equivalent SQL:
        #   SELECT cntrycode, COUNT(*) AS numcust,
        #          SUM(c_acctbal) AS totacctbal
        #   FROM (
        #       SELECT SUBSTRING(c_phone FROM 1 FOR 2) AS cntrycode,
        #              c_acctbal
        #       FROM customer
        #       WHERE SUBSTRING(c_phone FROM 1 FOR 2)
        #               IN ('13','31','23','29','30','18','17')
        #         AND c_acctbal > (
        #             SELECT AVG(c_acctbal)
        #             FROM customer
        #             WHERE c_acctbal > 0.00
        #               AND SUBSTRING(c_phone FROM 1 FOR 2)
        #                     IN ('13','31','23','29','30','18','17')
        #         )
        #         AND NOT EXISTS (
        #             SELECT * FROM orders WHERE o_custkey = c_custkey
        #         )
        #   ) AS custsale
        #   GROUP BY cntrycode
        #   ORDER BY cntrycode;
        #
        # Note:
        # The scalar AVG subquery is computed first as a plain float via
        # Dataset.mean(). The NOT EXISTS is implemented as a left_anti join.

        # Load tables with early projection.
        customer = load_table("customer", args.sf).select_columns(
            ["c_custkey", "c_phone", "c_acctbal"]
        )
        orders = load_table("orders", args.sf).select_columns(["o_custkey"])

        # Q22 parameters
        codes_regex = "^(13|31|23|29|30|18|17)$"

        # Derive country code and cast acctbal to float64.
        customer = customer.with_column("cntrycode", col("c_phone").str.slice(0, 2))
        customer = customer.with_column("c_acctbal_f", to_f64(col("c_acctbal")))

        # Filter to target country codes.
        customer_filtered = customer.filter(
            expr=col("cntrycode").str.match_regex(codes_regex)
        )

        # Scalar AVG subquery: average balance among positive-balance
        # customers in the target country codes.
        avg_acctbal = customer_filtered.filter(expr=col("c_acctbal_f") > 0.0).mean(
            "c_acctbal_f"
        )

        # Keep customers whose balance exceeds the average.
        custsale = customer_filtered.filter(expr=col("c_acctbal_f") > avg_acctbal)

        # NOT EXISTS: exclude customers who have placed orders.
        custsale = custsale.join(
            orders,
            join_type="left_anti",
            num_partitions=16,
            on=("c_custkey",),
            right_on=("o_custkey",),
        )

        # Group by country code, aggregate count and total balance.
        _ = (
            custsale.groupby("cntrycode")
            .aggregate(
                Count(alias_name="numcust"),
                Sum(on="c_acctbal_f", alias_name="totacctbal"),
            )
            .sort(key="cntrycode")
            .materialize()
        )

        # Report arguments for the benchmark.
        return vars(args)

    run_tpch_benchmark("tpch_q22", benchmark_fn)


if __name__ == "__main__":
    ray.init()
    args = parse_tpch_args()
    main(args)
