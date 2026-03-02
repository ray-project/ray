import pyarrow.compute as pc
import ray
from ray.data.aggregate import Count
from common import parse_tpch_args, load_table, run_tpch_benchmark


def main(args):
    def benchmark_fn():
        # Q13: Customer Distribution Query
        # Find the distribution of customers by number of orders,
        # excluding orders with comments matching '%[WORD1]%[WORD2]%'
        #
        # Equivalent SQL:
        #   SELECT c_count, count(*) AS custdist
        #   FROM (
        #     SELECT c_custkey, count(o_orderkey) AS c_count
        #     FROM customer LEFT OUTER JOIN orders
        #       ON c_custkey = o_custkey
        #      AND o_comment NOT LIKE '%[WORD1]%[WORD2]%'
        #     GROUP BY c_custkey
        #   ) AS c_orders
        #   GROUP BY c_count
        #   ORDER BY custdist DESC, c_count DESC

        # Q13 substitution parameters
        word1 = "special"
        word2 = "requests"

        customers = load_table("customer", args.sf).select_columns(["c_custkey"])
        orders = load_table("orders", args.sf).select_columns(
            ["o_orderkey", "o_custkey", "o_comment"]
        )

        # Filter out orders whose comment matches '%<word1>%<word2>%' before the join
        # o_comment NOT LIKE '%special%requests%'
        orders = orders.map_batches(
            lambda batch: batch.filter(
                pc.invert(
                    pc.match_substring_regex(batch["o_comment"], f"{word1}.*{word2}")
                )
            ),
            batch_format="pyarrow",
        )

        # Left outer join: customer LEFT OUTER JOIN orders ON c_custkey = o_custkey
        # Customers with no matching orders will have null o_orderkey
        joined = customers.join(
            orders,
            join_type="left_outer",
            num_partitions=128,
            on=("c_custkey",),
            right_on=("o_custkey",),
        )

        # Count non-null o_orderkey per c_custkey (null = customer has no orders)
        # SELECT c_custkey, count(o_orderkey) AS c_count
        # ...
        # GROUP BY c_custkey
        c_orders = joined.groupby(["c_custkey"]).aggregate(
            Count(on="o_orderkey", ignore_nulls=True, alias_name="c_count")
        )

        # Count customers per c_count, then sort custdist DESC, c_count DESC
        # SELECT c_count, count(*) AS custdist
        # ...
        # GROUP BY c_count
        # ORDER BY custdist DESC, c_count DESC
        _ = (
            c_orders.groupby(["c_count"])
            .aggregate(Count(alias_name="custdist"))
            .sort(key=["custdist", "c_count"], descending=[True, True])
            .materialize()
        )

        return vars(args)

    run_tpch_benchmark("tpch_q13", benchmark_fn)


if __name__ == "__main__":
    ray.init()
    args = parse_tpch_args()
    main(args)
