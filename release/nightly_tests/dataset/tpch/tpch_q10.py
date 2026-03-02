import ray
from ray.data.aggregate import Sum
from ray.data.expressions import col
from common import parse_tpch_args, load_table, to_f64, run_tpch_benchmark


def main(args):
    def benchmark_fn():
        from datetime import datetime

        # Q10: Returned Item Reporting Query
        # Top customers by revenue from returned lineitems in a 3-month order-date window.
        #
        # Equivalent SQL:
        #   SELECT c_custkey, c_name,
        #          SUM(l_extendedprice * (1 - l_discount)) AS revenue,
        #          c_acctbal, n_name, c_address, c_phone, c_comment
        #   FROM customer, orders, lineitem, nation
        #   WHERE c_custkey = o_custkey
        #     AND l_orderkey = o_orderkey
        #     AND o_orderdate >= DATE '1993-10-01'
        #     AND o_orderdate <  DATE '1994-01-01'
        #     AND l_returnflag = 'R'
        #     AND c_nationkey = n_nationkey
        #   GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
        #   ORDER BY revenue DESC;
        #
        # Note:
        # The pipeline is kept linear:
        # orders -> customer -> nation -> lineitem.

        # Load all required tables with early projection.
        customer = load_table("customer", args.sf).select_columns(
            [
                "c_custkey",
                "c_name",
                "c_nationkey",
                "c_acctbal",
                "c_address",
                "c_phone",
                "c_comment",
            ]
        )
        orders = load_table("orders", args.sf).select_columns(
            ["o_orderkey", "o_custkey", "o_orderdate"]
        )
        lineitem = load_table("lineitem", args.sf).select_columns(
            ["l_orderkey", "l_extendedprice", "l_discount", "l_returnflag"]
        )
        nation = load_table("nation", args.sf).select_columns(["n_nationkey", "n_name"])

        # Q10 parameters
        date = datetime(1993, 10, 1)
        # Calculate end date (3 months later)
        if date.month <= 9:
            end_date = datetime(date.year, date.month + 3, date.day)
        else:
            end_date = datetime(date.year + 1, date.month + 3 - 12, date.day)

        # Filter orders by date (3 months range)
        orders_filtered = orders.filter(
            expr=((col("o_orderdate") >= date) & (col("o_orderdate") < end_date))
        )

        # Filter lineitem by return flag
        lineitem_filtered = lineitem.filter(expr=col("l_returnflag") == "R")

        # Join orders with customer.
        orders_customer = orders_filtered.join(
            customer,
            join_type="inner",
            num_partitions=16,
            on=("o_custkey",),
            right_on=("c_custkey",),
        )

        # Join with nation.
        orders_customer_nation = orders_customer.join(
            nation,
            join_type="inner",
            num_partitions=16,
            on=("c_nationkey",),
            right_on=("n_nationkey",),
        )
        orders_customer_nation = orders_customer_nation.select_columns(
            [
                "o_orderkey",
                "o_custkey",
                "c_name",
                "c_acctbal",
                "n_name",
                "c_address",
                "c_phone",
                "c_comment",
            ]
        )

        # Join with returned lineitems.
        ds = orders_customer_nation.join(
            lineitem_filtered,
            join_type="inner",
            num_partitions=16,
            on=("o_orderkey",),
            right_on=("l_orderkey",),
        )

        # Calculate revenue
        ds = ds.with_column(
            "revenue",
            to_f64(col("l_extendedprice")) * (1 - to_f64(col("l_discount"))),
        )

        # Aggregate by customer key, customer name, address, phone, account balance, nation, and region
        _ = (
            ds.groupby(
                [
                    "o_custkey",
                    "c_name",
                    "c_acctbal",
                    "n_name",
                    "c_address",
                    "c_phone",
                    "c_comment",
                ]
            )
            .aggregate(Sum(on="revenue", alias_name="revenue"))
            .sort(key="revenue", descending=True)
            .materialize()
        )

        # Report arguments for the benchmark.
        return vars(args)

    run_tpch_benchmark("tpch_q10", benchmark_fn)


if __name__ == "__main__":
    ray.init()
    args = parse_tpch_args()
    main(args)
