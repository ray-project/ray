import ray
from ray.data.aggregate import Sum
from ray.data.expressions import col
from common import parse_tpch_args, load_table, to_f64, run_tpch_benchmark


def main(args):
    def benchmark_fn():
        from datetime import datetime

        # Q7: Volume Shipping Query
        # Revenue between two nations by supplier nation, customer nation, and ship year.
        #
        # Equivalent SQL:
        #   SELECT supp_nation, cust_nation, l_year,
        #          SUM(l_extendedprice * (1 - l_discount)) AS revenue
        #   FROM supplier, lineitem, orders, customer, nation n1, nation n2
        #   WHERE s_suppkey = l_suppkey
        #     AND o_orderkey = l_orderkey
        #     AND c_custkey = o_custkey
        #     AND s_nationkey = n1.n_nationkey
        #     AND c_nationkey = n2.n_nationkey
        #     AND (
        #       (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
        #        OR
        #       (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
        #     )
        #     AND l_shipdate >= DATE '1995-01-01'
        #     AND l_shipdate <  DATE '1997-01-01'
        #   GROUP BY supp_nation, cust_nation, l_year
        #   ORDER BY supp_nation, cust_nation, l_year;
        #
        # Note:
        # This implementation keeps a mostly linear pipeline:
        # (nation->customer)->orders->lineitem->supplier->nation.

        # Load tables. Projection pushdown derives the columns each read needs.
        supplier = load_table("supplier", args.sf)
        lineitem = load_table("lineitem", args.sf)
        orders = load_table("orders", args.sf)
        customer = load_table("customer", args.sf)
        nation = load_table("nation", args.sf)

        # Q7 parameters
        date1 = datetime(1995, 1, 1)
        date2 = datetime(1997, 1, 1)
        nation1 = "FRANCE"
        nation2 = "GERMANY"

        customer_nation = nation.join(
            customer,
            num_partitions=200,
            join_type="inner",
            on=("n_nationkey",),
            right_on=("c_nationkey",),
        )
        customer_nation = customer_nation.rename_columns({"n_name": "n_name_cust"})
        # Drop nation's other columns so the second nation join does not
        # collide on them.
        customer_nation = customer_nation.select_columns(["c_custkey", "n_name_cust"])

        orders_customer = orders.join(
            customer_nation,
            num_partitions=200,
            join_type="inner",
            on=("o_custkey",),
            right_on=("c_custkey",),
            left_suffix="",
        )

        # Join lineitem with orders and filter by date
        lineitem_filtered = lineitem.filter(
            expr=((col("l_shipdate") >= date1) & (col("l_shipdate") < date2))
        )
        lineitem_orders = lineitem_filtered.join(
            orders_customer,
            num_partitions=200,
            join_type="inner",
            on=("l_orderkey",),
            right_on=("o_orderkey",),
        )

        # Keep supplier join and supplier-nation join in the same linear pipeline.
        lineitem_supplier = lineitem_orders.join(
            supplier,
            num_partitions=200,
            join_type="inner",
            on=("l_suppkey",),
            right_on=("s_suppkey",),
        )

        ds = lineitem_supplier.join(
            nation,
            num_partitions=200,
            join_type="inner",
            on=("s_nationkey",),
            right_on=("n_nationkey",),
        ).rename_columns({"n_name": "n_name_supp"})

        ds = ds.filter(
            expr=(
                ((col("n_name_supp") == nation1) & (col("n_name_cust") == nation2))
                | ((col("n_name_supp") == nation2) & (col("n_name_cust") == nation1))
            )
        )

        # Calculate revenue
        ds = ds.with_column(
            "revenue",
            to_f64(col("l_extendedprice")) * (1 - to_f64(col("l_discount"))),
        )

        # Extract year from shipdate
        ds = ds.with_column(
            "l_year",
            col("l_shipdate").dt.year(),
        )

        # Aggregate by supplier nation, customer nation, and year
        _ = (
            ds.groupby(["n_name_supp", "n_name_cust", "l_year"])
            .aggregate(Sum(on="revenue", alias_name="revenue"))
            .sort(key=["n_name_supp", "n_name_cust", "l_year"])
            .materialize()
        )

        # Report arguments for the benchmark.
        return vars(args)

    run_tpch_benchmark("tpch_q7", benchmark_fn)


if __name__ == "__main__":
    ray.init()
    args = parse_tpch_args()
    main(args)
