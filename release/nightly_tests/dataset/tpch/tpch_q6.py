import ray
from ray.data.aggregate import Sum
from ray.data.expressions import col
from common import parse_tpch_args, load_table, to_f64, run_tpch_benchmark


def main(args):
    def benchmark_fn():
        from datetime import datetime

        ds = load_table("lineitem", args.sf)

        # Q6 parameters
        date = datetime(1994, 1, 1)
        discount = 0.06
        quantity = 24

        # Filter by date, discount, and quantity
        ds = ds.filter(
            expr=(
                (col("l_shipdate") >= date)
                & (col("l_shipdate") < datetime(date.year + 1, date.month, date.day))
                & (col("l_discount") >= discount - 0.01)
                & (col("l_discount") <= discount + 0.01)
                & (col("l_quantity") < quantity)
            )
        )

        # Calculate revenue
        ds = ds.with_column(
            "revenue", to_f64(col("l_extendedprice")) * to_f64(col("l_discount"))
        )

        # Aggregate
        result = ds.aggregate(Sum(on="revenue", alias_name="revenue"))
        return result

    run_tpch_benchmark("tpch_q6", benchmark_fn)


if __name__ == "__main__":
    ray.init()
    args = parse_tpch_args()
    main(args)
