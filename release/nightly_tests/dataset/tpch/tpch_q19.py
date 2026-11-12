import ray
from ray.data.aggregate import Sum
from ray.data.expressions import col
from common import load_table, parse_tpch_args, run_tpch_benchmark, to_f64


def main(args):
    def benchmark_fn():
        # Q19: Discounted Revenue Query
        # Revenue from qualifying part/lineitem combinations across three
        # brand/container/quantity/size clauses.
        #
        # Equivalent SQL:
        #   SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue
        #   FROM lineitem, part
        #   WHERE (p_partkey = l_partkey AND p_brand = 'Brand#12'
        #      AND p_container IN ('SM CASE','SM BOX','SM PACK','SM PKG')
        #      AND l_quantity >= 1 AND l_quantity <= 11
        #      AND p_size BETWEEN 1 AND 5
        #      AND l_shipmode IN ('AIR','AIR REG')
        #      AND l_shipinstruct = 'DELIVER IN PERSON')
        #   OR (... 'Brand#23', MED containers, quantity 10..20, size 1..10 ...)
        #   OR (... 'Brand#34', LG containers, quantity 20..30, size 1..15 ...);

        part = load_table("part", args.sf).select_columns(
            ["p_partkey", "p_brand", "p_size", "p_container"]
        )
        lineitem = load_table("lineitem", args.sf).select_columns(
            [
                "l_partkey",
                "l_quantity",
                "l_extendedprice",
                "l_discount",
                "l_shipinstruct",
                "l_shipmode",
            ]
        )

        # Q19 parameters
        clauses = [
            ("Brand#12", ["SM CASE", "SM BOX", "SM PACK", "SM PKG"], 1, 11, 5),
            ("Brand#23", ["MED BAG", "MED BOX", "MED PKG", "MED PACK"], 10, 20, 10),
            ("Brand#34", ["LG CASE", "LG BOX", "LG PACK", "LG PKG"], 20, 30, 15),
        ]

        part_disjunction = None
        disjunction = None
        for brand, containers, qty_lo, qty_hi, size_hi in clauses:
            part_clause = (
                (col("p_brand") == brand)
                & col("p_container").is_in(containers)
                & (col("p_size") >= 1)
                & (col("p_size") <= size_hi)
            )
            clause = (
                part_clause
                & (col("l_quantity") >= qty_lo)
                & (col("l_quantity") <= qty_hi)
            )
            part_disjunction = (
                part_clause
                if part_disjunction is None
                else (part_disjunction | part_clause)
            )
            disjunction = clause if disjunction is None else (disjunction | clause)

        part_filtered = part.filter(expr=part_disjunction)

        min_qty = min(qty_lo for _, _, qty_lo, _, _ in clauses)
        max_qty = max(qty_hi for _, _, _, qty_hi, _ in clauses)
        lineitem_filtered = lineitem.filter(
            expr=col("l_shipmode").is_in(["AIR", "AIR REG"])
            & (col("l_shipinstruct") == "DELIVER IN PERSON")
            & (col("l_quantity") >= min_qty)
            & (col("l_quantity") <= max_qty)
        )

        joined = lineitem_filtered.join(
            part_filtered,
            join_type="inner",
            num_partitions=200,
            on=("l_partkey",),
            right_on=("p_partkey",),
        )

        ds = joined.filter(expr=disjunction)

        ds = ds.with_column(
            "revenue",
            to_f64(col("l_extendedprice")) * (1 - to_f64(col("l_discount"))),
        )

        _ = ds.aggregate(Sum(on="revenue", alias_name="revenue"))

        # Report arguments for the benchmark.
        return vars(args)

    run_tpch_benchmark("tpch_q19", benchmark_fn)


if __name__ == "__main__":
    ray.init()
    args = parse_tpch_args()
    main(args)
