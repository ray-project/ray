from typing import Tuple

import ray
from ray.data.aggregate import _AggregateOnKeyBase, Max, Mean, Min, Sum
from ray.data.block import Block, KeyFn
from ray.data.dataset import Dataset
import pyarrow.compute as pac

from benchmark import Benchmark


def run_h2oai(benchmark: Benchmark):
    """This benchmark is originally from https://github.com/h2oai/db-benchmark

    Here we run all group-by queries from the benchmark on Ray Datasets.
    The input files are pre-generated and stored in AWS S3 beforehand.
    """

    # Test input file schema={
    #   id1: string, id2: string, id3: string, id4: int64, id5: int64, id6: int64,
    #   v1: int64, v2: int64, v3: double
    # })
    test_input = [
        ("s3://air-example-data/h2oai_benchmark/G1_1e7_1e2_0_0.csv", "h2oai-500M")
    ]
    for path, test_name in test_input:
        input_ds = ray.data.read_csv(path)
        # Number of blocks (parallelism) should be set as number of available CPUs
        # to get best performance.
        num_blocks = int(ray.cluster_resources().get("CPU", 1))
        input_ds = input_ds.repartition(num_blocks).fully_executed()

        q_list = [
            (h2oai_q1, "q1"),
            (h2oai_q3, "q3"),
            (h2oai_q4, "q4"),
            (h2oai_q5, "q5"),
            (h2oai_q7, "q7"),
            (h2oai_q8, "q8"),
        ]

        for q, name in q_list:
            benchmark.run(f"{test_name}-{name}", q, ds=input_ds)


def h2oai_q1(ds: Dataset) -> Dataset:
    return ds.groupby("id1").sum("v1")


def h2oai_q2(ds: Dataset) -> Dataset:
    # TODO(chengsu): Run this after dataset supports multiple group-by keys.
    # return ds.groupby(["id1", "id2"]).sum("v1")
    raise NotImplementedError


def h2oai_q3(ds: Dataset) -> Dataset:
    return ds.groupby("id3").aggregate(Sum("v1"), Mean("v3"))


def h2oai_q4(ds: Dataset) -> Dataset:
    return ds.groupby("id4").aggregate(Mean("v1"), Mean("v2"), Mean("v3"))


def h2oai_q5(ds: Dataset) -> Dataset:
    return ds.groupby("id6").aggregate(Sum("v1"), Sum("v2"), Sum("v3"))


def h2oai_q6(ds: Dataset) -> Dataset:
    # TODO(chengsu): Run this after dataset supports multiple group-by keys.
    # return ds.groupby(["id4", "id5"]).aggregate(Median("v3"), Std("v3"))
    raise NotImplementedError


def h2oai_q7(ds: Dataset) -> Dataset:
    ds = ds.groupby("id3").aggregate(Max("v1"), Min("v2"))
    ds = ds.map_batches(lambda df: df.assign(result=df["max(v1)"] - df["min(v2)"]))
    return ds


def h2oai_q8(ds: Dataset) -> Dataset:
    def accumulate_block(agg: Tuple[float, float], block: Block) -> Tuple[float, float]:
        column = block["v3"]
        top_k_indices = pac.top_k_unstable(column, k=2)
        top_k_result = pac.take(column, top_k_indices).to_pylist()
        top_k_result.extend([float("-inf")] * (2 - len(top_k_result)))
        top_k_result = (top_k_result[0], top_k_result[1])
        return merge(agg, top_k_result)

    def merge(
        agg1: Tuple[float, float],
        agg2: Tuple[float, float],
    ) -> Tuple[float, float]:
        if agg1[0] >= agg2[0]:
            value1 = agg1[0]
            value2 = max(agg1[1], agg2[0])
        else:
            value1 = agg2[0]
            value2 = max(agg1[0], agg2[1])
        return (value1, value2)

    class Top2(_AggregateOnKeyBase):
        def __init__(self, on: KeyFn):
            self._set_key_fn(on)
            super().__init__(
                init=lambda _: (float("-inf"), float("-inf")),
                merge=merge,
                accumulate_block=accumulate_block,
                name=(f"top2({str(on)})"),
            )

    return ds.groupby("id6").aggregate(Top2("v3"))


def h2oai_q9(ds: Dataset) -> Dataset:
    # TODO(chengsu): Run this after dataset supports multiple group-by keys.
    # return ds.groupby(["id2", "id4"]).aggregate(pow(corr("v1", "v2"), 2))
    raise NotImplementedError


def h2oai_q10(ds: Dataset) -> Dataset:
    # TODO(chengsu): Run this after dataset supports multiple group-by keys.
    # return ds.groupby(["id1", "id2", "id3", "id4", "id5", "id6"])
    #          .aggregate(Count(), Sum("v3"))
    raise NotImplementedError


if __name__ == "__main__":
    benchmark = Benchmark("aggregate")

    run_h2oai(benchmark)

    benchmark.write_result()
