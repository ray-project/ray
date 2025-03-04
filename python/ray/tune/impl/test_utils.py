from sklearn.datasets import load_breast_cancer

from ray import tune
from ray.data import Dataset, Datasource, ReadTask, read_datasource
from ray.data.block import BlockMetadata
from ray.tune.impl.utils import execute_dataset


# TODO(xwjiang): Enable this when Clark's out-of-band-serialization is landed.
class TestDatasource(Datasource):
    def prepare_read(self, parallelism: int, **read_args):
        import pyarrow as pa

        def load_data():
            data_raw = load_breast_cancer(as_frame=True)
            dataset_df = data_raw["data"]
            dataset_df["target"] = data_raw["target"]
            return [pa.Table.from_pandas(dataset_df)]

        meta = BlockMetadata(
            num_rows=None,
            size_bytes=None,
            schema=None,
            input_files=None,
            exec_stats=None,
        )
        return [ReadTask(load_data, meta)]


def gen_dataset_func() -> Dataset:
    test_datasource = TestDatasource()
    return read_datasource(test_datasource)


def test_grid_search():
    ds1 = gen_dataset_func().lazy().map(lambda x: x)
    ds2 = gen_dataset_func().lazy().map(lambda x: x)
    assert not ds1._plan._has_final_stage_snapshot()
    assert not ds2._plan._has_final_stage_snapshot()
    param_space = {"train_dataset": tune.grid_search([ds1, ds2])}
    execute_dataset(param_space)
    executed_ds = param_space["train_dataset"]["grid_search"]
    assert len(executed_ds) == 2
    assert executed_ds[0]._plan._has_final_stage_snapshot()
    assert executed_ds[1]._plan._has_final_stage_snapshot()


def test_choice():
    ds1 = gen_dataset_func().lazy().map(lambda x: x)
    ds2 = gen_dataset_func().lazy().map(lambda x: x)
    assert not ds1._plan._has_final_stage_snapshot()
    assert not ds2._plan._has_final_stage_snapshot()
    param_space = {"train_dataset": tune.choice([ds1, ds2])}
    execute_dataset(param_space)
    executed_ds = param_space["train_dataset"].categories
    assert len(executed_ds) == 2
    assert executed_ds[0]._plan._has_final_stage_snapshot()
    assert executed_ds[1]._plan._has_final_stage_snapshot()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
