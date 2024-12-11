from typing import Any, Callable, Dict, List, Optional

import pyarrow as pa

from ray.data import from_items
from ray.data.datasource import Datasource


def custom_inplace(
    datasource: Datasource,
    value_fn: Dict[str, str] | Callable[[pa.RecordBatch], pa.RecordBatch],
    params: Dict[str, Any] = None,
):
    custom_tasks = datasource.get_custom_tasks(value_fn, params)
    ds = from_items(custom_tasks).map(lambda t: t["item"]())
    # ds is constructed by commit messages
    print(
        f"======={type(ds.take_all())}, 0 elment {type(ds.take_all()[0])}, {ds.take_all()[0]}, -----{ds.take_all()[1]}"
    )

    commit_messages = [item["commit_messages"] for item in ds.take_all()]
    datasource.commit_tasks(commit_messages)
    print(f"=======1111111")


def add_columns(
    datasource: Datasource,
    value_fn: Callable[[pa.RecordBatch], pa.RecordBatch],
    read_columns: List[str],
):
    custom_inplace(
        datasource,
        value_fn=value_fn,
        params={"read_columns": read_columns, "action": "add_column"},
    )


def delete_rows(
    datasource: Datasource,
    value_fn: Callable[[pa.RecordBatch], pa.RecordBatch],
    delete_rows_predicate: str,
):
    custom_inplace(
        datasource,
        value_fn=value_fn,
        params={
            "delete_rows_predicate": delete_rows_predicate,
            "action": "delete_rows",
        },
    )
