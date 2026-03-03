import contextlib
import traceback

import ray


def _deserialize_and_fully_execute_if_needed(serialized_ds: bytes):
    ds = ray.data.Dataset.deserialize_lineage(serialized_ds)
    return ds


def _reduce(ds: ray.data.Dataset):
    tb_list = traceback.format_list(traceback.extract_stack())
    _already_in_out_of_band_serialization = False
    for tb in tb_list:
        # TODO(xwjiang): Let's make this less hacky.
        if "serialize_lineage" in tb:
            _already_in_out_of_band_serialization = True
            break
    if not _already_in_out_of_band_serialization and ds.has_serializable_lineage():
        return _deserialize_and_fully_execute_if_needed, (ds.serialize_lineage(),)
    else:
        return ds.__reduce__()


@contextlib.contextmanager
def out_of_band_serialize_dataset():
    context = ray._private.worker.global_worker.get_serialization_context()
    try:
        context._register_cloudpickle_reducer(ray.data.Dataset, _reduce)
        yield
    finally:
        context._unregister_cloudpickle_reducer(ray.data.Dataset)
