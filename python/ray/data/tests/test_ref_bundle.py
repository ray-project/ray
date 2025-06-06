from unittest.mock import patch

from ray import ObjectRef
from ray.data._internal.execution.interfaces import RefBundle
from ray.data.block import BlockMetadata


def test_get_preferred_locations():

    first_block_ref = ObjectRef(b"1" * 28)
    second_block_ref = ObjectRef(b"2" * 28)
    third_block_ref = ObjectRef(b"3" * 28)

    meta = BlockMetadata(num_rows=None, size_bytes=1, exec_stats=None, input_files=None)

    bundle = RefBundle(
        blocks=[
            (first_block_ref, meta),
            (second_block_ref, meta),
            (third_block_ref, meta),
        ],
        owns_blocks=True,
        schema=None,
    )

    def _get_obj_locs(obj_refs):
        assert obj_refs == [first_block_ref, second_block_ref, third_block_ref]

        return {
            first_block_ref: {
                "object_size": 1024,
                "did_spill": False,
                "node_ids": ["1", "2", "3"],
            },
            second_block_ref: {
                "object_size": 2048,
                "did_spill": False,
                "node_ids": ["2", "3"],
            },
            third_block_ref: {
                "object_size": 4096,
                "did_spill": False,
                "node_ids": ["2"],
            },
        }

    with patch("ray.experimental.get_local_object_locations", _get_obj_locs):
        preferred_object_locs = bundle.get_preferred_object_locations()

    assert {
        "1": 1024,  # first_block_ref]
        "2": 7168,  # first_block_ref, second_block_ref, third_block_ref
        "3": 3072,  # first_block_ref, second_block_ref
    } == preferred_object_locs
