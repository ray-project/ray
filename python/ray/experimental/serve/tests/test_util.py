import json

from ray.experimental.serve.utils import BytesEncoder


def test_bytes_encoder():
    data_before = {"inp": {"nest": b"bytes"}}
    data_after = {"inp": {"nest": "bytes"}}
    assert json.loads(json.dumps(data_before, cls=BytesEncoder)) == data_after
