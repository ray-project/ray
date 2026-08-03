import json
from unittest.mock import patch

from ray.dashboard.modules.job.job_agent import _encode_log_chunk
from ray.dashboard.modules.job.job_log_storage_client import (
    JOB_LOG_CHUNK_SIZE,
    JobLogStorageClient,
)


def test_get_log_chunks_are_bounded_and_json_encodable(tmp_path):
    pattern = 'line\nquote: " backslash: \\ unicode: \u2603\n'
    content = pattern * (2 * JOB_LOG_CHUNK_SIZE // len(pattern) + 1)
    log_path = tmp_path / "job.log"
    log_path.write_text(content)
    client = JobLogStorageClient()

    with patch.object(client, "get_log_file_path", return_value=log_path):
        chunks = list(client.get_log_chunks("job"))

    assert "".join(chunks) == content
    assert len(chunks) == 3
    assert all(len(chunk) <= JOB_LOG_CHUNK_SIZE for chunk in chunks)

    response = b'{"logs":"' + b"".join(map(_encode_log_chunk, chunks)) + b'"}'
    assert json.loads(response)["logs"] == content
