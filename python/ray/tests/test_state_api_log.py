import json
import os
import sys
import asyncio
from typing import List
import urllib
from unittest.mock import MagicMock, AsyncMock

import pytest
from ray.util.state.state_cli import logs_state_cli_group
from ray.util.state import list_jobs
import requests
from click.testing import CliRunner
import grpc

from pathlib import Path

import ray
from ray._private.test_utils import (
    format_web_url,
    wait_for_condition,
    wait_until_server_available,
    skip_flaky_core_test_premerge,
)

from ray._private.ray_constants import (
    LOG_PREFIX_TASK_ATTEMPT_START,
    LOG_PREFIX_TASK_ATTEMPT_END,
)
from ray._raylet import ActorID, NodeID, TaskID, WorkerID
from ray.core.generated.common_pb2 import Address
from ray.core.generated.gcs_service_pb2 import GetTaskEventsReply
from ray.core.generated.reporter_pb2 import ListLogsReply, StreamLogReply
from ray.core.generated.gcs_pb2 import (
    ActorTableData,
    TaskEvents,
    TaskStateUpdate,
    TaskLogInfo,
)
from ray.dashboard.modules.actor.actor_head import actor_table_data_to_dict
from ray.dashboard.modules.log.log_agent import (
    find_offset_of_content_in_file,
    find_end_offset_file,
    find_end_offset_next_n_lines_from_offset,
    find_start_offset_last_n_lines_from_offset,
    LogAgentV1Grpc,
)
from ray.dashboard.modules.log.log_agent import _stream_log_in_chunk
from ray.dashboard.modules.log.log_manager import LogsManager
from ray.dashboard.tests.conftest import *  # noqa
from ray.util.state import get_log, list_logs, list_nodes, list_workers
from ray.util.state.common import GetLogOptions
from ray.util.state.exception import DataSourceUnavailable, RayStateApiException
from ray.util.state.state_manager import StateDataSourceClient


def generate_task_event(
    task_id,
    node_id,
    attempt_number,
    worker_id,
    stdout_file=None,
    stderr_file=None,
    stdout_start=None,
    stderr_start=None,
    stdout_end=None,
    stderr_end=None,
):
    task_event = TaskEvents(
        task_id=task_id.binary(),
        attempt_number=attempt_number,
        job_id=b"",
        state_updates=TaskStateUpdate(
            node_id=node_id.binary(),
            worker_id=worker_id.binary(),
            task_log_info=TaskLogInfo(
                stdout_file=stdout_file,
                stderr_file=stderr_file,
                stdout_start=stdout_start,
                stderr_start=stderr_start,
                stdout_end=stdout_end,
                stderr_end=stderr_end,
            ),
        ),
    )

    return task_event


def generate_actor_data(id, node_id, worker_id):
    if worker_id:
        worker_id = worker_id.binary()
    message = ActorTableData(
        actor_id=id.binary(),
        state=ActorTableData.ActorState.ALIVE,
        name="abc",
        pid=1234,
        class_name="class",
        address=Address(
            raylet_id=node_id.binary(),
            ip_address="127.0.0.1",
            port=1234,
            worker_id=worker_id,
        ),
    )
    return actor_table_data_to_dict(message)


# Unit Tests (Log Agent)
def _read_file(fp, start, end):
    """Help func to read a file with offsets"""
    fp.seek(start, 0)
    if end == -1:
        return fp.read()
    return fp.read(end - start)


async def _stream_log(context, fp, start, end):
    """Help func to stream a log with offsets"""
    result = bytearray()
    async for chunk_res in _stream_log_in_chunk(
        context=context,
        file=fp,
        start_offset=start,
        end_offset=end,
        keep_alive_interval_sec=-1,
    ):
        result += chunk_res.data
    return result


TEST_LINE_TEMPLATE = "{}-test-line"


def _write_lines_and_get_offset_at_index(
    f, num_lines, start_offset=0, trailing_new_line=True
):
    """
    Write multiple lines into a file, and record offsets

    Args:
        f: a binary file object that's writable
        num_lines: Number of lines to write
        start_offset: The offset to start writing
        trailing_new_line: True if a '\n' is added at the end of the
            lines.

    Return:
        offsets: A list of offsets of the lines.
        offset_end: The offset of the end of file.
    """
    f.seek(start_offset, 0)

    offsets = []
    for i in range(num_lines):
        offsets.append(f.tell())
        if i == num_lines - 1 and not trailing_new_line:
            # Last line no newline
            line = TEST_LINE_TEMPLATE.format(i)
        else:
            line = TEST_LINE_TEMPLATE.format(i) + "\n"
        f.write(line.encode("utf-8"))

    f.flush()
    f.seek(0, 2)
    offset_end = f.tell()

    return offsets, offset_end


@pytest.mark.parametrize("new_line", [True, False])
@pytest.mark.parametrize("block_size", [4, 16, 256])
def test_find_start_offset_last_n_lines_from_offset(new_line, temp_file, block_size):
    file = temp_file
    o, end_file = _write_lines_and_get_offset_at_index(
        file, num_lines=50, start_offset=0, trailing_new_line=new_line
    )
    # Test the function with different offsets and number of lines to find
    assert find_start_offset_last_n_lines_from_offset(file, o[3], 1, block_size) == o[2]
    assert (
        find_start_offset_last_n_lines_from_offset(file, o[10], 10, block_size) == o[0]
    )

    # Test end of file last 1 line
    assert find_start_offset_last_n_lines_from_offset(file, -1, 1, block_size) == o[-1]

    # Test end of file no line
    assert (
        find_start_offset_last_n_lines_from_offset(file, -1, 0, block_size) == end_file
    )

    # Test no line from middle of file
    assert (
        find_start_offset_last_n_lines_from_offset(file, o[30], 0, block_size) == o[30]
    )

    # Test more lines than file
    assert (
        find_start_offset_last_n_lines_from_offset(file, o[30], 100, block_size) == o[0]
    )

    # Test offsets in the middle of a line
    assert (
        find_start_offset_last_n_lines_from_offset(file, o[2] + 1, 1, block_size)
        == o[2]
    )
    assert (
        find_start_offset_last_n_lines_from_offset(file, o[2] - 1, 1, block_size)
        == o[1]
    )


def test_find_end_offset_next_n_lines_from_offset(temp_file):
    file = temp_file
    o, end_file = _write_lines_and_get_offset_at_index(
        file, num_lines=10, start_offset=0
    )
    # Test the function with different offsets and number of lines to find
    assert find_end_offset_next_n_lines_from_offset(file, o[3], 1) == o[4]
    assert find_end_offset_next_n_lines_from_offset(file, o[3], 2) == o[5]
    assert find_end_offset_next_n_lines_from_offset(file, 0, 1) == o[1]

    # Test end of file
    assert find_end_offset_next_n_lines_from_offset(file, o[3], 999) == end_file

    # Test offset diff
    assert find_end_offset_next_n_lines_from_offset(file, 1, 1) == o[1]
    assert find_end_offset_next_n_lines_from_offset(file, o[1] - 1, 1) == o[1]


def test_find_offset_of_content_in_file(temp_file):
    file = temp_file
    o, end_file = _write_lines_and_get_offset_at_index(file, num_lines=10)

    assert (
        find_offset_of_content_in_file(
            file, TEST_LINE_TEMPLATE.format(0).encode("utf-8")
        )
        == o[0]
    )

    assert (
        find_offset_of_content_in_file(
            file, TEST_LINE_TEMPLATE.format(3).encode("utf-8"), o[1] + 1
        )
        == o[3]
    )

    assert (
        find_offset_of_content_in_file(
            file, TEST_LINE_TEMPLATE.format(4).encode("utf-8"), o[1] - 1
        )
        == o[4]
    )

    # Not found
    assert (
        find_offset_of_content_in_file(
            file, TEST_LINE_TEMPLATE.format(1000).encode("utf-8"), o[1] - 1
        )
        == -1
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("random_ascii_file", [1 << 20], indirect=True)
@pytest.mark.parametrize(
    "start_offset,end_offset",
    [
        (0, 1 << 20),
        (1 << 20, 1 << 20),
        (0, 0),
        (0, 1),
        (1 << 16, 1 << 20),
        (1024, 2042),
    ],
)
async def test_stream_log_in_chunk(random_ascii_file, start_offset, end_offset):
    """Test streaming of a file from different offsets"""
    test_file = random_ascii_file
    context = MagicMock(grpc.aio.ServicerContext)
    context.done.return_value = False

    expected_file_content = _read_file(test_file, start_offset, end_offset)
    actual_log_content = await _stream_log(context, test_file, start_offset, end_offset)

    assert (
        expected_file_content == actual_log_content
    ), "Non-matching content from log streamed"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "lines_to_tail,total_lines",
    [(0, 100), (100, 100), (10, 100), (1, 100), (99, 100)],
)
@pytest.mark.parametrize("trailing_new_line", [True, False])
async def test_log_tails(lines_to_tail, total_lines, trailing_new_line, temp_file):
    """Test tailing a file works"""
    _write_lines_and_get_offset_at_index(
        temp_file,
        total_lines,
        trailing_new_line=trailing_new_line,
    )
    test_file = temp_file
    context = MagicMock(grpc.aio.ServicerContext)
    context.done.return_value = False
    start_offset = find_start_offset_last_n_lines_from_offset(
        test_file, offset=-1, n=lines_to_tail
    )

    actual_data = await _stream_log(context, test_file, start_offset, -1)
    expected_data = _read_file(test_file, start_offset, -1)

    assert actual_data == expected_data, "Non-matching data from stream log"

    all_lines = actual_data.decode("utf-8")
    assert all_lines.count("\n") == (
        lines_to_tail if trailing_new_line or lines_to_tail == 0 else lines_to_tail - 1
    ), "Non-matching number of lines tailed"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "lines_to_tail,total_lines",
    [(0, 5), (5, 5), (2, 5), (1, 5), (4, 5)],
)
async def test_log_tails_with_appends(lines_to_tail, total_lines, temp_file):
    """Test tailing a log file that grows at the same time"""
    _write_lines_and_get_offset_at_index(temp_file, total_lines)
    test_file = temp_file
    context = MagicMock(grpc.aio.ServicerContext)
    context.done.return_value = False
    start_offset = find_start_offset_last_n_lines_from_offset(
        test_file, offset=-1, n=lines_to_tail
    )

    actual_data = await _stream_log(context, test_file, start_offset, -1)

    end_offset = find_end_offset_file(test_file)
    expected_data = _read_file(test_file, start_offset, end_offset)
    assert actual_data == expected_data, "Non-matching data from stream log"

    all_lines = actual_data.decode("utf-8")
    assert all_lines.count("\n") == lines_to_tail, "Non-matching number of lines tailed"

    # Modify the file with append here
    num_new_lines = 2
    _write_lines_and_get_offset_at_index(
        temp_file, num_new_lines, start_offset=end_offset
    )

    # Tail again should read the new lines written
    start_offset = find_start_offset_last_n_lines_from_offset(
        test_file, offset=-1, n=lines_to_tail + num_new_lines
    )

    expected_data = _read_file(test_file, start_offset, -1)
    actual_data = await _stream_log(context, test_file, start_offset, -1)

    assert (
        actual_data == expected_data
    ), "Non-matching data from stream log after append"

    all_lines = actual_data.decode("utf-8")
    assert (
        all_lines.count("\n") == lines_to_tail + num_new_lines
    ), "Non-matching number of lines tailed after append"


@pytest.mark.asyncio
async def test_log_agent_find_task_log_offsets(temp_file):
    log_file_content = ""
    task_id = "taskid1234"
    attempt_number = 0
    # Previous data
    for i in range(3):
        log_file_content += TEST_LINE_TEMPLATE.format(i) + "\n"
    # Task's logs
    log_file_content += f"{LOG_PREFIX_TASK_ATTEMPT_START}{task_id}-{attempt_number}\n"
    expected_start = len(log_file_content)
    for i in range(10):
        log_file_content += TEST_LINE_TEMPLATE.format(i) + "\n"
    expected_end = len(log_file_content)
    log_file_content += f"{LOG_PREFIX_TASK_ATTEMPT_END}{task_id}-{attempt_number}\n"

    # Next data
    for i in range(3):
        log_file_content += TEST_LINE_TEMPLATE.format(i) + "\n"

    # Write to files
    temp_file.write(log_file_content.encode("utf-8"))

    # Test all task logs
    start_offset, end_offset = await LogAgentV1Grpc._find_task_log_offsets(
        task_id, attempt_number, -1, temp_file
    )
    assert start_offset == expected_start
    assert end_offset == expected_end

    # Test tailing last X lines
    num_tail = 3
    start_offset, end_offset = await LogAgentV1Grpc._find_task_log_offsets(
        task_id, attempt_number, num_tail, temp_file
    )
    assert end_offset == expected_end
    exclude_tail_content = ""
    for i in range(10 - num_tail):
        exclude_tail_content += TEST_LINE_TEMPLATE.format(i) + "\n"
    assert start_offset == expected_start + len(exclude_tail_content)


def test_log_agent_resolve_filename(temp_dir):
    """
    Test that LogAgentV1Grpc.resolve_filename(root, filename) works:
    1. Not possible to resolve a file that doesn't exist.
    2. Not able to resolve files outside of the temp dir root.
        - with a absolute path.
        - with a relative path recursive up.
    3. Permits a file in a directory that's symlinked into the root dir.
    """
    root = Path(temp_dir)
    # Create a file in the temp dir.
    file = root / "valid_file"
    file.touch()
    subdir = root / "subdir"
    subdir.mkdir()

    # Create a directory in the root that contains a valid file and
    # is symlinked to by a path in the subdir.
    symlinked_dir = root / "symlinked"
    symlinked_dir.mkdir()
    symlinked_file = symlinked_dir / "valid_file"
    symlinked_file.touch()
    symlinked_path_in_subdir = subdir / "symlink_to_outside_dir"
    symlinked_path_in_subdir.symlink_to(symlinked_dir)

    # Test file doesn't exist
    with pytest.raises(FileNotFoundError):
        LogAgentV1Grpc._resolve_filename(root, "non-exist-file")

    # Test absolute path outside of root is not allowed
    with pytest.raises(FileNotFoundError):
        LogAgentV1Grpc._resolve_filename(subdir, root.resolve() / "valid_file")

    # Test relative path recursive up is not allowed
    with pytest.raises(FileNotFoundError):
        LogAgentV1Grpc._resolve_filename(subdir, "../valid_file")

    # Test relative path a valid file is allowed
    assert (
        LogAgentV1Grpc._resolve_filename(root, "valid_file")
        == (root / "valid_file").resolve()
    )

    # Test relative path to a valid file following a symlink is allowed
    assert (
        LogAgentV1Grpc._resolve_filename(subdir, "symlink_to_outside_dir/valid_file")
        == (root / "symlinked" / "valid_file").resolve()
    )


# Unit Tests (LogsManager)


@pytest.fixture
def logs_manager():
    client = AsyncMock(StateDataSourceClient)
    manager = LogsManager(client)
    yield manager


def generate_list_logs(log_files: List[str]):
    return ListLogsReply(log_files=log_files)


def generate_logs_stream_chunk(index: int):
    return f"{str(index)*10}"


async def generate_logs_stream(num_chunks: int):
    for i in range(num_chunks):
        data = generate_logs_stream_chunk(index=i)
        yield StreamLogReply(data=data.encode())


@pytest.mark.asyncio
async def test_logs_manager_list_logs(logs_manager):
    logs_client = logs_manager.data_source_client

    logs_client.get_all_registered_log_agent_ids = MagicMock()
    logs_client.get_all_registered_log_agent_ids.return_value = ["1", "2"]

    logs_client.list_logs.side_effect = [
        generate_list_logs(["gcs_server.out"]),
        DataSourceUnavailable(),
    ]

    # Unregistered node id should raise a DataSourceUnavailable.
    with pytest.raises(DataSourceUnavailable):
        result = await logs_manager.list_logs(
            node_id="3", timeout=30, glob_filter="*gcs*"
        )

    result = await logs_manager.list_logs(node_id="2", timeout=30, glob_filter="*gcs*")
    assert len(result) == 1
    assert result["gcs_server"] == ["gcs_server.out"]
    assert result["raylet"] == []
    logs_client.get_all_registered_log_agent_ids.assert_called()
    logs_client.list_logs.assert_awaited_with("2", "*gcs*", timeout=30)

    # The second call raises DataSourceUnavailable, which will
    # return DataSourceUnavailable to the caller.
    with pytest.raises(DataSourceUnavailable):
        result = await logs_manager.list_logs(
            node_id="1", timeout=30, glob_filter="*gcs*"
        )


@pytest.mark.asyncio
async def test_logs_manager_resolve_file(logs_manager):
    node_id = NodeID(b"1" * 28)
    """
    Test filename is given.
    """
    logs_client = logs_manager.data_source_client
    logs_client.get_all_registered_log_agent_ids = MagicMock()
    logs_client.get_all_registered_log_agent_ids.return_value = [node_id.hex()]
    expected_filename = "filename"
    res = await logs_manager.resolve_filename(
        node_id=node_id.hex(),
        log_filename=expected_filename,
        actor_id=None,
        task_id=None,
        pid=None,
        get_actor_fn=lambda _: True,
        timeout=10,
    )
    log_file_name, n = res.filename, res.node_id
    assert log_file_name == expected_filename
    assert n == node_id.hex()
    """
    Test actor id is given.
    """
    # Actor doesn't exist.
    with pytest.raises(ValueError):
        actor_id = ActorID(b"2" * 16)

        def get_actor_fn(id):
            if id == actor_id:
                return None
            assert False, "Not reachable."

        await logs_manager.resolve_filename(
            node_id=node_id.hex(),
            log_filename=None,
            actor_id=actor_id,
            task_id=None,
            pid=None,
            get_actor_fn=get_actor_fn,
            timeout=10,
        )

    # Actor exists, but it is not scheduled yet.
    actor_id = ActorID(b"2" * 16)

    with pytest.raises(ValueError):
        await logs_manager.resolve_filename(
            node_id=node_id.hex(),
            log_filename=None,
            actor_id=actor_id,
            task_id=None,
            pid=None,
            get_actor_fn=lambda _: generate_actor_data(actor_id, node_id, None),
            timeout=10,
        )

    # Actor exists.
    actor_id = ActorID(b"2" * 16)
    worker_id = WorkerID(b"3" * 28)
    logs_manager.list_logs = AsyncMock()
    logs_manager.list_logs.return_value = {
        "worker_out": [f"worker-{worker_id.hex()}-123-123.out"],
        "worker_err": [],
    }
    res = await logs_manager.resolve_filename(
        node_id=node_id.hex(),
        log_filename=None,
        actor_id=actor_id,
        task_id=None,
        pid=None,
        get_actor_fn=lambda _: generate_actor_data(actor_id, node_id, worker_id),
        timeout=10,
    )
    log_file_name, n = res.filename, res.node_id
    logs_manager.list_logs.assert_awaited_with(
        node_id.hex(), 10, glob_filter=f"*{worker_id.hex()}*out"
    )
    assert log_file_name == f"worker-{worker_id.hex()}-123-123.out"
    assert n == node_id.hex()

    """
    Test task id is given.
    """
    task_id = TaskID(b"2" * 24)
    logs_client = logs_manager.data_source_client
    logs_client.get_all_task_info = AsyncMock()
    logs_client.get_all_task_info.return_value = GetTaskEventsReply(
        events_by_task=[
            generate_task_event(
                task_id,
                node_id,
                attempt_number=1,
                worker_id=worker_id,
                stdout_file=f"worker-{worker_id.hex()}-123-123.out",
            )
        ]
    )

    # Expect resolved file.
    res = await logs_manager.resolve_filename(task_id=task_id, attempt_number=1)
    filename, n = res.filename, res.node_id
    # Default out file. See generate_task_event() for filename
    assert filename == f"worker-{worker_id.hex()}-123-123.out"
    assert n == node_id.hex()

    # Wrong task attempt
    with pytest.raises(FileNotFoundError):
        await logs_manager.resolve_filename(task_id=task_id, attempt_number=0)

    # No task found
    logs_client.get_all_task_info.return_value = GetTaskEventsReply(events_by_task=[])
    with pytest.raises(FileNotFoundError):
        await logs_manager.resolve_filename(task_id=TaskID(b"1" * 24), attempt_number=1)

    """
    Test pid is given.
    """
    # Pid doesn't exist.
    with pytest.raises(FileNotFoundError):
        pid = 456
        logs_manager.list_logs = AsyncMock()
        # Provide the wrong pid.
        logs_manager.list_logs.return_value = {
            "worker_out": ["worker-123-123-123.out"],
            "worker_err": [],
        }
        await logs_manager.resolve_filename(
            node_id=node_id.hex(),
            log_filename=None,
            actor_id=None,
            task_id=None,
            pid=pid,
            get_actor_fn=lambda _: generate_actor_data(actor_id, node_id, worker_id),
            timeout=10,
        )

    # Pid exists.
    pid = 123
    logs_manager.list_logs = AsyncMock()
    # Provide the wrong pid.
    logs_manager.list_logs.return_value = {
        "worker_out": [f"worker-123-123-{pid}.out"],
        "worker_err": [],
    }
    res = await logs_manager.resolve_filename(
        node_id=node_id.hex(),
        log_filename=None,
        actor_id=None,
        task_id=None,
        pid=pid,
        get_actor_fn=lambda _: generate_actor_data(actor_id, node_id, worker_id),
        timeout=10,
    )
    log_file_name, n = res.filename, res.node_id
    logs_manager.list_logs.assert_awaited_with(
        node_id.hex(), 10, glob_filter=f"*{pid}*out"
    )
    assert log_file_name == f"worker-123-123-{pid}.out"

    """
    Test nothing is given.
    """
    with pytest.raises(FileNotFoundError):
        await logs_manager.resolve_filename(
            node_id=node_id.hex(),
            log_filename=None,
            actor_id=None,
            task_id=None,
            pid=None,
            get_actor_fn=lambda _: generate_actor_data(actor_id, node_id, worker_id),
            timeout=10,
        )

    """
    Test suffix is specified
    """
    pid = 123
    logs_manager.list_logs = AsyncMock()
    logs_manager.list_logs.return_value = {
        "worker_out": [f"worker-123-123-{pid}.out"],
        "worker_err": [],
    }
    res = await logs_manager.resolve_filename(
        node_id=node_id.hex(),
        log_filename=None,
        actor_id=None,
        task_id=None,
        pid=pid,
        get_actor_fn=lambda _: generate_actor_data(actor_id, node_id, worker_id),
        timeout=10,
    )
    log_file_name, n = res.filename, res.node_id
    logs_manager.list_logs.assert_awaited_with(
        node_id.hex(), 10, glob_filter=f"*{pid}*out"
    )
    assert log_file_name == f"worker-123-123-{pid}.out"

    logs_manager.list_logs.return_value = {
        "worker_out": [],
        "worker_err": [f"worker-123-123-{pid}.err"],
    }
    res = await logs_manager.resolve_filename(
        node_id=node_id.hex(),
        log_filename=None,
        actor_id=None,
        task_id=None,
        pid=pid,
        get_actor_fn=lambda _: generate_actor_data(actor_id, node_id, worker_id),
        timeout=10,
        suffix="err",
    )
    log_file_name, n = res.filename, res.node_id
    logs_manager.list_logs.assert_awaited_with(
        node_id.hex(), 10, glob_filter=f"*{pid}*err"
    )
    assert log_file_name == f"worker-123-123-{pid}.err"


@pytest.mark.asyncio
async def test_logs_manager_stream_log(logs_manager):
    NUM_LOG_CHUNKS = 10
    logs_client = logs_manager.data_source_client

    logs_client.get_all_registered_log_agent_ids = MagicMock()
    logs_client.get_all_registered_log_agent_ids.return_value = ["1", "2"]
    logs_client.ip_to_node_id = MagicMock()
    logs_client.stream_log.return_value = generate_logs_stream(NUM_LOG_CHUNKS)

    # Test file_name, media_type="file", node_id
    options = GetLogOptions(
        timeout=30, media_type="file", lines=10, node_id="1", filename="raylet.out"
    )

    i = 0
    async for chunk in logs_manager.stream_logs(options):
        assert chunk.decode("utf-8") == generate_logs_stream_chunk(index=i)
        i += 1
    assert i == NUM_LOG_CHUNKS
    logs_client.stream_log.assert_awaited_with(
        node_id="1",
        log_file_name="raylet.out",
        keep_alive=False,
        lines=10,
        interval=None,
        timeout=30,
        start_offset=None,
        end_offset=None,
    )

    # Test pid, media_type = "stream", node_ip

    logs_client.ip_to_node_id.return_value = "1"
    logs_client.list_logs.side_effect = [
        generate_list_logs(
            ["worker-0-0-10.out", "worker-0-0-11.out", "worker-0-0-10.err"]
        ),
    ]
    options = GetLogOptions(
        timeout=30, media_type="stream", lines=10, interval=0.5, node_id="1", pid="10"
    )

    logs_client.stream_log.return_value = generate_logs_stream(NUM_LOG_CHUNKS)
    i = 0
    async for chunk in logs_manager.stream_logs(options):
        assert chunk.decode("utf-8") == generate_logs_stream_chunk(index=i)
        i += 1
    assert i == NUM_LOG_CHUNKS
    logs_client.stream_log.assert_awaited_with(
        node_id="1",
        log_file_name="worker-0-0-10.out",
        keep_alive=True,
        lines=10,
        interval=0.5,
        timeout=None,
        start_offset=None,
        end_offset=None,
    )

    # Currently cannot test actor_id with AsyncMock.
    # It will be tested by the integration test.


@pytest.mark.asyncio
async def test_logs_manager_keepalive_no_timeout(logs_manager):
    """Test when --follow is specified, there's no timeout.

    Related: https://github.com/ray-project/ray/issues/25721
    """
    NUM_LOG_CHUNKS = 10
    logs_client = logs_manager.data_source_client

    logs_client.get_all_registered_log_agent_ids = MagicMock()
    logs_client.get_all_registered_log_agent_ids.return_value = ["1", "2"]
    logs_client.ip_to_node_id = MagicMock()
    logs_client.stream_log.return_value = generate_logs_stream(NUM_LOG_CHUNKS)

    # Test file_name, media_type="file", node_id
    options = GetLogOptions(
        timeout=30, media_type="stream", lines=10, node_id="1", filename="raylet.out"
    )

    async for chunk in logs_manager.stream_logs(options):
        pass

    # Make sure timeout == None when media_type == stream. This is to avoid
    # closing the connection due to DEADLINE_EXCEEDED when --follow is specified.
    logs_client.stream_log.assert_awaited_with(
        node_id="1",
        log_file_name="raylet.out",
        keep_alive=True,
        lines=10,
        interval=None,
        timeout=None,
        start_offset=None,
        end_offset=None,
    )


# Integration tests


def test_logs_list(ray_start_with_dashboard):
    assert (
        wait_until_server_available(ray_start_with_dashboard.address_info["webui_url"])
        is True
    )
    webui_url = ray_start_with_dashboard.address_info["webui_url"]
    webui_url = format_web_url(webui_url)
    node_id = list_nodes()[0]["node_id"]

    def verify():
        response = requests.get(webui_url + f"/api/v0/logs?node_id={node_id}")
        response.raise_for_status()
        result = json.loads(response.text)
        assert result["result"]
        logs = result["data"]["result"]

        # Test worker logs
        outs = logs["worker_out"]
        errs = logs["worker_err"]
        core_worker_logs = logs["core_worker"]

        assert len(outs) == len(errs) == len(core_worker_logs)
        assert len(outs) > 0

        # Test gcs / raylet / dashboard
        for file in ["gcs_server.out", "gcs_server.err"]:
            assert file in logs["gcs_server"]
        for file in ["raylet.out", "raylet.err"]:
            assert file in logs["raylet"]
        for file in ["dashboard.log"]:
            assert file in logs["dashboard"]
        for file in ["dashboard_agent.log"]:
            assert file in logs["agent"]
        return True

    wait_for_condition(verify)

    def verify_filter():
        # Test that logs/list can be filtered
        response = requests.get(
            webui_url + f"/api/v0/logs?node_id={node_id}&glob=*gcs*"
        )
        response.raise_for_status()
        result = json.loads(response.text)
        assert result["result"]
        logs = result["data"]["result"]
        assert "gcs_server" in logs
        assert "internal" in logs
        assert len(logs) == 2
        assert "gcs_server.out" in logs["gcs_server"]
        assert "gcs_server.err" in logs["gcs_server"]
        assert "debug_state_gcs.txt" in logs["internal"]
        return True

    wait_for_condition(verify_filter)

    def verify_worker_logs():
        response = requests.get(
            webui_url + f"/api/v0/logs?node_id={node_id}&glob=*worker*"
        )
        response.raise_for_status()
        result = json.loads(response.text)
        assert result["result"]
        logs = result["data"]["result"]
        worker_log_categories = [
            "core_worker",
            "worker_out",
            "worker_err",
        ]
        assert all([cat in logs for cat in worker_log_categories])
        num_workers = len(
            list(filter(lambda w: w["worker_type"] == "WORKER", list_workers()))
        )
        assert (
            len(logs["worker_out"])
            == len(logs["worker_err"])
            == len(logs["worker_out"])
        )
        assert num_workers == len(logs["worker_out"])
        return True

    wait_for_condition(verify_worker_logs)


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_logs_stream_and_tail(ray_start_with_dashboard):
    assert (
        wait_until_server_available(ray_start_with_dashboard.address_info["webui_url"])
        is True
    )
    webui_url = ray_start_with_dashboard.address_info["webui_url"]
    webui_url = format_web_url(webui_url)
    node_id = list_nodes()[0]["node_id"]

    def verify_basic():
        stream_response = requests.get(
            webui_url
            + f"/api/v0/logs/file?node_id={node_id}&filename=gcs_server.out&lines=5",
            stream=True,
        )
        if stream_response.status_code != 200:
            raise ValueError(stream_response.content.decode("utf-8"))
        lines = []
        for line in stream_response.iter_lines():
            lines.append(line.decode("utf-8"))
        assert len(lines) == 5 or len(lines) == 6
        return True

    wait_for_condition(verify_basic)

    @ray.remote
    class Actor:
        def write_log(self, strings):
            for s in strings:
                print(s)

        def getpid(self):
            return os.getpid()

    test_log_text = "test_log_text_日志_{}"
    actor = Actor.remote()
    ray.get(actor.write_log.remote([test_log_text.format("XXXXXX")]))

    # Test stream and fetching by actor id
    stream_response = requests.get(
        webui_url
        + "/api/v0/logs/stream?&lines=-1"
        + f"&actor_id={actor._ray_actor_id.hex()}",
        stream=True,
    )
    if stream_response.status_code != 200:
        raise ValueError(stream_response.content.decode("utf-8"))
    stream_iterator = stream_response.iter_content(chunk_size=None)
    actual_output = next(stream_iterator).decode("utf-8")
    assert "actor_name:Actor\n" in actual_output
    assert test_log_text.format("XXXXXX") in actual_output

    streamed_string = ""
    for i in range(5):
        strings = []
        for j in range(3):
            strings.append(test_log_text.format(f"{3*i + j:06d}"))

        ray.get(actor.write_log.remote(strings))

        string = ""
        for s in strings:
            string += s + "\n"
        streamed_string += string
        # NOTE: Prefix 1 indicates the stream has succeeded.
        assert string in next(stream_iterator).decode("utf-8")
    del stream_response

    # Test tailing log by actor id
    LINES = 150
    file_response = requests.get(
        webui_url
        + f"/api/v0/logs/file?&lines={LINES}"
        + "&actor_id="
        + actor._ray_actor_id.hex(),
    ).content.decode("utf-8")
    # NOTE: Prefix 1 indicates the stream has succeeded.
    for line in streamed_string.split("\n")[-(LINES + 1) :]:
        assert line in file_response

    # Test query by pid & node_ip instead of actor id.
    node_ip = list(ray.nodes())[0]["NodeManagerAddress"]
    pid = ray.get(actor.getpid.remote())
    file_response = requests.get(
        webui_url
        + f"/api/v0/logs/file?node_ip={node_ip}&lines={LINES}"
        + f"&pid={pid}",
    ).content.decode("utf-8")
    # NOTE: Prefix 1 indicates the stream has succeeded.
    for line in streamed_string.split("\n")[-(LINES + 1) :]:
        assert line in file_response


def test_log_list(ray_start_cluster):
    cluster = ray_start_cluster
    num_nodes = 5
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)

    def verify():
        for node in list_nodes():
            # When glob filter is not provided, it should provide all logs
            logs = list_logs(node_id=node["node_id"])
            assert "raylet" in logs
            assert "gcs_server" in logs
            assert "dashboard" in logs
            assert "agent" in logs
            assert "internal" in logs
            assert "driver" in logs
            assert "autoscaler" in logs

            # Test glob works.
            logs = list_logs(node_id=node["node_id"], glob_filter="raylet*")
            assert len(logs) == 1
            return True

    wait_for_condition(verify)

    node_id = "XXXX"
    with pytest.raises(requests.HTTPError) as e:
        list_logs(node_id=node_id)

    e.match(f"Given node id {node_id} is not available")


@pytest.mark.skipif(
    sys.platform == "win32", reason="Job submission is failing on windows."
)
def test_log_job(ray_start_with_dashboard):
    assert wait_until_server_available(ray_start_with_dashboard["webui_url"]) is True
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)
    node_id = list_nodes()[0]["node_id"]

    # Submit a job
    from ray.job_submission import JobSubmissionClient

    JOB_LOG = "test-job-log"
    client = JobSubmissionClient(webui_url)
    entrypoint = f"python -c \"print('{JOB_LOG}')\""
    job_id = client.submit_job(entrypoint=entrypoint)

    def job_done():
        jobs = list_jobs(filters=[("submission_id", "=", job_id)])
        assert len(jobs) == 1
        assert jobs[0].status == "SUCCEEDED"
        return True

    wait_for_condition(job_done)

    def verify():
        logs = "".join(get_log(submission_id=job_id, node_id=node_id))
        assert JOB_LOG + "\n" == logs

        return True

    wait_for_condition(verify)


def test_log_get_invalid_filenames(ray_start_with_dashboard, temp_file):
    assert (
        wait_until_server_available(ray_start_with_dashboard.address_info["webui_url"])
        is True
    )
    webui_url = ray_start_with_dashboard.address_info["webui_url"]
    webui_url = format_web_url(webui_url)
    node_id = list_nodes()[0]["node_id"]

    # log_dir = ray._private.worker.global_worker.node.get_logs_dir_path()

    def verify():
        # Kind of hack that we know the file node_ip_address.json exists in ray.
        with pytest.raises(RayStateApiException) as e:
            logs = "".join(get_log(node_id=node_id, filename="../node_ip_address.json"))
            print(logs)
            assert "does not start with " in str(e.value)
        return True

    wait_for_condition(verify)

    # Verify that reading file outside of the log directory is not allowed
    # with absolute path.
    def verify():
        # Kind of hack that we know the file node_ip_address.json exists in ray.
        temp_file_abs_path = str(Path(temp_file.name).resolve())
        with pytest.raises(RayStateApiException) as e:
            logs = "".join(get_log(node_id=node_id, filename=temp_file_abs_path))
            print(logs)
            assert "does not start with " in str(e.value)
        return True

    wait_for_condition(verify)


def test_log_get_subdir(ray_start_with_dashboard):
    assert (
        wait_until_server_available(ray_start_with_dashboard.address_info["webui_url"])
        is True
    )
    webui_url = ray_start_with_dashboard.address_info["webui_url"]
    webui_url = format_web_url(webui_url)
    node_id = list_nodes()[0]["node_id"]

    log_dir = ray._private.worker.global_worker.node.get_logs_dir_path()
    subdir = "test_subdir"
    file = "test_#file.log"
    path = Path(log_dir) / subdir / file
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("test log")

    # HTTP endpoint
    def verify():
        # Direct logs stream
        response = requests.get(
            webui_url
            + f"/api/v0/logs/file?node_id={node_id}"
            + f"&filename={urllib.parse.quote('test_subdir/test_#file.log')}"
        )
        assert response.status_code == 200, response.reason
        assert "test log" in response.text
        return True

    wait_for_condition(verify)

    # get log SDK
    def verify():
        logs = "".join(get_log(node_id=node_id, filename="test_subdir/test_#file.log"))
        assert "test log" in logs
        return True

    wait_for_condition(verify)


def test_log_get(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)
    head_node = list_nodes()[0]
    cluster.add_node(num_cpus=1)

    @ray.remote(num_cpus=1)
    class Actor:
        def print(self, i):
            for _ in range(i):
                print("1")

        def getpid(self):
            import os

            return os.getpid()

    """
    Test filename match
    """

    def verify():
        # By default, node id should be configured to the head node.
        for log in get_log(
            node_id=head_node["node_id"], filename="raylet.out", tail=10
        ):
            # + 1 since the last line is just empty.
            assert len(log.split("\n")) == 11
        return True

    wait_for_condition(verify)

    """
    Test worker pid / IP match
    """
    a = Actor.remote()
    pid = ray.get(a.getpid.remote())
    ray.get(a.print.remote(20))

    def verify():
        # By default, node id should be configured to the head node.
        for log in get_log(node_ip=head_node["node_ip"], pid=pid, tail=10):
            # + 1 since the last line is just empty.
            assert len(log.split("\n")) == 11
        return True

    wait_for_condition(verify)

    """
    Test actor logs.
    """
    actor_id = a._actor_id.hex()

    def verify():
        # By default, node id should be configured to the head node.
        for log in get_log(actor_id=actor_id, tail=10):
            # + 1 since the last line is just empty.
            assert len(log.split("\n")) == 11
        return True

    wait_for_condition(verify)

    del a
    """
    Test log suffix selection for worker/actor
    """
    ACTOR_LOG_LINE = "{dest}:test actor log"

    @ray.remote
    class Actor:
        def __init__(self):
            import sys

            print(ACTOR_LOG_LINE.format(dest="out"))
            print(ACTOR_LOG_LINE.format(dest="err"), file=sys.stderr)

    actor = Actor.remote()
    actor_id = actor._actor_id.hex()

    WORKER_LOG_LINE = "{dest}:test worker log"

    @ray.remote
    def worker_func():
        import os
        import sys

        print(WORKER_LOG_LINE.format(dest="out"))
        print(WORKER_LOG_LINE.format(dest="err"), file=sys.stderr)
        return os.getpid()

    pid = ray.get(worker_func.remote())

    def verify():
        # Test actors
        lines = get_log(actor_id=actor_id, suffix="err")
        assert ACTOR_LOG_LINE.format(dest="err") in "".join(lines)

        lines = get_log(actor_id=actor_id, suffix="out")
        assert ACTOR_LOG_LINE.format(dest="out") in "".join(lines)

        # Default to out
        lines = get_log(actor_id=actor_id)
        assert ACTOR_LOG_LINE.format(dest="out") in "".join(lines)

        # Test workers
        lines = get_log(node_ip=head_node["node_ip"], pid=pid, suffix="err")
        assert WORKER_LOG_LINE.format(dest="err") in "".join(lines)

        lines = get_log(node_ip=head_node["node_ip"], pid=pid, suffix="out")
        assert WORKER_LOG_LINE.format(dest="out") in "".join(lines)

        lines = get_log(node_ip=head_node["node_ip"], pid=pid)
        assert WORKER_LOG_LINE.format(dest="out") in "".join(lines)

        return True

    wait_for_condition(verify)

    def verify():
        runner = CliRunner()
        result = runner.invoke(
            logs_state_cli_group,
            ["actor", "--id", actor_id],
        )
        assert result.exit_code == 0, result.exception
        assert ACTOR_LOG_LINE.format(dest="out") in result.output

        result = runner.invoke(
            logs_state_cli_group,
            [
                "actor",
                "--id",
                actor_id,
                "--err",
            ],
        )
        assert result.exit_code == 0, result.exception
        assert ACTOR_LOG_LINE.format(dest="err") in result.output
        return True

    wait_for_condition(verify)
    ##############################
    # Test binary files and encodings.
    ##############################
    # Write a binary file to ray log directory.
    log_dir = ray._private.worker.global_worker.node.get_logs_dir_path()
    file = "test.bin"
    binary_file = os.path.join(log_dir, file)
    with open(binary_file, "wb") as f:
        data = bytearray(i for i in range(256))
        f.write(data)

    # Get the log
    def verify():
        for read in get_log(node_ip=head_node["node_ip"], filename=file, encoding=None):
            assert read == data

        # Default utf-8
        for read in get_log(
            node_ip=head_node["node_ip"], filename=file, errors="replace"
        ):
            assert read == data.decode(encoding="utf-8", errors="replace")

        for read in get_log(
            node_ip=head_node["node_ip"],
            filename=file,
            encoding="iso-8859-1",
            errors="replace",
        ):
            assert read == data.decode(encoding="iso-8859-1", errors="replace")

        return True

    wait_for_condition(verify)

    # Test running task logs
    @ray.remote
    def sleep_task(out_msg):
        print(out_msg, end="", file=sys.stdout)
        import time

        time.sleep(10)

    expected_out = "This is a test log from stdout\n"
    task = sleep_task.remote(expected_out)

    def verify():
        lines = get_log(task_id=task.task_id().hex())
        assert expected_out == "".join(lines)

        return True

    wait_for_condition(verify)


@pytest.mark.skipif(
    sys.platform == "win32", reason="Windows has logging race from tasks."
)
@skip_flaky_core_test_premerge("https://github.com/ray-project/ray/issues/40959")
def test_log_task(shutdown_only):
    from ray.runtime_env import RuntimeEnv

    ray.init()

    # Test get log by multiple task id
    @ray.remote
    def task_log():
        out_msg = "This is a test log from stdout\n"
        print(out_msg, end="", file=sys.stdout)
        err_msg = "THIS IS A TEST LOG FROM STDERR\n"
        print(err_msg, end="", file=sys.stderr)

        return out_msg, err_msg

    # Run some other tasks before and after to make sure task
    # log only outputs the task's log.
    ray.get(task_log.remote())
    task = task_log.remote()
    expected_out, expected_err = ray.get(task)
    ray.get(task_log.remote())

    def verify():
        lines = get_log(task_id=task.task_id().hex())
        assert expected_out in "".join(lines)

        # Test suffix
        lines = get_log(task_id=task.task_id().hex(), suffix="err")
        assert expected_err in "".join(lines)

        return True

    wait_for_condition(verify)

    enabled_actor_task_log_runtime_env = RuntimeEnv(
        env_vars={"RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING": "1"}
    )

    # Test actor task logs
    @ray.remote
    class Actor:
        def print_log(self, out_msg):
            for _ in range(3):
                print(out_msg, end="", file=sys.stdout)
                print(out_msg, end="", file=sys.stderr)

    a = Actor.options(runtime_env=enabled_actor_task_log_runtime_env).remote()
    out_msg = "This is a test log\n"
    t = a.print_log.remote(out_msg)
    ray.get(t)

    def verify():
        lines = get_log(task_id=t.task_id().hex())
        assert out_msg * 3 == "".join(lines)

        return True

    wait_for_condition(verify)

    # Test actor task logs with interleaving logs should raise
    # errors to ask users to user actor log instead.
    @ray.remote
    class AsyncActor:
        async def print_log(self, out_msg):
            for _ in range(3):
                print(out_msg, end="", file=sys.stdout)
                await asyncio.sleep(1)

    actor = AsyncActor.options(
        max_concurrency=2, runtime_env=enabled_actor_task_log_runtime_env
    ).remote()
    out_msg = "[{name}]: This is a test log from stdout\n"
    task_a = actor.print_log.remote(out_msg.format(name="a"))
    task_b = actor.print_log.remote(out_msg.format(name="b"))
    ray.get([task_a, task_b])

    def verify():
        lines = get_log(task_id=task_a.task_id().hex())
        assert "".join(lines).count(out_msg.format(name="a")) == 3
        return True

    wait_for_condition(verify)

    def verify_actor_task_error(task_id, actor_id):
        with pytest.raises(RayStateApiException) as e:
            for log in get_log(task_id=task_id):
                pass

        assert "For actor task, please query actor log" in str(e.value), str(e.value)
        assert f"ray logs actor --id {actor_id}" in str(e.value), str(e.value)

        return True

    # Getting task logs from actor with actor task log not enabled should raise errors.
    a = Actor.remote()
    t = a.print_log.remote(out_msg)
    ray.get(t)
    wait_for_condition(
        verify_actor_task_error, task_id=t.task_id().hex(), actor_id=a._actor_id.hex()
    )

    a = AsyncActor.options(max_concurrency=2).remote()
    t = a.print_log.remote(out_msg)
    ray.get(t)
    wait_for_condition(
        verify_actor_task_error, task_id=t.task_id().hex(), actor_id=a._actor_id.hex()
    )

    # Test task logs tail with lines.
    expected_out = [f"task-{i}\n" for i in range(5)]

    @ray.remote
    def f():
        print("".join(expected_out), end="", file=sys.stdout)

    t = f.remote()
    ray.get(t)

    def verify():
        lines = get_log(task_id=t.task_id().hex(), tail=2)
        actual_output = "".join(lines)
        assert actual_output == "".join(expected_out[-2:])
        return True

    wait_for_condition(verify)


def test_log_cli(shutdown_only):
    ray.init(num_cpus=1)
    runner = CliRunner()

    # Test the head node is chosen by default.
    def verify():
        result = runner.invoke(logs_state_cli_group, ["cluster"])
        assert result.exit_code == 0, result.exception
        assert "raylet.out" in result.output
        assert "raylet.err" in result.output
        assert "gcs_server.out" in result.output
        assert "gcs_server.err" in result.output
        return True

    wait_for_condition(verify)

    # Test when there's only 1 match, it prints logs.
    def verify():
        result = runner.invoke(logs_state_cli_group, ["cluster", "raylet.out"])
        assert result.exit_code == 0
        assert "raylet.out" not in result.output
        assert "raylet.err" not in result.output
        assert "gcs_server.out" not in result.output
        assert "gcs_server.err" not in result.output
        # Make sure it prints the log message.
        assert "NodeManager server started" in result.output
        return True

    wait_for_condition(verify)

    # Test when there's more than 1 match, it prints a list of logs.
    def verify():
        result = runner.invoke(logs_state_cli_group, ["cluster", "raylet.*"])
        assert result.exit_code == 0, result.exception
        assert "raylet.out" in result.output
        assert "raylet.err" in result.output
        assert "gcs_server.out" not in result.output
        assert "gcs_server.err" not in result.output
        return True

    wait_for_condition(verify)

    # Test actor log: `ray logs actor`
    ACTOR_LOG_LINE = "test actor log"

    @ray.remote
    class Actor:
        def __init__(self):
            print(ACTOR_LOG_LINE)

    actor = Actor.remote()
    actor_id = actor._actor_id.hex()

    def verify():
        result = runner.invoke(logs_state_cli_group, ["actor", "--id", actor_id])
        assert result.exit_code == 0, result.exception
        assert ACTOR_LOG_LINE in result.output
        return True

    wait_for_condition(verify)

    # Test worker log: `ray logs worker`
    WORKER_LOG_LINE = "test worker log"

    @ray.remote
    def worker_func():
        import os

        print(WORKER_LOG_LINE)
        return os.getpid()

    pid = ray.get(worker_func.remote())

    def verify():
        result = runner.invoke(logs_state_cli_group, ["worker", "--pid", pid])
        assert result.exit_code == 0, result.exception
        assert WORKER_LOG_LINE in result.output
        return True

    wait_for_condition(verify)

    # Test `ray logs raylet.*` forwarding to `ray logs cluster raylet.*`
    def verify():
        result = runner.invoke(logs_state_cli_group, ["raylet.*"])
        assert result.exit_code == 0, result.exception
        assert "raylet.out" in result.output
        assert "raylet.err" in result.output
        assert "gcs_server.out" not in result.output
        assert "gcs_server.err" not in result.output
        return True

    wait_for_condition(verify)

    # Test binary binary files and encodings.
    log_dir = ray._private.worker.global_worker.node.get_logs_dir_path()
    file = "test.bin"
    binary_file = os.path.join(log_dir, file)
    with open(binary_file, "wb") as f:
        data = bytearray(i for i in range(256))
        f.write(data)

    def verify():
        # Tailing with lines is not supported for binary files, thus the `tail=-1`
        result = runner.invoke(
            logs_state_cli_group,
            [
                file,
                "--encoding",
                "iso-8859-1",
                "--encoding-errors",
                "replace",
                "--tail",
                "-1",
            ],
        )
        assert result.exit_code == 0, result.exception
        assert result.output == data.decode(encoding="iso-8859-1", errors="replace")
        return True

    wait_for_condition(verify)


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
