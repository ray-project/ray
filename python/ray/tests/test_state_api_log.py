import json
import os
import sys
from typing import List
from unittest.mock import MagicMock

import pytest
import requests
from click.testing import CliRunner
import grpc

import ray
import ray.scripts.scripts as scripts
from ray._private.test_utils import (
    format_web_url,
    wait_for_condition,
    wait_until_server_available,
)
from ray._raylet import ActorID, NodeID, TaskID, WorkerID
from ray.core.generated.common_pb2 import Address
from ray.core.generated.gcs_pb2 import ActorTableData
from ray.core.generated.reporter_pb2 import ListLogsReply, StreamLogReply
from ray.dashboard.modules.actor.actor_head import actor_table_data_to_dict
from ray.dashboard.modules.log.log_agent import _find_tail_start_from_last_lines
from ray.dashboard.modules.log.log_agent import _stream_log_in_chunk

from ray.dashboard.modules.log.log_manager import LogsManager
from ray.dashboard.tests.conftest import *  # noqa
from ray.experimental.state.api import get_log, list_logs, list_nodes, list_workers
from ray.experimental.state.common import GetLogOptions
from ray.experimental.state.exception import DataSourceUnavailable
from ray.experimental.state.state_manager import StateDataSourceClient

if sys.version_info > (3, 7, 0):
    from unittest.mock import AsyncMock
else:
    from asyncmock import AsyncMock


ASYNCMOCK_MIN_PYTHON_VER = (3, 8)


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


def _write_lines_and_get_offset_at_index(
    f, num_lines, line_index_with_offset, start_offset=0, trailing_new_line=True
):
    """
    Write multiple lines into a file, and record offsets

    Args:
        f: a binary file object that's writable
        num_lines: Number of lines to write
        line_index_with_offset: The index of the lines for which
            the start offset of the line should be recorded an returned.
        start_offset: The offset to start writing
        trailing_new_line: True if a '\n' is added at the end of the
            lines.

    Return:
        offset_at_line: The file offset of the start of the line at index
            `line_index_with_offset`
        offset_end: The offset of the end of file
    """
    assert line_index_with_offset <= num_lines
    f.seek(start_offset, 0)

    nwrite = 0
    offset_at_line = -1
    for i in range(num_lines):
        if i == line_index_with_offset:
            offset_at_line = nwrite

        if i == num_lines - 1 and not trailing_new_line:
            # Last line no newline
            line = f"{i}-test-line"
        else:
            line = f"{i}-test-line\n"
        nwrite += f.write(line.encode("utf-8"))

    f.flush()
    f.seek(0, 2)
    offset_end = f.tell()

    # Marking offset past the last line.
    if line_index_with_offset == num_lines:
        offset_at_line = offset_end
    return offset_at_line, offset_end


@pytest.mark.parametrize(
    "lines_to_tail",
    [0, 1, 10, 100, 1000],
)
@pytest.mark.parametrize(
    "block_size",
    [4, 8, 16, 32, 512, 1024, 1 << 16],
)
@pytest.mark.parametrize(
    "total_lines",
    [1000],
)
def test_find_tail_start_from_last_lines(
    lines_to_tail, block_size, total_lines, temp_file
):
    """Test getting correct offsets with trailing lines count"""
    expect_offset, end = _write_lines_and_get_offset_at_index(
        temp_file, total_lines, total_lines - lines_to_tail
    )
    with open(temp_file.name, "rb") as test_file:

        start_offset, end_offset = _find_tail_start_from_last_lines(
            test_file, lines_to_tail=lines_to_tail, block_size=block_size
        )

        assert (
            start_offset == expect_offset
        ), "Non-matching offset for finding the tail start pos"

        assert end_offset == end, "Non-matching offset for file end"


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
    with open(random_ascii_file.name, "rb") as test_file:
        context = MagicMock(grpc.aio.ServicerContext)
        context.done.return_value = False

        expected_file_content = _read_file(test_file, start_offset, end_offset)
        actual_log_content = await _stream_log(
            context, test_file, start_offset, end_offset
        )

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
        total_lines - lines_to_tail,
        trailing_new_line=trailing_new_line,
    )

    with open(temp_file.name, "rb") as test_file:
        context = MagicMock(grpc.aio.ServicerContext)
        context.done.return_value = False

        start_offset, end_offset = _find_tail_start_from_last_lines(
            test_file, lines_to_tail=lines_to_tail
        )

        actual_data = await _stream_log(context, test_file, start_offset, end_offset)
        expected_data = _read_file(test_file, start_offset, end_offset)

        assert actual_data == expected_data, "Non-matching data from stream log"

        all_lines = actual_data.decode("utf-8")
        assert all_lines.count("\n") == (
            lines_to_tail
            if trailing_new_line or lines_to_tail == 0
            else lines_to_tail - 1
        ), "Non-matching number of lines tailed"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "lines_to_tail,total_lines",
    [(0, 100), (100, 100), (10, 100), (1, 100), (99, 100)],
)
async def test_log_tails_with_appends(lines_to_tail, total_lines, temp_file):
    """Test tailing a log file that grows at the same time"""
    _write_lines_and_get_offset_at_index(
        temp_file, total_lines, total_lines - lines_to_tail
    )

    with open(temp_file.name, "rb") as test_file:
        context = MagicMock(grpc.aio.ServicerContext)
        context.done.return_value = False

        start_offset, end_offset = _find_tail_start_from_last_lines(
            test_file, lines_to_tail=lines_to_tail
        )

        # Modify the file with append here
        expected_data = _read_file(test_file, start_offset, end_offset)
        num_new_lines = 100
        _, new_end_offset = _write_lines_and_get_offset_at_index(
            temp_file, num_new_lines, -1, start_offset=end_offset
        )

        actual_data = await _stream_log(context, test_file, start_offset, end_offset)

        assert actual_data == expected_data, "Non-matching data from stream log"

        all_lines = actual_data.decode("utf-8")
        assert (
            all_lines.count("\n") == lines_to_tail
        ), "Non-matching number of lines tailed"

        # Tail again should read the new lines written
        start_offset, end_offset = _find_tail_start_from_last_lines(
            test_file, lines_to_tail=lines_to_tail + num_new_lines
        )

        assert (
            end_offset == new_end_offset
        ), "Non-matching end offset found after append"
        expected_data = _read_file(test_file, start_offset, new_end_offset)
        actual_data = await _stream_log(context, test_file, start_offset, end_offset)

        assert (
            actual_data == expected_data
        ), "Non-matching data from stream log after append"

        all_lines = actual_data.decode("utf-8")
        assert (
            all_lines.count("\n") == lines_to_tail + num_new_lines
        ), "Non-matching number of lines tailed after append"


# Unit Tests (LogsManager)


@pytest.fixture
def logs_manager():
    if sys.version_info < ASYNCMOCK_MIN_PYTHON_VER:
        raise Exception(f"Unsupported for this version of python {sys.version_info}")
    from unittest.mock import AsyncMock

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


@pytest.mark.skipif(
    sys.version_info < ASYNCMOCK_MIN_PYTHON_VER,
    reason=f"unittest.mock.AsyncMock requires python {ASYNCMOCK_MIN_PYTHON_VER}"
    " or higher",
)
@pytest.mark.asyncio
async def test_logs_manager_list_logs(logs_manager):
    logs_client = logs_manager.data_source_client

    logs_client.get_all_registered_agent_ids = MagicMock()
    logs_client.get_all_registered_agent_ids.return_value = ["1", "2"]

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
    logs_client.get_all_registered_agent_ids.assert_called()
    logs_client.list_logs.assert_awaited_with("2", "*gcs*", timeout=30)

    # The second call raises DataSourceUnavailable, which will
    # return DataSourceUnavailable to the caller.
    with pytest.raises(DataSourceUnavailable):
        result = await logs_manager.list_logs(
            node_id="1", timeout=30, glob_filter="*gcs*"
        )


@pytest.mark.skipif(
    sys.version_info < ASYNCMOCK_MIN_PYTHON_VER,
    reason=f"unittest.mock.AsyncMock requires python {ASYNCMOCK_MIN_PYTHON_VER}"
    " or higher",
)
@pytest.mark.asyncio
async def test_logs_manager_resolve_file(logs_manager):
    node_id = NodeID(b"1" * 28)
    """
    Test filename is given.
    """
    logs_client = logs_manager.data_source_client
    logs_client.get_all_registered_agent_ids = MagicMock()
    logs_client.get_all_registered_agent_ids.return_value = [node_id.hex()]
    expected_filename = "filename"
    log_file_name, n = await logs_manager.resolve_filename(
        node_id=node_id,
        log_filename=expected_filename,
        actor_id=None,
        task_id=None,
        pid=None,
        get_actor_fn=lambda _: True,
        timeout=10,
    )
    assert log_file_name == expected_filename
    assert n == node_id
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

        log_file_name, n = await logs_manager.resolve_filename(
            node_id=node_id,
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
        log_file_name, n = await logs_manager.resolve_filename(
            node_id=node_id,
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
        "worker_out": [f"worker-{worker_id.hex()}-123-123.out"]
    }
    log_file_name, n = await logs_manager.resolve_filename(
        node_id=node_id.hex(),
        log_filename=None,
        actor_id=actor_id,
        task_id=None,
        pid=None,
        get_actor_fn=lambda _: generate_actor_data(actor_id, node_id, worker_id),
        timeout=10,
    )
    logs_manager.list_logs.assert_awaited_with(
        node_id.hex(), 10, glob_filter=f"*{worker_id.hex()}*"
    )
    assert log_file_name == f"worker-{worker_id.hex()}-123-123.out"
    assert n == node_id.hex()

    """
    Test task id is given.
    """
    with pytest.raises(NotImplementedError):
        task_id = TaskID(b"2" * 24)
        log_file_name, n = await logs_manager.resolve_filename(
            node_id=node_id.hex(),
            log_filename=None,
            actor_id=None,
            task_id=task_id,
            pid=None,
            get_actor_fn=lambda _: generate_actor_data(actor_id, node_id, worker_id),
            timeout=10,
        )

    """
    Test pid is given.
    """
    # Pid doesn't exist.
    with pytest.raises(FileNotFoundError):
        pid = 456
        logs_manager.list_logs = AsyncMock()
        # Provide the wrong pid.
        logs_manager.list_logs.return_value = {"worker_out": ["worker-123-123-123.out"]}
        log_file_name = await logs_manager.resolve_filename(
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
    logs_manager.list_logs.return_value = {"worker_out": [f"worker-123-123-{pid}.out"]}
    log_file_name, n = await logs_manager.resolve_filename(
        node_id=node_id.hex(),
        log_filename=None,
        actor_id=None,
        task_id=None,
        pid=pid,
        get_actor_fn=lambda _: generate_actor_data(actor_id, node_id, worker_id),
        timeout=10,
    )
    logs_manager.list_logs.assert_awaited_with(
        node_id.hex(), 10, glob_filter=f"*{pid}*"
    )
    assert log_file_name == f"worker-123-123-{pid}.out"

    """
    Test nothing is given.
    """
    with pytest.raises(FileNotFoundError):
        log_file_name = await logs_manager.resolve_filename(
            node_id=node_id.hex(),
            log_filename=None,
            actor_id=None,
            task_id=None,
            pid=None,
            get_actor_fn=lambda _: generate_actor_data(actor_id, node_id, worker_id),
            timeout=10,
        )


@pytest.mark.skipif(
    sys.version_info < ASYNCMOCK_MIN_PYTHON_VER,
    reason=f"unittest.mock.AsyncMock requires python {ASYNCMOCK_MIN_PYTHON_VER}"
    " or higher",
)
@pytest.mark.asyncio
async def test_logs_manager_stream_log(logs_manager):
    NUM_LOG_CHUNKS = 10
    logs_client = logs_manager.data_source_client

    logs_client.get_all_registered_agent_ids = MagicMock()
    logs_client.get_all_registered_agent_ids.return_value = ["1", "2"]
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
    )

    # Currently cannot test actor_id with AsyncMock.
    # It will be tested by the integration test.


@pytest.mark.skipif(
    sys.version_info < ASYNCMOCK_MIN_PYTHON_VER,
    reason=f"unittest.mock.AsyncMock requires python {ASYNCMOCK_MIN_PYTHON_VER}"
    " or higher",
)
@pytest.mark.asyncio
async def test_logs_manager_keepalive_no_timeout(logs_manager):
    """Test when --follow is specified, there's no timeout.

    Related: https://github.com/ray-project/ray/issues/25721
    """
    NUM_LOG_CHUNKS = 10
    logs_client = logs_manager.data_source_client

    logs_client.get_all_registered_agent_ids = MagicMock()
    logs_client.get_all_registered_agent_ids.return_value = ["1", "2"]
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
        return len(lines) == 5 or len(lines) == 6

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
        + "/api/v0/logs/stream?&lines=2"
        + f"&actor_id={actor._ray_actor_id.hex()}",
        stream=True,
    )
    if stream_response.status_code != 200:
        raise ValueError(stream_response.content.decode("utf-8"))
    stream_iterator = stream_response.iter_content(chunk_size=None)
    # NOTE: Prefix 1 indicates the stream has succeeded.
    assert (
        next(stream_iterator).decode("utf-8")
        == "1:actor_name:Actor\n" + test_log_text.format("XXXXXX") + "\n"
    )

    streamed_string = ""
    for i in range(5):
        strings = []
        for j in range(100):
            strings.append(test_log_text.format(f"{100*i + j:06d}"))

        ray.get(actor.write_log.remote(strings))

        string = ""
        for s in strings:
            string += s + "\n"
        streamed_string += string
        # NOTE: Prefix 1 indicates the stream has succeeded.
        assert next(stream_iterator).decode("utf-8") == "1" + string
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
    assert file_response == "1" + "\n".join(streamed_string.split("\n")[-(LINES + 1) :])

    # Test query by pid & node_ip instead of actor id.
    node_ip = list(ray.nodes())[0]["NodeManagerAddress"]
    pid = ray.get(actor.getpid.remote())
    file_response = requests.get(
        webui_url
        + f"/api/v0/logs/file?node_ip={node_ip}&lines={LINES}"
        + f"&pid={pid}",
    ).content.decode("utf-8")
    # NOTE: Prefix 1 indicates the stream has succeeded.
    assert file_response == "1" + "\n".join(streamed_string.split("\n")[-(LINES + 1) :])


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

    with pytest.raises(NotImplementedError):
        for _ in get_log(task_id=123, tail=10):
            pass


def test_log_cli(shutdown_only):
    ray.init(num_cpus=1)
    runner = CliRunner()

    # Test the head node is chosen by default.
    def verify():
        result = runner.invoke(scripts.ray_logs)
        print(result.output)
        assert result.exit_code == 0
        assert "raylet.out" in result.output
        assert "raylet.err" in result.output
        assert "gcs_server.out" in result.output
        assert "gcs_server.err" in result.output
        return True

    wait_for_condition(verify)

    # Test when there's only 1 match, it prints logs.
    def verify():
        result = runner.invoke(scripts.ray_logs, ["raylet.out"])
        assert result.exit_code == 0
        print(result.output)
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
        result = runner.invoke(scripts.ray_logs, ["raylet.*"])
        assert result.exit_code == 0
        print(result.output)
        assert "raylet.out" in result.output
        assert "raylet.err" in result.output
        assert "gcs_server.out" not in result.output
        assert "gcs_server.err" not in result.output
        return True

    wait_for_condition(verify)


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
