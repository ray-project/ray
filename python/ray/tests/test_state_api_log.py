import json
import os
import sys
from typing import List
from unittest.mock import MagicMock

import pytest
from ray.experimental.state.state_cli import logs_state_cli_group
import requests
from click.testing import CliRunner

import ray
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
from ray.dashboard.modules.log.log_agent import tail as tail_file
from ray.dashboard.modules.log.log_manager import LogsManager
from ray.dashboard.tests.conftest import *  # noqa
from ray.experimental.state.api import get_log, list_logs, list_nodes, list_workers
from ray.experimental.state.common import GetLogOptions
from ray.experimental.state.exception import DataSourceUnavailable
from ray.experimental.state.state_manager import StateDataSourceClient

if sys.version_info >= (3, 8, 0):
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


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_logs_tail():
    """
    Unit test for tail
    """
    TOTAL_LINES = 1000
    FILE_NAME = "test_file.txt"
    try:
        with open(FILE_NAME, "w") as f:
            for i in range(TOTAL_LINES):
                # Check this works with unicode
                f.write(f"Message 日志 {i:4}\n")
        file = open(FILE_NAME, "rb")
        text, byte_pos = tail_file(file, 100)
        assert byte_pos == TOTAL_LINES * len(
            "Message 日志 1000\n".encode(encoding="utf-8")
        )
        lines = text.decode("utf-8").split("\n")
        assert len(lines) == 100
        assert lines[0] == "Message 日志  900"
        assert lines[99] == "Message 日志  999"
    except Exception as e:
        raise e
    finally:
        if os.path.exists(FILE_NAME):
            os.remove(FILE_NAME)


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
        "worker_out": [f"worker-{worker_id.hex()}-123-123.out"],
        "worker_err": [],
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
        logs_manager.list_logs.return_value = {
            "worker_out": ["worker-123-123-123.out"],
            "worker_err": [],
        }
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
    logs_manager.list_logs.return_value = {
        "worker_out": [f"worker-123-123-{pid}.out"],
        "worker_err": [],
    }
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

    """
    Test suffix is specified
    """
    pid = 123
    logs_manager.list_logs = AsyncMock()
    logs_manager.list_logs.return_value = {
        "worker_out": [f"worker-123-123-{pid}.out"],
        "worker_err": [],
    }
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

    logs_manager.list_logs.return_value = {
        "worker_out": [],
        "worker_err": [f"worker-123-123-{pid}.err"],
    }
    log_file_name, n = await logs_manager.resolve_filename(
        node_id=node_id.hex(),
        log_filename=None,
        actor_id=None,
        task_id=None,
        pid=pid,
        get_actor_fn=lambda _: generate_actor_data(actor_id, node_id, worker_id),
        timeout=10,
        suffix="err",
    )
    logs_manager.list_logs.assert_awaited_with(
        node_id.hex(), 10, glob_filter=f"*{pid}*err"
    )
    assert log_file_name == f"worker-123-123-{pid}.err"


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


def test_log_cli(shutdown_only):
    ray.init(num_cpus=1)
    runner = CliRunner()

    # Test the head node is chosen by default.
    def verify():
        result = runner.invoke(logs_state_cli_group, ["cluster"])
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
        result = runner.invoke(logs_state_cli_group, ["cluster", "raylet.out"])
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
        result = runner.invoke(logs_state_cli_group, ["cluster", "raylet.*"])
        assert result.exit_code == 0
        print(result.output)
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
        assert result.exit_code == 0
        print(result.output)
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
        assert result.exit_code == 0
        print(result.output)
        assert WORKER_LOG_LINE in result.output
        return True

    wait_for_condition(verify)

    # Test `ray logs raylet.*` forwarding to `ray logs cluster raylet.*`
    def verify():
        result = runner.invoke(logs_state_cli_group, ["raylet.*"])
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
