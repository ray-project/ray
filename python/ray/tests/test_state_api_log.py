import json
import os
import requests

from unittest.mock import MagicMock
from typing import List
import pytest
import sys

if sys.version_info > (3, 7, 0):
    from unittest.mock import AsyncMock
else:
    from asyncmock import AsyncMock

import ray

from ray._private.test_utils import (
    format_web_url,
    wait_until_server_available,
)
from ray._raylet import WorkerID, ActorID, TaskID, NodeID
from ray.dashboard.modules.actor.actor_head import actor_table_data_to_dict
from ray.dashboard.tests.conftest import *  # noqa
from ray.core.generated.common_pb2 import Address
from ray.core.generated.reporter_pb2 import StreamLogReply, ListLogsReply
from ray.core.generated.gcs_pb2 import ActorTableData
from ray.dashboard.modules.log.log_agent import tail as tail_file
from ray.dashboard.modules.log.log_manager import LogsManager
from ray.experimental.state.api import list_nodes, list_workers
from ray.experimental.state.common import GetLogOptions
from ray.experimental.state.exception import DataSourceUnavailable
from ray.experimental.state.state_manager import StateDataSourceClient
from ray._private.test_utils import wait_for_condition

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
    expected_filename = "filename"
    log_file_name = await logs_manager.resolve_filename(
        node_id=node_id,
        log_filename=expected_filename,
        actor_id=None,
        task_id=None,
        pid=None,
        get_actor_fn=lambda _: True,
        timeout=10,
    )
    assert log_file_name == expected_filename
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

        log_file_name = await logs_manager.resolve_filename(
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
        log_file_name = await logs_manager.resolve_filename(
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
    log_file_name = await logs_manager.resolve_filename(
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

    """
    Test task id is given.
    """
    with pytest.raises(NotImplementedError):
        task_id = TaskID(b"2" * 24)
        log_file_name = await logs_manager.resolve_filename(
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
    log_file_name = await logs_manager.resolve_filename(
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
        timeout=30,
    )

    # Currently cannot test actor_id with AsyncMock.
    # It will be tested by the integration test.


# Integration tests


def test_logs_list(ray_start_with_dashboard):
    assert wait_until_server_available(ray_start_with_dashboard["webui_url"]) is True
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)
    node_id = list(list_nodes().keys())[0]

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
        assert len(logs.keys()) == 2
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
            list(
                filter(lambda w: w["worker_type"] == "WORKER", list_workers().values())
            )
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
    assert wait_until_server_available(ray_start_with_dashboard["webui_url"]) is True
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)
    node_id = list(list_nodes().keys())[0]

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

    # def verify_actor_stream_log():
    # Test stream and fetching by actor id
    stream_response = requests.get(
        webui_url
        + f"/api/v0/logs/stream?node_id={node_id}&lines=2"
        + f"&actor_id={actor._ray_actor_id.hex()}",
        stream=True,
    )
    if stream_response.status_code != 200:
        raise ValueError(stream_response.content.decode("utf-8"))
    stream_iterator = stream_response.iter_content(chunk_size=None)
    assert (
        next(stream_iterator).decode("utf-8")
        == ":actor_name:Actor\n" + test_log_text.format("XXXXXX") + "\n"
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
        assert next(stream_iterator).decode("utf-8") == string
    del stream_response

    # Test tailing log by actor id
    LINES = 150
    file_response = requests.get(
        webui_url
        + f"/api/v0/logs/file?node_id={node_id}&lines={LINES}"
        + "&actor_id="
        + actor._ray_actor_id.hex(),
    ).content.decode("utf-8")
    assert file_response == "\n".join(streamed_string.split("\n")[-(LINES + 1) :])

    # Test query by pid & node_ip instead of actor id.
    node_ip = list(ray.nodes())[0]["NodeManagerAddress"]
    pid = ray.get(actor.getpid.remote())
    file_response = requests.get(
        webui_url
        + f"/api/v0/logs/file?node_ip={node_ip}&lines={LINES}"
        + f"&pid={pid}",
    ).content.decode("utf-8")
    assert file_response == "\n".join(streamed_string.split("\n")[-(LINES + 1) :])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
