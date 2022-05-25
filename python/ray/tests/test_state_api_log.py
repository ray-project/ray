import json
import os
import requests
import asyncio
import time

from unittest.mock import MagicMock
from typing import List
import pytest
import sys

import ray

from ray._private.test_utils import (
    format_web_url,
    wait_until_server_available,
)
from ray.dashboard.tests.conftest import *  # noqa
from ray.core.generated.reporter_pb2 import StreamLogReply, ListLogsReply
from ray.dashboard.modules.log.log_agent import tail as tail_file
from ray.dashboard.modules.log.log_manager import LogsManager
from ray.experimental.log.common import (
    LogStreamOptions,
    LogIdentifiers,
    FileIdentifiers,
)
from ray.experimental.state.state_manager import StateDataSourceClient

ASYNCMOCK_MIN_PYTHON_VER = (3, 8)

# Unit Tests (LogAgentV1)


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


async def wait_for_500_ms():
    await asyncio.sleep(0.5)


async def raise_timeout():
    raise ValueError("timed out")


@pytest.mark.skipif(
    sys.version_info < ASYNCMOCK_MIN_PYTHON_VER,
    reason=f"unittest.mock.AsyncMock requires python {ASYNCMOCK_MIN_PYTHON_VER}"
    " or higher",
)
@pytest.mark.asyncio
async def test_logs_manager_list_logs(logs_manager):
    logs_client = logs_manager.logs_client

    logs_client.get_all_registered_nodes = MagicMock()
    logs_client.get_all_registered_nodes.return_value = ["1", "2"]

    logs_client.list_logs.side_effect = [
        generate_list_logs(["raylet.out", "gcs_server.out"]),
        generate_list_logs(["raylet.out", "gcs_server.out"]),
    ]
    result = await logs_manager.list_logs(node_id="2", filters=["gcs"])
    assert len(result) == 1
    node_2_result = result["2"]
    assert node_2_result["gcs_server"] == ["gcs_server.out"]
    assert node_2_result["raylet"] == []
    logs_client.get_all_registered_nodes.assert_called_once()
    logs_client.list_logs.assert_awaited_with("2", timeout=None)


@pytest.mark.skipif(
    sys.version_info < ASYNCMOCK_MIN_PYTHON_VER,
    reason=f"unittest.mock.AsyncMock requires python {ASYNCMOCK_MIN_PYTHON_VER}"
    " or higher",
)
@pytest.mark.asyncio
async def test_logs_manager_stream_log(logs_manager):
    NUM_LOG_CHUNKS = 10
    logs_client = logs_manager.client

    logs_client.get_all_registered_nodes = MagicMock()
    logs_client.get_all_registered_nodes.return_value = ["1", "2"]
    logs_client.ip_to_node_id = MagicMock()
    logs_client.stream_log.return_value = generate_logs_stream(NUM_LOG_CHUNKS)

    # Test file_name, media_type="file", node_id
    stream_options = LogStreamOptions(media_type="file", lines=10, interval=0.5)
    node_identifiers = NodeIdentifiers(node_id="1")
    file_identifiers = FileIdentifiers(log_file_name="raylet.out")
    identifiers = LogIdentifiers(file=file_identifiers, node=node_identifiers)

    stream = await logs_manager.create_log_stream(
        identifiers=identifiers, stream_options=stream_options
    )
    i = 0
    async for chunk in stream:
        assert chunk.data.decode("utf-8") == generate_logs_stream_chunk(index=i)
        i += 1
    assert i == NUM_LOG_CHUNKS
    logs_client.stream_log.assert_awaited_with(
        node_id="1",
        log_file_name="raylet.out",
        keep_alive=False,
        lines=10,
        interval=0.5,
    )

    # Test pid, media_type = "stream", node_ip

    logs_client.ip_to_node_id.return_value = "1"
    logs_client.list_logs.side_effect = [
        generate_list_logs(["raylet.out", "gcs_server.out", "worker-0-0-10.out"]),
    ]

    stream_options = LogStreamOptions(media_type="stream", lines=100, interval=0.5)
    node_identifiers = NodeIdentifiers(node_ip="1")
    file_identifiers = FileIdentifiers(pid="10")
    identifiers = LogIdentifiers(file=file_identifiers, node=node_identifiers)

    logs_client.stream_log.return_value = generate_logs_stream(NUM_LOG_CHUNKS)

    stream = await logs_manager.create_log_stream(
        identifiers=identifiers, stream_options=stream_options
    )
    i = 0
    async for chunk in stream:
        assert chunk.data.decode("utf-8") == generate_logs_stream_chunk(index=i)
        i += 1
    assert i == NUM_LOG_CHUNKS
    logs_client.stream_log.assert_awaited_with(
        node_id="1",
        log_file_name="worker-0-0-10.out",
        keep_alive=True,
        lines=100,
        interval=0.5,
    )

    # Currently cannot test actor_id with AsyncMock


@pytest.mark.skipif(
    sys.version_info < ASYNCMOCK_MIN_PYTHON_VER,
    reason=f"unittest.mock.AsyncMock requires python {ASYNCMOCK_MIN_PYTHON_VER}"
    " or higher",
)
@pytest.mark.asyncio
async def test_log_manager_wait_for_client(logs_manager):
    # Check that logs manager raises error if client does not initialize
    logs_client = logs_manager.logs_client
    logs_client.wait_until_initialized.side_effect = raise_timeout
    with pytest.raises(ValueError) as e:
        await logs_manager.list_logs("1", [])
        assert str(e) == "timed out"

    with pytest.raises(ValueError) as e:
        await logs_manager.resolve_node_id(NodeIdentifiers())
        assert str(e) == "timed out"

    with pytest.raises(ValueError) as e:
        await logs_manager.create_log_stream(
            identifiers=LogIdentifiers(
                file=FileIdentifiers(),
                node=NodeIdentifiers(),
            ),
            stream_options=LogStreamOptions(media_type="file", lines=10, interval=0.5),
        )
        assert str(e) == "timed out"

    # Check that logs manager waits for client to initialize
    logs_client.wait_until_initialized.side_effect = wait_for_500_ms
    start_time = time.time()
    try:
        await logs_manager.list_logs("1", [])
    except Exception:
        pass
    assert time.time() - start_time > 0.5

    start_time = time.time()
    try:
        await logs_manager.resolve_node_id(NodeIdentifiers())
    except Exception:
        pass
    assert time.time() - start_time > 0.5

    start_time = time.time()
    try:
        await logs_manager.create_log_stream(
            identifiers=LogIdentifiers(
                file=FileIdentifiers(),
                node=NodeIdentifiers(),
            ),
            stream_options=LogStreamOptions(media_type="file", lines=10, interval=0.5),
        )
    except Exception:
        pass
    assert time.time() - start_time > 0.5


# Integration tests


def test_logs_list(ray_start_with_dashboard):
    assert wait_until_server_available(ray_start_with_dashboard["webui_url"]) is True
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    # test that list logs is comprehensive
    response = requests.get(webui_url + "/api/v0/logs")
    response.raise_for_status()
    logs = json.loads(response.text)
    assert len(logs) == 1
    node_id = next(iter(logs))

    # test worker logs
    outs = logs[node_id]["worker_stdout"]
    errs = logs[node_id]["worker_errors"]
    core_worker_logs = logs[node_id]["python_core_worker"]

    assert len(outs) == len(errs) == len(core_worker_logs)
    assert len(outs) > 0

    for file in ["gcs_server.out", "gcs_server.err"]:
        assert file in logs[node_id]["gcs_server"]
    for file in ["raylet.out", "raylet.err"]:
        assert file in logs[node_id]["raylet"]
    for file in ["dashboard_agent.log", "dashboard.log"]:
        assert file in logs[node_id]["dashboard"]
    return True

    # Test that logs/list can be filtered
    response = requests.get(webui_url + "/api/v0/logs?filters=gcs")
    response.raise_for_status()
    logs = json.loads(response.text)
    assert len(logs) == 1
    node_id = next(iter(logs))
    assert "gcs_server" in logs[node_id]
    for category in logs[node_id]:
        if category != "gcs_server":
            assert len(logs[node_id][category]) == 0

    response = requests.get(webui_url + "/api/v0/logs?filters=worker")
    response.raise_for_status()
    logs = json.loads(response.text)
    assert len(logs) == 1
    node_id = next(iter(logs))
    worker_log_categories = [
        "python_core_worker",
        "worker_stdout",
        "worker_errors",
    ]
    assert all([cat in logs[node_id] for cat in worker_log_categories])
    for category in logs[node_id]:
        if category not in worker_log_categories:
            assert len(logs[node_id][category]) == 0


def test_logs_stream_and_tail(ray_start_with_dashboard):
    @ray.remote
    class Actor:
        def write_log(self, strings):
            for s in strings:
                print(s)

    test_log_text = "test_log_text_日志_{}"
    assert wait_until_server_available(ray_start_with_dashboard["webui_url"]) is True
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    response = requests.get(webui_url + "/api/v0/logs")
    response.raise_for_status()
    logs = json.loads(response.text)
    assert len(logs) == 1
    node_id = next(iter(logs))

    actor = Actor.remote()
    ray.get(actor.write_log.remote([test_log_text.format("XXXXXX")]))

    # Test stream and fetching by actor id
    stream_response = requests.get(
        webui_url
        + f"/api/v0/logs/stream?node_id={node_id}&lines=2"
        + "&actor_id="
        + actor._ray_actor_id.hex(),
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


def test_logs_grpc_client_termination(ray_start_with_dashboard):
    assert wait_until_server_available(ray_start_with_dashboard["webui_url"]) is True
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)
    node_id = ray_start_with_dashboard["node_id"]

    time.sleep(1)
    # Get raylet log
    RAYLET_FILE_NAME = "raylet.out"
    DASHBOARD_AGENT_FILE_NAME = "dashboard_agent.log"
    stream_response = requests.get(
        webui_url
        + f"/api/v0/logs/stream?node_id={node_id}"
        + f"&lines=0&log_file_name={RAYLET_FILE_NAME}",
        stream=True,
    )
    if stream_response.status_code != 200:
        raise ValueError(stream_response.text)
    # give enough time for the initiation message to be written to the log
    time.sleep(1)

    file_response = requests.get(
        webui_url
        + f"/api/v0/logs/file?node_id={node_id}"
        + f"&lines=10&log_file_name={DASHBOARD_AGENT_FILE_NAME}",
    )

    # Check that gRPC stream initiated as a result of starting the stream
    assert (
        f'initiated StreamLog:\nlog_file_name: "{RAYLET_FILE_NAME}"'
        "\nkeep_alive: true"
    ) in file_response.text
    # Check that gRPC stream has not terminated (is kept alive)
    assert (
        f'terminated StreamLog:\nlog_file_name: "{RAYLET_FILE_NAME}"'
        "\nkeep_alive: true"
    ) not in file_response.text

    del stream_response
    # give enough time for the termination message to be written to the log
    time.sleep(1)

    file_response = requests.get(
        webui_url
        + f"/api/v0/logs/file?node_id={node_id}"
        + f"&lines=10&log_file_name={DASHBOARD_AGENT_FILE_NAME}",
    )

    # Check that gRPC terminated as a result of closing the stream
    assert (
        f'terminated StreamLog:\nlog_file_name: "{RAYLET_FILE_NAME}"'
        "\nkeep_alive: true"
    ) in file_response.text


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
