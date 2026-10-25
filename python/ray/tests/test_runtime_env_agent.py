import asyncio
import logging
import os
import re
import sys
import time
from typing import List, Tuple

import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray._private import ray_constants
from ray._private.runtime_env.agent.runtime_env_agent import (
    ReferenceTable,
    SetupLoggerFactory,
    UriType,
)
from ray._private.test_utils import (
    get_error_message,
    init_error_pubsub,
)
from ray.core.generated import common_pb2
from ray.runtime_env import RuntimeEnv

import psutil

logger = logging.getLogger(__name__)


def test_reference_table():
    expected_unused_uris = []
    expected_unused_runtime_env = str()

    def uris_parser(runtime_env) -> Tuple[str, UriType]:
        result = list()
        result.append((runtime_env.working_dir(), "working_dir"))
        py_module_uris = runtime_env.py_modules()
        for uri in py_module_uris:
            result.append((uri, "py_modules"))
        return result

    def unused_uris_processor(unused_uris: List[Tuple[str, UriType]]) -> None:
        nonlocal expected_unused_uris
        assert expected_unused_uris
        for unused in unused_uris:
            assert unused in expected_unused_uris
            expected_unused_uris.remove(unused)
        assert not expected_unused_uris

    def unused_runtime_env_processor(unused_runtime_env: str) -> None:
        nonlocal expected_unused_runtime_env
        assert expected_unused_runtime_env
        assert expected_unused_runtime_env == unused_runtime_env
        expected_unused_runtime_env = None

    reference_table = ReferenceTable(
        uris_parser, unused_uris_processor, unused_runtime_env_processor
    )
    runtime_env_1 = RuntimeEnv(
        working_dir="s3://working_dir_1.zip",
        py_modules=["s3://py_module_A.zip", "s3://py_module_B.zip"],
    )
    runtime_env_2 = RuntimeEnv(
        working_dir="s3://working_dir_2.zip",
        py_modules=["s3://py_module_A.zip", "s3://py_module_C.zip"],
    )
    # Add runtime env 1
    reference_table.increase_reference(
        runtime_env_1, runtime_env_1.serialize(), "raylet"
    )
    # Add runtime env 2
    reference_table.increase_reference(
        runtime_env_2, runtime_env_2.serialize(), "raylet"
    )
    # Add runtime env 1 by `client_server`, this will be skipped by reference table.
    reference_table.increase_reference(
        runtime_env_1, runtime_env_1.serialize(), "client_server"
    )

    # Remove runtime env 1
    expected_unused_uris.append(("s3://working_dir_1.zip", "working_dir"))
    expected_unused_uris.append(("s3://py_module_B.zip", "py_modules"))
    expected_unused_runtime_env = runtime_env_1.serialize()
    reference_table.decrease_reference(
        runtime_env_1, runtime_env_1.serialize(), "raylet"
    )
    assert not expected_unused_uris
    assert not expected_unused_runtime_env

    # Remove runtime env 2
    expected_unused_uris.append(("s3://working_dir_2.zip", "working_dir"))
    expected_unused_uris.append(("s3://py_module_A.zip", "py_modules"))
    expected_unused_uris.append(("s3://py_module_C.zip", "py_modules"))
    expected_unused_runtime_env = runtime_env_2.serialize()
    reference_table.decrease_reference(
        runtime_env_2, runtime_env_2.serialize(), "raylet"
    )
    assert not expected_unused_uris
    assert not expected_unused_runtime_env


def search_agent(processes):
    for p in processes:
        try:
            for c in p.cmdline():
                # in case linux truncates the proctitle
                if ray_constants.AGENT_PROCESS_TYPE_RUNTIME_ENV_AGENT[:15] in c:
                    return p
        except Exception:
            pass


def check_agent_register(raylet_proc, agent_pid):
    # Check if agent register is OK.
    for x in range(5):
        logger.info("Check agent is alive.")
        agent_proc = search_agent(raylet_proc.children())
        assert agent_proc.pid == agent_pid
        time.sleep(1)


@pytest.mark.skipif(sys.platform == "win32", reason="no fate sharing for windows")
def test_raylet_and_agent_share_fate(shutdown_only):
    """Test raylet and agent share fate."""

    ray.init()
    p = init_error_pubsub()

    node = ray._private.worker._global_node
    all_processes = node.all_processes
    raylet_proc_info = all_processes[ray_constants.PROCESS_TYPE_RAYLET][0]
    raylet_proc = psutil.Process(raylet_proc_info.process.pid)

    wait_for_condition(lambda: search_agent(raylet_proc.children()))
    agent_proc = search_agent(raylet_proc.children())
    agent_pid = agent_proc.pid

    check_agent_register(raylet_proc, agent_pid)

    # The agent should be dead if raylet exits.
    raylet_proc.terminate()
    raylet_proc.wait()
    agent_proc.wait(15)

    # No error should be reported for graceful termination.
    errors = get_error_message(p, 1, ray_constants.RAYLET_DIED_ERROR)
    assert len(errors) == 0, errors

    ray.shutdown()

    ray_context = ray.init()
    all_processes = ray._private.worker._global_node.all_processes
    raylet_proc_info = all_processes[ray_constants.PROCESS_TYPE_RAYLET][0]
    raylet_proc = psutil.Process(raylet_proc_info.process.pid)
    wait_for_condition(lambda: search_agent(raylet_proc.children()))
    agent_proc = search_agent(raylet_proc.children())
    agent_pid = agent_proc.pid

    check_agent_register(raylet_proc, agent_pid)

    # The raylet should be dead if agent exits.
    agent_proc.kill()
    agent_proc.wait()
    raylet_proc.wait(15)

    worker_node_id = ray_context.address_info["node_id"]
    worker_node_info = [
        node for node in ray.nodes() if node["NodeID"] == worker_node_id
    ][0]
    assert not worker_node_info["Alive"]
    assert worker_node_info["DeathReason"] == common_pb2.NodeDeathInfo.Reason.Value(
        "UNEXPECTED_TERMINATION"
    )
    assert (
        "failed and raylet fate-shares with it."
        in worker_node_info["DeathReasonMessage"]
    )


@pytest.mark.skipif(sys.platform == "win32", reason="no fate sharing for windows")
def test_agent_report_unexpected_raylet_death(shutdown_only):
    """Test agent reports Raylet death if it is not SIGTERM."""

    ray.init()
    p = init_error_pubsub()

    node = ray._private.worker._global_node
    all_processes = node.all_processes
    raylet_proc_info = all_processes[ray_constants.PROCESS_TYPE_RAYLET][0]
    raylet_proc = psutil.Process(raylet_proc_info.process.pid)

    wait_for_condition(lambda: search_agent(raylet_proc.children()))
    agent_proc = search_agent(raylet_proc.children())
    agent_pid = agent_proc.pid

    check_agent_register(raylet_proc, agent_pid)

    # The agent should be dead if raylet exits.
    raylet_proc.kill()
    raylet_proc.wait()
    agent_proc.wait(15)

    errors = get_error_message(p, 1, ray_constants.RAYLET_DIED_ERROR)
    assert len(errors) == 1, errors
    err = errors[0]
    assert err["type"] == ray_constants.RAYLET_DIED_ERROR
    assert "Termination is unexpected." in err["error_message"], err["error_message"]
    assert "Raylet logs:" in err["error_message"], err["error_message"]
    assert (
        os.path.getsize(os.path.join(node.get_session_dir_path(), "logs", "raylet.out"))
        < 1 * 1024**2
    )


@pytest.mark.skipif(sys.platform == "win32", reason="no fate sharing for windows")
def test_agent_report_unexpected_raylet_death_large_file(shutdown_only):
    """Test agent reports Raylet death if it is not SIGTERM."""

    ray.init()
    p = init_error_pubsub()

    node = ray._private.worker._global_node
    all_processes = node.all_processes
    raylet_proc_info = all_processes[ray_constants.PROCESS_TYPE_RAYLET][0]
    raylet_proc = psutil.Process(raylet_proc_info.process.pid)

    wait_for_condition(lambda: search_agent(raylet_proc.children()))
    agent_proc = search_agent(raylet_proc.children())
    agent_pid = agent_proc.pid

    check_agent_register(raylet_proc, agent_pid)

    # Append to the Raylet log file with data >> 1 MB.
    with open(
        os.path.join(node.get_session_dir_path(), "logs", "raylet.out"), "a"
    ) as f:
        f.write("test data\n" * 1024**2)

    # The agent should be dead if raylet exits.
    raylet_proc.kill()
    raylet_proc.wait()
    agent_proc.wait(15)

    # Reading and publishing logs should still work.
    errors = get_error_message(p, 1, ray_constants.RAYLET_DIED_ERROR)
    assert len(errors) == 1, errors
    err = errors[0]
    assert err["type"] == ray_constants.RAYLET_DIED_ERROR
    assert "Termination is unexpected." in err["error_message"], err["error_message"]
    assert "Raylet logs:" in err["error_message"], err["error_message"]


def _make_setup_logger_factory(log_dir, logging_level=logging.INFO):
    return SetupLoggerFactory(
        logging_level=logging_level,
        logging_format=ray_constants.LOGGER_FORMAT,
        log_dir=str(log_dir),
        max_bytes=1024 * 1024,
        backup_count=1,
    )


def test_setup_logger_releases_file_handles_and_keeps_file(tmp_path):
    """The FD is scoped to the setup; the log file on disk is not."""
    factory = _make_setup_logger_factory(tmp_path)
    with factory.setup_logger("01000000", []) as logger:
        logger.info("hello")
        assert len(factory._pooled_handlers) == 1
        (entry,) = factory._pooled_handlers.values()
    # Pool is empty and the handler's stream is closed...
    assert factory._pooled_handlers == {}
    assert entry.handler.stream is None
    # ...but the file is still on disk for the log monitor and dashboard.
    log_file = tmp_path / "runtime_env_setup-01000000.log"
    assert "hello" in log_file.read_text()


def test_setup_logger_shares_handler_across_concurrent_setups(tmp_path):
    """Two jobs writing the same log_files entry share one reference-counted
    handler; the file closes only when the last setup exits."""
    factory = _make_setup_logger_factory(tmp_path)
    shared_path = os.path.abspath(str(tmp_path / "shared.log"))
    with factory.setup_logger("0aaa", ["shared.log"]) as first:
        with factory.setup_logger("0bbb", ["shared.log"]) as second:
            entry = factory._pooled_handlers[shared_path]
            assert entry.ref_count == 2
            assert entry.handler in first.handlers
            assert entry.handler in second.handlers
            # Two per-job setup logs plus the one shared handler.
            assert len(factory._pooled_handlers) == 3
            first.info("from first")
            second.info("from second")
        # Inner setup exited: still open for the outer one.
        assert factory._pooled_handlers[shared_path].ref_count == 1
        assert entry.handler.stream is not None
    assert factory._pooled_handlers == {}
    assert entry.handler.stream is None
    content = (tmp_path / "shared.log").read_text()
    assert "from first" in content and "from second" in content


@pytest.mark.parametrize("exit_path", ["exception", "timeout"])
def test_setup_logger_releases_on_error_paths(tmp_path, exit_path):
    factory = _make_setup_logger_factory(tmp_path)
    if exit_path == "exception":
        with pytest.raises(RuntimeError):
            with factory.setup_logger("01000000", []) as logger:
                logger.info("started")
                raise RuntimeError("setup failed")
    else:

        async def _setup():
            with factory.setup_logger("01000000", []) as logger:
                logger.info("started")
                await asyncio.sleep(60)

        async def _run():
            with pytest.raises(asyncio.TimeoutError):
                # wait_for awaits the cancelled coroutine before raising, so
                # the context manager unwinds before the timeout is reported.
                await asyncio.wait_for(_setup(), timeout=0.05)

        asyncio.run(_run())
    assert factory._pooled_handlers == {}


def test_setup_logger_not_in_global_registry(tmp_path):
    """The per-setup logger must not be resurrectable via logging.getLogger."""
    factory = _make_setup_logger_factory(tmp_path)
    with factory.setup_logger("01000000", []) as logger:
        assert logger.name == "runtime_env_01000000"
        assert logger.name not in logging.Logger.manager.loggerDict
    assert "runtime_env_01000000" not in logging.Logger.manager.loggerDict


def test_setup_logger_late_write_does_not_reopen_file(tmp_path):
    """A write after release must not silently reopen the file.

    FileHandler.emit() lazily reopens its file when the stream is None, so a
    closed handler that is still attached to the logger would leak the
    descriptor again with no owner left to close it. This pins the
    detach-before-close ordering.
    """
    factory = _make_setup_logger_factory(tmp_path)
    with factory.setup_logger("01000000", []) as logger:
        (entry,) = factory._pooled_handlers.values()
    logger.warning("late write after release")
    assert entry.handler.stream is None
    assert factory._pooled_handlers == {}
    assert "late write" not in (tmp_path / "runtime_env_setup-01000000.log").read_text()
    assert all(isinstance(h, logging.NullHandler) for h in logger.handlers)


def test_setup_logger_concurrent_late_emit_does_not_reopen_file(tmp_path):
    """A racing thread that grabbed the handler before it was detached must
    not reopen the file by emitting after close.

    Logger.callHandlers reads the handler list before Handler.handle runs, so
    an executor thread (e.g. conda work logging after a setup timeout) can
    hold the handler across the detach + close. FileHandler.emit lazily
    reopens the file in append mode, which would leak a descriptor with no
    owner left to close it. Calling handle() on the handler directly
    simulates that thread.
    """
    factory = _make_setup_logger_factory(tmp_path)
    with factory.setup_logger("01000000", []) as logger:
        (entry,) = factory._pooled_handlers.values()
        handler = entry.handler
        logger.info("in scope")
    record = logging.LogRecord(
        "runtime_env_01000000",
        logging.WARNING,
        __file__,
        0,
        "late concurrent write",
        None,
        None,
    )
    handler.handle(record)
    assert handler.stream is None
    log_text = (tmp_path / "runtime_env_setup-01000000.log").read_text()
    assert "late concurrent write" not in log_text


def test_setup_logger_release_swaps_handler_list_atomically(tmp_path):
    """Release must replace the handler list object, not mutate it in place.

    A concurrent late write iterates whichever list object it read first, so
    the old list must stay intact as a snapshot (its handlers refuse to emit
    once closed) and the new list must never be empty — an empty handler list
    falls through to logging.lastResort on the agent's stderr (the logger has
    propagate off and no parent).
    """
    factory = _make_setup_logger_factory(tmp_path)
    with factory.setup_logger("01000000", []) as logger:
        old_handler_list = logger.handlers
        old_snapshot = list(old_handler_list)
        assert old_snapshot
    assert logger.handlers is not old_handler_list
    assert list(old_handler_list) == old_snapshot
    assert logger.handlers
    assert all(isinstance(h, logging.NullHandler) for h in logger.handlers)


def test_setup_logger_dedupes_paths_within_one_setup(tmp_path):
    factory = _make_setup_logger_factory(tmp_path)
    with factory.setup_logger("01000000", ["dup.log", "dup.log", "./dup.log"]):
        entry = factory._pooled_handlers[os.path.abspath(str(tmp_path / "dup.log"))]
        assert entry.ref_count == 1
    assert factory._pooled_handlers == {}


def test_setup_logger_matches_component_logger_format_and_level(tmp_path):
    """Parity with setup_component_logger: level filtering and LOGGER_FORMAT.

    A bare logging.Logger would pass every DEBUG record (NOTSET with no
    parent) and a bare handler would write unformatted messages; nothing else
    in the suite would notice either regression.
    """
    factory = _make_setup_logger_factory(tmp_path, logging_level=logging.INFO)
    with factory.setup_logger("01000000", []) as logger:
        logger.debug("must be filtered")
        logger.info("visible message")
    lines = (tmp_path / "runtime_env_setup-01000000.log").read_text().splitlines()
    assert not any("must be filtered" in line for line in lines)
    (line,) = [line for line in lines if "visible message" in line]
    # LOGGER_FORMAT: "%(asctime)s\t%(levelname)s %(filename)s:%(lineno)s -- %(message)s"
    assert re.match(
        r"^\d{4}-\d{2}-\d{2} [\d:,]+\tINFO \S+:\d+ -- visible message$", line
    ), line


def test_setup_logger_accepts_string_logging_level(tmp_path):
    factory = SetupLoggerFactory(
        logging_level="info",
        logging_format=ray_constants.LOGGER_FORMAT,
        log_dir=str(tmp_path),
        max_bytes=0,
        backup_count=1,
    )
    with factory.setup_logger("01000000", []) as logger:
        assert logger.level == logging.INFO


@pytest.mark.skipif(sys.platform != "linux", reason="/proc/self/fd is Linux-only")
def test_setup_logger_fd_count_stays_flat_across_many_setups(tmp_path):
    """The regression that motivated this: FDs must scale with in-flight
    setups, not with the cumulative number of jobs (#54935)."""

    def fd_count():
        return len(os.listdir("/proc/self/fd"))

    factory = _make_setup_logger_factory(tmp_path)
    with factory.setup_logger("warmup", []) as logger:
        logger.info("warmup")
    before = fd_count()
    for i in range(200):
        with factory.setup_logger(f"{i:08d}", []) as logger:
            logger.info("job %d", i)
    assert fd_count() <= before + 1


def test_setup_logger_falls_back_to_stream_handler_without_log_dir(tmp_path):
    """Mirrors setup_component_logger: no log_dir means stderr, nothing pooled."""
    factory = SetupLoggerFactory(
        logging_level=logging.INFO,
        logging_format=ray_constants.LOGGER_FORMAT,
        log_dir="",
        max_bytes=0,
        backup_count=1,
    )
    with factory.setup_logger("01000000", []) as logger:
        assert factory._pooled_handlers == {}
        assert any(isinstance(h, logging.StreamHandler) for h in logger.handlers)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
