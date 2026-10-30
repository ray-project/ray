import queue
import sys
from unittest import mock

import pytest

from ray.util.client.server.logservicer import StdStreamHandler


def _batch(pid, lines, is_err=False):
    return {
        "ip": "127.0.0.1",
        "localhost": "127.0.0.1",
        "pid": pid,
        "job": None,
        "is_err": is_err,
        "lines": list(lines),
        "actor_name": None,
        "task_name": None,
    }


def test_std_stream_handler_deduplicates_worker_logs():
    """Regression test for https://github.com/ray-project/ray/issues/58750.

    The Ray Client server forwarded every worker log line verbatim, bypassing
    the log deduplication that regular drivers apply.
    """
    log_queue = queue.Queue()
    handler = StdStreamHandler(log_queue)

    handler.handle(_batch(1, ["hello from issue 58750"]))
    handler.handle(_batch(2, ["hello from issue 58750"]))
    handler.handle(_batch(3, ["hello from issue 58750"]))

    # Only the first occurrence is forwarded immediately; the repeats are
    # aggregated inside the deduplication window.
    assert log_queue.qsize() == 1
    first = log_queue.get()
    assert first.level == -1
    assert first.name == "stdout"
    assert "hello from issue 58750" in first.msg

    # Unregistering flushes the aggregated repeats as a single summary line.
    handler.unregister_global()
    assert log_queue.qsize() == 1
    summary = log_queue.get()
    assert "hello from issue 58750" in summary.msg
    assert "repeated 2x" in summary.msg


def test_std_stream_handlers_do_not_share_dedup_state():
    """The dispatcher hands every batch to each client handler and to the
    server's own print_to_stdstream, so sharing deduplicators would count each
    repeat several times and mix clients."""
    from ray._private.ray_logging import stdout_deduplicator

    queue1, queue2 = queue.Queue(), queue.Queue()
    handler1, handler2 = StdStreamHandler(queue1), StdStreamHandler(queue2)

    for pid in (1, 2):
        batch = _batch(pid, ["shared line 58750"])
        stdout_deduplicator.deduplicate(dict(batch))  # The server's own printer.
        handler1.handle(dict(batch))
        handler2.handle(dict(batch))

    for log_queue, handler in ((queue1, handler1), (queue2, handler2)):
        assert log_queue.qsize() == 1
        assert "shared line 58750" in log_queue.get().msg
        handler.unregister_global()
        # One real repeat is flushed as the plain line; a shared deduplicator
        # would report it as "[repeated 3x across cluster]".
        summary = log_queue.get_nowait()
        assert "(pid=2)" in summary.msg
        assert "repeated" not in summary.msg, summary.msg
    stdout_deduplicator.flush()


def test_std_stream_handler_does_not_dedup_autoscaler_logs():
    log_queue = queue.Queue()
    handler = StdStreamHandler(log_queue)

    # Formatting autoscaler lines needs a GCS connection, which is irrelevant here.
    def fake_print_worker_logs(batch, file):
        file.write("\n".join(batch["lines"]))

    with mock.patch(
        "ray.util.client.server.logservicer.print_worker_logs",
        side_effect=fake_print_worker_logs,
    ):
        handler.handle(_batch("autoscaler", ["autoscaler line 58750"], is_err=True))
        handler.handle(_batch("autoscaler", ["autoscaler line 58750"], is_err=True))

    assert log_queue.qsize() == 2
    assert all(record.level == -2 for record in list(log_queue.queue))


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
