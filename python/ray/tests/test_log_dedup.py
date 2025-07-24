import sys

import pytest

from ray._private.ray_logging import LogDeduplicator


def test_nodedup_logs_single_process():
    dedup = LogDeduplicator(5, None, None)
    batch1 = {
        "ip": "node1",
        "pid": 100,
        "lines": ["hello world", "good bye id=123, 0x13"],
    }

    # Immediately prints always.
    out1 = dedup.deduplicate(batch1)
    assert out1 == [batch1]
    out1 = dedup.deduplicate(batch1)
    assert out1 == [batch1]
    out1 = dedup.deduplicate(batch1)
    assert out1 == [batch1]


def test_nodedup_logs_buffer_only_lines():
    now = 142300000.0

    def gettime():
        return now

    dedup = LogDeduplicator(5, None, None, _timesource=gettime)
    batch1 = {
        "ip": "node1",
        "pid": 100,
        # numbers are canonicalised, so this would lead to empty dedup_key
        "lines": ["1"],
    }

    # Immediately prints always.
    out1 = dedup.deduplicate(batch1)
    assert out1 == [batch1]

    now += 1.0

    # Should print new lines even if it is number only again
    batch2 = {
        "ip": "node2",
        "pid": 200,
        "lines": ["2"],
    }
    out2 = dedup.deduplicate(batch2)
    assert out2 == [
        {
            "ip": "node2",
            "pid": 200,
            "lines": ["2"],
        }
    ]

    now += 3.0

    # Should print new lines even if it is same number
    batch3 = {
        "ip": "node3",
        "pid": 300,
        "lines": ["2"],
    }
    # Should buffer duplicates.
    out3 = dedup.deduplicate(batch3)
    assert out3 == [
        {
            "ip": "node3",
            "pid": 300,
            "lines": ["2"],
        }
    ]


def test_dedup_logs_multiple_processes():
    now = 142300000.0

    def gettime():
        return now

    dedup = LogDeduplicator(5, None, None, _timesource=gettime)
    batch1 = {
        "ip": "node1",
        "pid": 100,
        "lines": ["hello world", "good bye id=123, 0x13"],
    }
    batch2 = {
        "ip": "node1",
        "pid": 200,
        "lines": ["hello world", "good bye id=456, 0x11", "extra message"],
    }
    batch3 = {
        "ip": "node2",
        "pid": 200,
        "lines": ["hello world", "good bye id=999, 0xfa", "extra message"],
    }
    batch4 = {
        "ip": "node3",
        "pid": 100,
        "lines": ["hello world", "something else"],
    }

    # Immediately prints.
    out1 = dedup.deduplicate(batch1)
    assert out1 == [batch1]
    now += 1.0

    # Should buffer duplicates.
    out2 = dedup.deduplicate(batch2)
    assert out2 == [
        {
            "ip": "node1",
            "pid": 200,
            "lines": ["extra message"],
        }
    ]
    now += 1.0
    out3 = dedup.deduplicate(batch3)
    assert out3 == [
        {
            "ip": "node2",
            "pid": 200,
            "lines": [],
        }
    ]

    # Should print duplicates.
    now += 5.0
    out4 = dedup.deduplicate(batch4)
    assert len(out4) == 4
    assert {"ip": "node3", "pid": 100, "lines": ["something else"]} in out4
    assert {
        "ip": "node3",
        "pid": 100,
        "lines": [
            "hello world\x1b[32m [repeated 3x across cluster] (Ray deduplicates "
            "logs by default. Set RAY_DEDUP_LOGS=0 to disable log deduplication, or "
            "see https://docs.ray.io/en/master/ray-observability/user-guides/"
            "configure-logging.html#log-deduplication for more options.)\x1b[0m"
        ],
    } in out4
    assert {
        "ip": "node2",
        "pid": 200,
        "lines": ["good bye id=999, 0xfa\x1b[32m [repeated 2x across cluster]\x1b[0m"],
    } in out4
    assert {"ip": "node2", "pid": 200, "lines": ["extra message"]} in out4

    # Nothing to flush.
    end = dedup.flush()
    assert not end

    # Flush fresh duplicates.
    out4 = dedup.deduplicate(batch3)
    out4 = dedup.deduplicate(batch4)
    out4 = dedup.deduplicate(batch4)
    end = dedup.flush()
    assert end == [
        {
            "ip": "node3",
            "pid": 100,
            "lines": ["hello world\x1b[32m [repeated 2x across cluster]\x1b[0m"],
        }
    ]


def test_dedup_logs_allow_and_skip_regexes():
    dedup = LogDeduplicator(5, "ALLOW_ME", "SKIP_ME")
    batch1 = {
        "ip": "node1",
        "pid": 100,
        "lines": ["hello world", "good bye ALLOW_ME"],
    }
    batch2 = {
        "ip": "node1",
        "pid": 200,
        "lines": ["hello world", "good bye ALLOW_ME", "extra message SKIP_ME"],
    }
    out1 = dedup.deduplicate(batch1)
    assert out1 == [batch1]
    out2 = dedup.deduplicate(batch2)
    assert out2 == [{"ip": "node1", "pid": 200, "lines": ["good bye ALLOW_ME"]}]


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
