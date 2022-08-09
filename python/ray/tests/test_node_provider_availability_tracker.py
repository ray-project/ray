import datetime
import dataclasses
import os
import sys
import pytest

from ray.autoscaler.node_launch_exception import NodeLaunchException
from ray.autoscaler._private.node_provider_availability_tracker import (
    NodeProviderAvailabilityTracker,
    NodeAvailabilitySummary,
    NodeAvailabilityRecord,
    UnavailableNodeInformation,
)


cur_time = float(0)


@pytest.fixture
def tracker() -> NodeProviderAvailabilityTracker:
    global cur_time
    cur_time = 1

    def get_time():
        global cur_time
        return cur_time

    return NodeProviderAvailabilityTracker(timer=get_time, ttl=60 * 30)


def test_basic(tracker: NodeProviderAvailabilityTracker):
    first_failure = NodeLaunchException(
        "DontFeelLikeIt", "This seems like a lot of work."
    )
    tracker.update_node_availability("my-node-a", 0, first_failure)
    assert len(tracker.summary().node_availabilities) == 1


def test_expiration(tracker: NodeProviderAvailabilityTracker):
    global cur_time

    exc = NodeLaunchException("DontFeelLikeIt", "This seems like a lot of work.")
    tracker.update_node_availability("my-node-a", 1, exc)
    assert len(tracker.summary().node_availabilities) == 1

    cur_time += 60 * 30 + 1

    assert len(tracker.summary().node_availabilities) == 0


def test_expiration_after_update(tracker: NodeProviderAvailabilityTracker):
    global cur_time

    exc = NodeLaunchException("DontFeelLikeIt", "This seems like a lot of work.")
    tracker.update_node_availability("my-node-a", 1, exc)
    assert len(tracker.summary().node_availabilities) == 1

    cur_time += 60 * 30 - 1

    tracker.update_node_availability("my-node-a", cur_time, exc)
    assert len(tracker.summary().node_availabilities) == 1

    cur_time += 2

    assert len(tracker.summary().node_availabilities) == 1

    cur_time += 60 * 30 + 1
    assert len(tracker.summary().node_availabilities) == 0


def test_reinsert_after_expiration(tracker: NodeProviderAvailabilityTracker):
    global cur_time

    exc = NodeLaunchException("DontFeelLikeIt", "This seems like a lot of work.")
    tracker.update_node_availability("my-node-a", 1, exc)
    assert len(tracker.summary().node_availabilities) == 1

    cur_time += 60 * 30 + 1

    assert len(tracker.summary().node_availabilities) == 0
    tracker.update_node_availability("my-node-a", cur_time, exc)
    assert len(tracker.summary().node_availabilities) == 1


def test_expire_multiple(tracker: NodeProviderAvailabilityTracker):
    """
    Insert A
    Insert B
    Insert C
    Update A

    -- after b's expiration, before c's --

    Assert B is evicted, A and C are not.
    """
    global cur_time
    cur_time = 20

    exc = NodeLaunchException("DontFeelLikeIt", "This seems like a lot of work.")
    tracker.update_node_availability("my-node-a", 0, exc)
    tracker.update_node_availability("my-node-b", 10, exc)
    tracker.update_node_availability("my-node-c", 20, node_launch_exception=None)

    assert len(tracker.summary().node_availabilities) == 3
    cur_time = 30

    tracker.update_node_availability("my-node-a", 30, exc)

    # This is after B expires
    cur_time = 60 * 30 + 11
    assert len(tracker.summary().node_availabilities) == 2

    summary = tracker.summary()

    assert "my-node-a" in summary.node_availabilities
    assert "my-node-c" in summary.node_availabilities
    assert "my-node-b" not in summary.node_availabilities

    assert summary.node_availabilities["my-node-a"].node_type == "my-node-a"
    assert summary.node_availabilities["my-node-a"].last_checked_timestamp == 30

    assert summary.node_availabilities["my-node-c"].node_type == "my-node-c"
    assert summary.node_availabilities["my-node-c"].last_checked_timestamp == 20


def test_summary(tracker: NodeProviderAvailabilityTracker):
    exc = NodeLaunchException("DontFeelLikeIt", "This seems like a lot of work.")
    tracker.update_node_availability("my-node-a", 0, exc)

    tracker.update_node_availability("my-node-b", 1, None)

    summary = tracker.summary()

    expected = {
        "my-node-a": NodeAvailabilityRecord(
            node_type="my-node-a",
            is_available=False,
            last_checked_timestamp=0,
            unavailable_node_information=UnavailableNodeInformation(
                category="DontFeelLikeIt", description="This seems like a lot of work."
            ),
        ),
        "my-node-b": NodeAvailabilityRecord(
            node_type="my-node-b",
            is_available=True,
            last_checked_timestamp=1,
            unavailable_node_information=None,
        ),
    }

    assert summary.node_availabilities == expected


def get_timestamp(hour: int, minute: int, second: int, microsecond: int) -> float:
    # Year, month, day don't just need to be filled out consistently.
    dt = datetime.datetime(
        year=2012,
        month=12,
        day=21,
        hour=hour,
        minute=minute,
        second=second,
        microsecond=microsecond,
    )
    return dt.timestamp()


def test_summary_string(tracker: NodeProviderAvailabilityTracker):
    global cur_time

    cur_time = get_timestamp(
        hour=12,
        minute=13,
        second=17,
        microsecond=1234,
    )
    exc = NodeLaunchException(
        "InsufficientInstanceCapacity", "Some message about spot instances."
    )
    tracker.update_node_availability("spot-gpu", cur_time, exc)

    cur_time = get_timestamp(
        hour=12,
        minute=13,
        second=51,
        microsecond=4123,
    )
    exc = NodeLaunchException(
        "InstanceLimitExceeded", "Some message about pesky quotas."
    )
    tracker.update_node_availability("newer-gpu-type", cur_time, exc)

    cur_time = get_timestamp(
        hour=12,
        minute=14,
        second=18,
        microsecond=1,
    )
    tracker.update_node_availability("on-demand-gpu", cur_time, None)

    summary = tracker.summary()

    summary_str = summary.summary_string(separator_len=60)

    expected = """
Availability
------------------------------------------------------------
Node types:
 spot-gpu (attempted=12:13:17): InsufficientInstanceCapacity
 newer-gpu-type (attempted=12:13:51): InstanceLimitExceeded
 on-demand-gpu (attempted=12:14:18): Available
    """.strip()

    assert summary_str == expected


def test_summary_from_dict():
    orig = NodeAvailabilitySummary(
        node_availabilities={
            "my-node-a": NodeAvailabilityRecord(
                node_type="my-node-a",
                is_available=False,
                last_checked_timestamp=0,
                unavailable_node_information=UnavailableNodeInformation(
                    category="DontFeelLikeIt",
                    description="This seems like a lot of work.",
                ),
            ),
            "my-node-b": NodeAvailabilityRecord(
                node_type="my-node-b",
                is_available=True,
                last_checked_timestamp=1,
                unavailable_node_information=None,
            ),
        }
    )

    to_dict = dataclasses.asdict(orig)

    remarshalled = NodeAvailabilitySummary.from_fields(**to_dict)

    assert orig == remarshalled


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
