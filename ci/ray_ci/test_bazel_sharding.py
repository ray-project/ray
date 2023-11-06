from typing import List
import pytest
import os
import shutil
import sys
import tempfile

# Required for bazel
file_parent = os.path.dirname(__file__)
sys.path.append(os.path.join(file_parent, "../"))

import bazel_sharding  # noqa: E402

WORKSPACE_KEY = "workspace"


def _prefix_rules(rules):
    return list({f"//{WORKSPACE_KEY}:{rule}" for rule in rules})


size_rules = _prefix_rules(["test_small", "test_medium", "test_large", "test_enormous"])
timeout_rules = _prefix_rules(
    ["test_short", "test_moderate", "test_long", "test_eternal"]
)
size_and_timeout_rules = _prefix_rules(["test_both_size_and_timeout"])
manual_rules = _prefix_rules(["test_default"])
all_rules = size_rules + timeout_rules + manual_rules + size_and_timeout_rules


@pytest.fixture
def mock_build_dir():
    """Create a mock bazel workspace"""
    tmpdir = tempfile.mkdtemp()
    with open(os.path.join(tmpdir, "WORKSPACE"), "w") as f:
        f.write(
            """
workspace(name = "fake_workspace")
            """
        )
    os.makedirs(os.path.join(tmpdir, WORKSPACE_KEY), exist_ok=True)
    shutil.copyfile(
        os.path.join(file_parent, "mock_BUILD"),
        os.path.join(tmpdir, WORKSPACE_KEY, "BUILD"),
    )
    cwd = os.getcwd()
    os.chdir(os.path.join(tmpdir, WORKSPACE_KEY))
    yield
    os.chdir(cwd)
    shutil.rmtree(tmpdir, ignore_errors=True)


def test_actual_timeouts(mock_build_dir):
    """Test that size and timeout attrs are mapped to seconds correctly.

    Assert that each of the fake rules is mapped correctly.
    """
    query = bazel_sharding.get_target_expansion_query(
        ["..."], tests_only=False, exclude_manual=False
    )
    xml_output = bazel_sharding.run_bazel_query(query, debug=False)
    rules = set(bazel_sharding.extract_rules_from_xml(xml_output))
    expected_timeouts = {
        "test_default": 60 * 5,
        "test_small": 60,
        "test_medium": 60 * 5,
        "test_large": 60 * 15,
        "test_enormous": 60 * 60,
        "test_short": 60,
        "test_moderate": 60 * 5,
        "test_long": 60 * 15,
        "test_eternal": 60 * 60,
        "test_both_size_and_timeout": 60 * 15,
    }
    assert len(rules) == len(expected_timeouts)
    assert (rule.actual_timeout_s == expected_timeouts[rule.name] for rule in rules)


def test_add_rule_to_best_shard():
    """Test that the best shard in optimal strategy is chosen correctly."""

    # If we start with an empty list, then add to first shard
    shards: List[List[bazel_sharding.BazelRule]] = [list() for _ in range(4)]
    optimum = 600

    rule = bazel_sharding.BazelRule("mock", "medium")
    bazel_sharding.add_rule_to_best_shard(rule, shards, optimum)
    assert shards[0][0] == rule
    assert all(not shard for shard in shards[1:])

    # Add to first shard below optimum
    old_rule = bazel_sharding.BazelRule("mock", "medium")
    shards: List[List[bazel_sharding.BazelRule]] = [[old_rule] for _ in range(4)]
    shards[3] = []
    optimum = old_rule.actual_timeout_s

    rule = bazel_sharding.BazelRule("mock", "small")
    bazel_sharding.add_rule_to_best_shard(rule, shards, optimum)
    assert shards[3][0] == rule
    assert all(shard[-1] == old_rule for shard in shards[0:3])

    # If all shards are above or equal optimum, add to the one with the smallest
    # difference
    old_rule = bazel_sharding.BazelRule("mock", "large")
    shards: List[List[bazel_sharding.BazelRule]] = [[old_rule] for _ in range(4)]
    optimum = old_rule.actual_timeout_s
    old_rule_medium = bazel_sharding.BazelRule("mock", "medium")
    shards[3][0] = old_rule_medium

    rule = bazel_sharding.BazelRule("mock", "small")
    bazel_sharding.add_rule_to_best_shard(rule, shards, optimum)
    assert shards[3][0] == old_rule_medium
    assert shards[3][-1] == rule
    assert all(shard[-1] == old_rule for shard in shards[0:3])


def test_bazel_sharding_end_to_end(mock_build_dir):
    """Test e2e working of the script without sharding.

    Assert that if we are doing no sharding, all the rules
    are outputted and the two strategies have the same
    outputs.
    """
    output = bazel_sharding.main(["..."], index=0, count=1)
    output = set(output)
    assert output == set(all_rules)

    output_naive = bazel_sharding.main(
        ["..."], index=0, count=1, sharding_strategy="naive"
    )
    output_naive = set(output_naive)
    assert output == output_naive

    output = bazel_sharding.main(["..."], index=0, count=1, exclude_manual=True)
    output = set(output)
    assert output == set(all_rules).difference(set(manual_rules))


def test_bazel_sharding_with_filters(mock_build_dir):
    """Test e2e working of the script without sharding with filters.

    Assert that the rules are properly filtered.
    """
    output = bazel_sharding.main(["..."], index=0, count=1, tag_filters="size")
    output = set(output)
    assert output == set(size_rules + size_and_timeout_rules)

    output = bazel_sharding.main(["..."], index=0, count=1, tag_filters="-timeout")
    output = set(output)
    assert output == set(size_rules + manual_rules)

    output = bazel_sharding.main(["..."], index=0, count=1, tag_filters="size,timeout")
    output = set(output)
    assert output == set(size_rules + timeout_rules + size_and_timeout_rules)

    output = bazel_sharding.main(["..."], index=0, count=1, tag_filters="size,-timeout")
    output = set(output)
    assert output == set(size_rules)

    output = bazel_sharding.main(
        ["..."], index=0, count=1, tag_filters="-size,-timeout"
    )
    output = set(output)
    assert output == set(manual_rules)


def test_bazel_sharding_two_shards(mock_build_dir):
    """Test e2e working of the script with sharding.

    Assert that the two shards are balanced as expected.
    """
    output_1_list = bazel_sharding.main(["..."], index=0, count=2)
    output_1 = set(output_1_list)

    output_2_list = bazel_sharding.main(["..."], index=1, count=2)
    output_2 = set(output_2_list)

    assert output_1.union(output_2) == set(all_rules)

    # We should be deterministic, therefore we can hardcode this
    assert output_1_list == [
        f"//{WORKSPACE_KEY}:test_eternal",
        f"//{WORKSPACE_KEY}:test_large",
        f"//{WORKSPACE_KEY}:test_long",
        f"//{WORKSPACE_KEY}:test_small",
    ]
    assert output_2_list == [
        f"//{WORKSPACE_KEY}:test_both_size_and_timeout",
        f"//{WORKSPACE_KEY}:test_default",
        f"//{WORKSPACE_KEY}:test_enormous",
        f"//{WORKSPACE_KEY}:test_medium",
        f"//{WORKSPACE_KEY}:test_moderate",
        f"//{WORKSPACE_KEY}:test_short",
    ]

    output_1_naive_list = bazel_sharding.main(
        ["..."], index=0, count=2, sharding_strategy="naive"
    )
    output_1_naive = set(output_1_naive_list)

    output_2_naive_list = bazel_sharding.main(
        ["..."], index=1, count=2, sharding_strategy="naive"
    )
    output_2_naive = set(output_2_naive_list)

    assert output_1_naive.union(output_2_naive) == set(all_rules)
    # We should be deterministic, therefore we can hardcode this
    assert output_1_naive_list == [
        f"//{WORKSPACE_KEY}:test_both_size_and_timeout",
        f"//{WORKSPACE_KEY}:test_enormous",
        f"//{WORKSPACE_KEY}:test_large",
        f"//{WORKSPACE_KEY}:test_medium",
        f"//{WORKSPACE_KEY}:test_short",
    ]
    assert output_2_naive_list == [
        f"//{WORKSPACE_KEY}:test_default",
        f"//{WORKSPACE_KEY}:test_eternal",
        f"//{WORKSPACE_KEY}:test_long",
        f"//{WORKSPACE_KEY}:test_moderate",
        f"//{WORKSPACE_KEY}:test_small",
    ]


@pytest.mark.parametrize("sharding_strategy", ("optimal", "naive"))
def test_bazel_sharding_optimal_too_many_shards(mock_build_dir, sharding_strategy):
    """
    Test e2e working of the script with sharding in the case of more shards than tests.

    Assert that the first shard has one test and the final one has none.
    """
    output_1 = bazel_sharding.main(
        ["..."], index=0, count=len(all_rules) + 1, sharding_strategy=sharding_strategy
    )
    output_1 = set(output_1)

    output_2 = bazel_sharding.main(
        ["..."],
        index=len(all_rules),
        count=len(all_rules) + 1,
        sharding_strategy=sharding_strategy,
    )
    output_2 = set(output_2)

    assert len(output_1) == 1
    assert not output_2


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
