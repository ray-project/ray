#!/usr/bin/env python3
#
# Copyright 2021 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# BASED ON https://github.com/philwo/bazel-utils/blob/main/sharding/sharding.py

from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable, List, Optional, Set, Tuple
import argparse
import os
import re
import shlex
import subprocess
import sys
import xml.etree.ElementTree as ET


@dataclass
class BazelRule:
    """
    Dataclass representing a bazel py_test rule (BUILD entry).

    Only the subset of fields we care about is included.
    """

    name: str
    size: str
    timeout: Optional[str] = None

    def __post_init__(self):
        assert self.size in ("small", "medium", "large", "enormous")
        assert self.timeout in (None, "short", "moderate", "long", "eternal")

    @property
    def actual_timeout_s(self) -> float:
        # See https://bazel.build/reference/be/common-definitions
        # Timeout takes priority over size
        if self.timeout == "short":
            return 60
        if self.timeout == "moderate":
            return 60 * 5
        if self.timeout == "long":
            return 60 * 15
        if self.timeout == "eternal":
            return 60 * 60
        if self.size == "small":
            return 60
        if self.size == "medium":
            return 60 * 5
        if self.size == "large":
            return 60 * 15
        if self.size == "enormous":
            return 60 * 60

    def __lt__(self, other: "BazelRule") -> bool:
        return (self.name, self.actual_timeout_s) < (other.name, other.actual_timeout_s)

    def __hash__(self) -> int:
        return self.name.__hash__()

    @classmethod
    def from_xml_element(cls, element: ET.Element) -> "BazelRule":
        """Create a BazelRule from an XML element.

        The XML element is expected to be produced by the
        ``bazel query --output=xml`` command.
        """
        name = element.get("name")
        all_string_tags = element.findall("string")
        size = next(
            (tag.get("value") for tag in all_string_tags if tag.get("name") == "size"),
            "medium",
        )
        timeout = next(
            (
                tag.get("value")
                for tag in all_string_tags
                if tag.get("name") == "timeout"
            ),
            None,
        )
        return cls(name=name, size=size, timeout=timeout)


def quote_targets(targets: Iterable[str]) -> str:
    """Quote each target in a list so that it can be passed used in subprocess."""
    return (" ".join(shlex.quote(t) for t in targets)) if targets else ""


def partition_targets(targets: Iterable[str]) -> Tuple[List[str], List[str]]:
    """
    Given a list of string targets, partition them into included and excluded
    lists depending on whether they start with a - (exclude) or not (include).
    """
    included_targets, excluded_targets = set(), set()
    for target in targets:
        if target[0] == "-":
            assert not target[1] == "-", f"Double negation is not allowed: {target}"
            excluded_targets.add(target[1:])
        else:
            included_targets.add(target)
    return included_targets, excluded_targets


def split_tag_filters(tag_str: str) -> Tuple[Set[str], Set[str]]:
    """Split tag_filters string into include & exclude tags."""
    split_tags = tag_str.split(",") if tag_str else []
    return partition_targets(split_tags)


def generate_regex_from_tags(tags: Iterable[str]) -> str:
    """Turn tag filters into a regex used in bazel query."""
    return "|".join([f"(\\b{re.escape(tag)}\\b)" for tag in tags])


def get_target_expansion_query(
    targets: Iterable[str],
    tests_only: bool,
    exclude_manual: bool,
    include_tags: Optional[Iterable[str]] = None,
    exclude_tags: Optional[Iterable[str]] = None,
) -> str:
    """Generate the bazel query to obtain individual rules."""
    included_targets, excluded_targets = partition_targets(targets)

    included_targets = quote_targets(included_targets)
    excluded_targets = quote_targets(excluded_targets)

    query = f"set({included_targets})"

    if include_tags:
        tags_regex = generate_regex_from_tags(include_tags)
        # Each rule has to have at least one tag from
        # include_tags
        query = f'attr("tags", "{tags_regex}", {query})'

    if tests_only:
        # Discard any non-test rules
        query = f"tests({query})"

    if excluded_targets:
        # Exclude the targets we do not want
        excluded_set = f"set({excluded_targets})"
        query = f"{query} except {excluded_set}"

    if exclude_manual:
        # Exclude targets with 'manual' tag
        exclude_tags = exclude_tags or set()
        exclude_tags.add("manual")

    if exclude_tags:
        # Exclude targets which have at least one exclude_tag
        tags_regex = generate_regex_from_tags(exclude_tags)
        query = f'{query} except attr("tags", "{tags_regex}", set({included_targets}))'

    return query


def run_bazel_query(query: str, debug: bool) -> ET.Element:
    """Runs bazel query with XML output format.

    We need the XML to obtain rule metadata such as
    size, timeout, etc.
    """
    args = ["bazel", "query", "--output=xml", query]
    if debug:
        print(f"$ {args}", file=sys.stderr)
        sys.stderr.flush()
    p = subprocess.run(
        args,
        check=True,
        stdout=subprocess.PIPE,
        errors="replace",
        universal_newlines=True,
    )
    output = p.stdout.strip()
    return ET.fromstring(output) if output else None


def extract_rules_from_xml(element: ET.Element) -> List[BazelRule]:
    """Extract BazelRules from the XML obtained from ``bazel query --output=xml``."""
    xml_rules = element.findall("rule")
    return [BazelRule.from_xml_element(element) for element in xml_rules]


def group_rules_by_time_needed(
    rules: List[BazelRule],
) -> List[Tuple[float, List[BazelRule]]]:
    """
    Return a list of tuples of (timeout in seconds, list of rules)
    sorted descending.
    """
    grouped_rules = defaultdict(list)
    for rule in rules:
        grouped_rules[rule.actual_timeout_s].append(rule)
    for timeout in grouped_rules:
        grouped_rules[timeout] = sorted(grouped_rules[timeout])
    return sorted(grouped_rules.items(), key=lambda x: x[0], reverse=True)


def get_rules_for_shard_naive(
    rules_grouped_by_time: List[Tuple[float, List[BazelRule]]], index: int, count: int
) -> List[str]:
    """Create shards by assigning the same number of rules to each shard."""
    all_rules = []
    for _, rules in rules_grouped_by_time:
        all_rules.extend(rules)
    shard = sorted(all_rules)[index::count]
    return [rule.name for rule in shard]


def add_rule_to_best_shard(
    rule_to_add: BazelRule, shards: List[List[BazelRule]], optimum: float
):
    """Adds a rule to the best shard.

    The best shard is determined in the following fashion:
    1. Pick first shard which is below optimum,
    2. If no shard is below optimum, pick the shard closest
        to optimum.
    """
    first_shard_index_below_optimum = None
    shard_index_right_above_optimum = None
    shard_index_right_above_optimum_time = None
    for i, shard in enumerate(shards):
        # Total time the shard needs to run so far
        shard_time = sum(rule.actual_timeout_s for rule in shard)
        # Total time the shard would need to run with the rule_to_add
        shard_time_with_item = shard_time + rule_to_add.actual_timeout_s

        if shard_time_with_item < optimum:
            # If there's a shard below optimum, just use that
            first_shard_index_below_optimum = i
            break
        elif (
            shard_index_right_above_optimum is None
            or shard_index_right_above_optimum_time > shard_time_with_item
        ):
            # Otherwise, pick the shard closest to optimum
            shard_index_right_above_optimum = i
            shard_index_right_above_optimum_time = shard_time_with_item
    if first_shard_index_below_optimum is not None:
        best_shard_index = first_shard_index_below_optimum
    else:
        best_shard_index = shard_index_right_above_optimum

    shards[best_shard_index].append(rule_to_add)


def get_rules_for_shard_optimal(
    rules_grouped_by_time: List[Tuple[float, List[BazelRule]]], index: int, count: int
) -> List[str]:
    """Creates shards by trying to make sure each shard takes around the same time.

    We use a simple heuristic here (as this problem is NP-complete):
    1. Determine how long one shard would take if they were ideally balanced
       (this may be impossible to attain, but that's fine).
    2. Allocate the next biggest item into the first shard that is below the optimum.
    3. If there's no shard below optimium, choose the shard closest to optimum.

    This works very well for our usecase and is fully deterministic.

    ``rules_grouped_by_time`` is expected to be a list of tuples of
    (timeout in seconds, list of rules) sorted by timeout descending.
    """
    # For sanity checks later.
    all_rules = []
    for _, rules in rules_grouped_by_time:
        all_rules.extend(rules)

    # Instantiate the shards, each represented by a list.
    shards: List[List[BazelRule]] = [list() for _ in range(count)]

    # The theoretical optimum we are aiming for. Note that this may be unattainable
    # as it doesn't take into account that tests are discrete and cannot be split.
    # This is however fine, because it should only serve as a guide which shard to
    # add the next test to.
    optimum = (
        sum(timeout * len(rules) for timeout, rules in rules_grouped_by_time) / count
    )

    def get_next_longest_rule() -> BazelRule:
        """
        Get the next longest (taking up the most time) BazelRule from the
        ``rules_grouped_by_time`` list.
        """
        item = None
        for _, items in rules_grouped_by_time:
            if items:
                return items.pop()
        return item

    rule_to_add = get_next_longest_rule()
    while rule_to_add:
        add_rule_to_best_shard(rule_to_add, shards, optimum)
        rule_to_add = get_next_longest_rule()

    # Sanity checks.
    num_all_rules = sum(len(shard) for shard in shards)

    # Make sure that there are no duplicate rules.
    all_rules_set = set()
    for shard in shards:
        all_rules_set = all_rules_set.union(set(shard))
    assert len(all_rules_set) == num_all_rules, (
        f"num of unique rules {len(all_rules_set)} "
        f"doesn't match num of rules {num_all_rules}"
    )

    # Make sure that all rules have been included in the shards.
    assert all_rules_set == set(all_rules_set), (
        f"unique rules after sharding {len(all_rules_set)} "
        f"doesn't match unique rules after sharding {num_all_rules}"
    )

    print(
        f"get_rules_for_shard statistics:\n\tOptimum: {optimum} seconds\n"
        + "\n".join(
            (
                f"\tShard {i}: {len(shard)} rules, "
                f"{sum(rule.actual_timeout_s for rule in shard)} seconds"
            )
            for i, shard in enumerate(shards)
        ),
        file=sys.stderr,
    )
    return sorted([rule.name for rule in shards[index]])


def main(
    targets: List[str],
    *,
    index: int,
    count: int,
    tests_only: bool = False,
    exclude_manual: bool = False,
    tag_filters: Optional[str] = None,
    sharding_strategy: str = "optimal",
    debug: bool = False,
) -> List[str]:
    include_tags, exclude_tags = split_tag_filters(tag_filters)

    query = get_target_expansion_query(
        targets, tests_only, exclude_manual, include_tags, exclude_tags
    )
    xml_output = run_bazel_query(query, debug)
    rules = extract_rules_from_xml(xml_output)
    rules_grouped_by_time = group_rules_by_time_needed(rules)
    if sharding_strategy == "optimal":
        rules_for_this_shard = get_rules_for_shard_optimal(
            rules_grouped_by_time, index, count
        )
    else:
        rules_for_this_shard = get_rules_for_shard_naive(
            rules_grouped_by_time, index, count
        )
    return rules_for_this_shard


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Expand and shard Bazel targets.")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--tests_only", action="store_true")
    parser.add_argument("--exclude_manual", action="store_true")
    parser.add_argument(
        "--index", type=int, default=os.getenv("BUILDKITE_PARALLEL_JOB", 1)
    )
    parser.add_argument(
        "--count", type=int, default=os.getenv("BUILDKITE_PARALLEL_JOB_COUNT", 1)
    )
    parser.add_argument(
        "--tag_filters",
        type=str,
        help=(
            "Accepts the same string as in bazel test --test_tag_filters "
            "to apply the filters during gathering targets here."
        ),
    )
    parser.add_argument(
        "--sharding_strategy",
        type=str,
        default="optimal",
        help=(
            "What sharding strategy to use. Can be 'optimal' (try to make sure each "
            "shard takes up around the same time) or 'naive' (assign the same number "
            "of targets to each shard)."
        ),
    )
    parser.add_argument("targets", nargs="+")
    args, extra_args = parser.parse_known_args()
    args.targets = list(args.targets) + list(extra_args)
    if args.index >= args.count:
        parser.error(f"--index must be between 0 and {args.count - 1}")

    if args.sharding_strategy not in ("optimal", "naive"):
        parser.error(
            "--sharding_strategy must be either 'optimal' or 'naive', "
            f"got {args.sharding_strategy}"
        )

    my_targets = main(
        targets=args.targets,
        index=args.index,
        count=args.count,
        tests_only=args.tests_only,
        exclude_manual=args.exclude_manual,
        tag_filters=args.tag_filters,
        sharding_strategy=args.sharding_strategy,
        debug=args.debug,
    )

    # Print so we can capture the stdout and pipe it somewhere.
    print(" ".join(my_targets))
    sys.exit(0)
