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
import subprocess
import sys
import xml.etree.ElementTree as ET


@dataclass
class BazelRule:
    # Only the subset of fields we care about
    name: str
    size: str
    timeout: Optional[str] = None

    def __post_init__(self):
        assert self.size in ("small", "medium", "large", "enormous")
        assert self.timeout in (None, "short", "moderate", "long", "eternal")

    @property
    def actual_timeout_s(self) -> float:
        # See https://bazel.build/reference/be/common-definitions
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
        return (self.actual_timeout_s, self.name) < (other.actual_timeout_s, other.name)

    def __hash__(self) -> int:
        return self.name.__hash__()

    @classmethod
    def from_xml_element(cls, element: ET.Element) -> "BazelRule":
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
        return BazelRule(name=name, size=size, timeout=timeout)


def partition_targets(targets: Iterable[str]) -> Tuple[List[str], List[str]]:
    included_targets, excluded_targets = [], []
    for target in targets:
        if target.startswith("-"):
            excluded_targets.append(target[1:])
        else:
            included_targets.append(target)
    return included_targets, excluded_targets


def quote_targets(targets: Iterable[str]) -> str:
    return (" ".join("'{}'".format(t) for t in targets)) if targets else ""


def split_tag_filters(tag_str: str) -> Tuple[Set[str], Set[str]]:
    """Split tag_filters str into include & exclude tags"""
    split_tags = tag_str.split(",") if tag_str else []
    include_tags = set()
    exclude_tags = set()
    for tag in split_tags:
        if tag[0] == "-":
            assert not tag[1] == "-", f"Double negation is not allowed {tag}"
            exclude_tags.add(tag[1:])
        else:
            include_tags.add(tag)
    return include_tags, exclude_tags


def generate_regex_from_tags(tags: Iterable[str]) -> str:
    return "|".join([f"(\\b{tag}\\b)" for tag in tags])


def get_target_expansion_query(
    targets: Iterable[str],
    tests_only: bool,
    exclude_manual: bool,
    include_tags: Optional[Iterable[str]] = None,
    exclude_tags: Optional[Iterable[str]] = None,
) -> str:
    included_targets, excluded_targets = partition_targets(targets)

    included_targets = quote_targets(included_targets)
    excluded_targets = quote_targets(excluded_targets)

    query = "set({})".format(included_targets)

    if include_tags:
        tags_regex = generate_regex_from_tags(include_tags)
        query = 'attr("tags", "{}", {})'.format(tags_regex, query)

    if tests_only:
        query = "tests({})".format(query)

    if excluded_targets:
        excluded_set = "set({})".format(excluded_targets)
        if tests_only:
            excluded_set = "tests({})".format(excluded_set)
        query = "{} except {}".format(query, excluded_set)

    if exclude_manual:
        query = '{} except tests(attr("tags", "\\bmanual\\b", set({})))'.format(
            query, included_targets
        )

    if exclude_tags:
        tags_regex = generate_regex_from_tags(exclude_tags)
        query = '{} except attr("tags", "{}", set({}))'.format(
            query, tags_regex, included_targets
        )

    return query


def run_bazel_query(query: str, debug: bool) -> ET.Element:
    """Runs bazel query with xml output format"""
    args = ["bazel", "query", query]
    if debug:
        print("$ {}".format(" ".join(args)), file=sys.stderr)
        sys.stderr.flush()
    p = subprocess.run(
        ["bazel", "query", "--output=xml", query],
        check=True,
        stdout=subprocess.PIPE,
        errors="replace",
        universal_newlines=True,
    )
    output = p.stdout.strip()
    return ET.fromstring(output) if output else None


def extract_rules_from_xml(element: ET.Element) -> List[BazelRule]:
    xml_rules = element.findall("rule")
    return [BazelRule.from_xml_element(element) for element in xml_rules]


def group_rules_by_time_needed(
    rules: List[BazelRule],
) -> List[Tuple[float, List[BazelRule]]]:
    """Returns a list of tuples of (timeout, list of rules) sorted descending"""
    grouped_rules = defaultdict(list)
    for rule in rules:
        grouped_rules[rule.actual_timeout_s].append(rule)
    for timeout in grouped_rules:
        grouped_rules[timeout] = sorted(grouped_rules[timeout])
    return sorted(grouped_rules.items(), key=lambda x: x[0], reverse=True)


def get_targets_for_shard_naive(
    rules_grouped_by_time: List[Tuple[float, List[BazelRule]]], index: int, count: int
) -> List[str]:
    """Create shards by assigning the same number of targets to each shard"""
    all_targets = []
    for timeout, targets in rules_grouped_by_time:
        all_targets.extend(targets)
    shard = sorted(all_targets)[index::count]
    return [rule.name for rule in shard]


def get_targets_for_shard_optimal(
    rules_grouped_by_time: List[Tuple[float, List[BazelRule]]], index: int, count: int
) -> List[str]:
    """Creates shards by trying to make sure each shard takes around the same time"""
    # For sanity checks later
    expected_num_all_targets = sum(
        len(targets) for timeout, targets in rules_grouped_by_time
    )
    all_targets = []
    for timeout, targets in rules_grouped_by_time:
        all_targets.extend(targets)

    # We use a simple heuristic here (as this problem is NP-complete):
    # 1. Determine how long one shard would take if they were ideally balanced
    #    (this may be impossible to attain, but that's fine)
    # 2. Allocate the next biggest item into the first shard that is below the optimum
    # 3. If there's no shard below optimium, choose the shard closest to optimum

    shards: List[List[BazelRule]] = [list() for _ in range(count)]
    optimum = (
        sum(timeout * len(rules) for timeout, rules in rules_grouped_by_time) / count
    )

    def get_next_biggest_item() -> BazelRule:
        item = None
        for timeout, items in rules_grouped_by_time:
            if items:
                item = items.pop()
                break
        return item

    def get_shard_index_to_add_to(item_to_add: BazelRule) -> int:
        shard_indices_below_optimum = []
        shard_index_right_above_optimum = None
        shard_index_right_above_optimum_time = None
        for i, shard in enumerate(shards):
            shard_time = sum(rule.actual_timeout_s for rule in shard)
            shard_time_with_item = shard_time + item_to_add.actual_timeout_s
            if shard_time_with_item < optimum:
                shard_indices_below_optimum.append(i)
            elif (
                shard_index_right_above_optimum is None
                or shard_index_right_above_optimum_time > shard_time_with_item
            ):
                shard_index_right_above_optimum = i
                shard_index_right_above_optimum_time = shard_time_with_item
        if shard_indices_below_optimum:
            return shard_indices_below_optimum[0]
        return shard_index_right_above_optimum

    item_to_add = get_next_biggest_item()
    while item_to_add:
        shard_index_to_add_to = get_shard_index_to_add_to(item_to_add)
        shards[shard_index_to_add_to].append(item_to_add)
        item_to_add = get_next_biggest_item()

    # Sanity checks.
    num_all_targets = sum(len(shard) for shard in shards)
    assert (
        num_all_targets == expected_num_all_targets
    ), f"got {num_all_targets} targets, expected {expected_num_all_targets}"
    all_targets_set = set()
    for shard in shards:
        all_targets_set = all_targets_set.union(set(shard))
    assert len(all_targets_set) == num_all_targets, (
        f"num of unique targets {len(all_targets_set)} "
        f"doesn't match num of targets {num_all_targets}"
    )
    assert all_targets_set == set(all_targets_set), (
        f"unique targets after sharding {len(all_targets_set)} "
        f"doesn't match unique targets after sharding {num_all_targets}"
    )

    print(
        f"get_targets_for_shard statistics:\n\tOptimum: {optimum}\n"
        + "\n".join(
            (
                f"\tShard {i}: {len(shard)} targets, "
                f"{sum(rule.actual_timeout_s for rule in shard)} time"
            )
            for i, shard in enumerate(shards)
        ),
        file=sys.stderr,
    )
    return [rule.name for rule in shards[index]]


def main():
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
        parser.error("--index must be between 0 and {}".format(args.count - 1))

    if args.sharding_strategy not in ("optimal", "naive"):
        parser.error("--sharding_strategy must be either 'optimal' or 'naive'")

    include_tags, exclude_tags = split_tag_filters(args.tag_filters)

    query = get_target_expansion_query(
        args.targets, args.tests_only, args.exclude_manual, include_tags, exclude_tags
    )
    xml_output = run_bazel_query(query, args.debug)
    rules = extract_rules_from_xml(xml_output)
    rules_grouped_by_time = group_rules_by_time_needed(rules)
    if args.sharding_strategy == "optimal":
        my_targets = get_targets_for_shard_optimal(
            rules_grouped_by_time, args.index, args.count
        )
    else:
        my_targets = get_targets_for_shard_naive(
            rules_grouped_by_time, args.index, args.count
        )
    print(" ".join(my_targets))

    return 0


if __name__ == "__main__":
    sys.exit(main())
