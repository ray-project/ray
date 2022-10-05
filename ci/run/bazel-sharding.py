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

# FROM https://github.com/philwo/bazel-utils/blob/main/sharding/sharding.py

import argparse
import os
import subprocess
import sys


def partition_targets(targets):
    included_targets, excluded_targets = [], []
    for target in targets:
        if target.startswith("-"):
            excluded_targets.append(target[1:])
        else:
            included_targets.append(target)
    return included_targets, excluded_targets


def quote_targets(targets):
    return (" ".join("'{}'".format(t) for t in targets)) if targets else ""


def get_target_expansion_query(targets, tests_only, exclude_manual):
    included_targets, excluded_targets = partition_targets(targets)

    included_targets = quote_targets(included_targets)
    excluded_targets = quote_targets(excluded_targets)

    query = "set({})".format(included_targets)
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

    return query


def run_bazel_query(query, debug):
    args = ["bazel", "query", query]
    if debug:
        print("$ {}".format(" ".join(args)), file=sys.stderr)
        sys.stderr.flush()
    p = subprocess.run(
        ["bazel", "query", query],
        check=True,
        stdout=subprocess.PIPE,
        errors="replace",
        universal_newlines=True,
    )
    output = p.stdout.strip()
    return output.splitlines() if output else []


def get_targets_for_shard(targets, index, count):
    # This is a very simple way of sharding targets. A more sophisticated
    # approach might want to take test sizes into account, for example.
    return sorted(targets)[index::count]


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
    parser.add_argument("targets", nargs="+")
    args, extra_args = parser.parse_known_args()
    args.targets = list(args.targets) + list(extra_args)

    if args.index >= args.count:
        parser.error("--index must be between 0 and {}".format(args.count - 1))

    query = get_target_expansion_query(
        args.targets, args.tests_only, args.exclude_manual
    )
    expanded_targets = run_bazel_query(query, args.debug)
    my_targets = get_targets_for_shard(expanded_targets, args.index, args.count)
    print(" ".join(my_targets))

    return 0


if __name__ == "__main__":
    sys.exit(main())
