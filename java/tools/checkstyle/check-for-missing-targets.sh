#!/usr/bin/env bash

# This script verifies that all Java targets except those having assigned `checkstyle_ignore` tag
# do have a accompanying Checkstyle target

# [1] Get all Java targets except those having assigned `checkstyle_ignore` tag
bazel query 'kind(java_*, //...) - kind(java_proto_library, //...) - //third_party/... - attr("tags", "checkstyle_ignore", //...)' | sort >/tmp/all_java_targets
# [2] Get all checkstyle targets' names, replace '-checkstyle' with nothing so it matches Java target name
bazel query 'kind(checkstyle_test, //...)' | sed -e 's|-checkstyle||g' | sort >/tmp/checkstyle_covered_java_targets

# [3] Targets in [1] which are not in [2]
NON_COVERED_JAVA_TARGETS=$(comm -23 /tmp/all_java_targets /tmp/checkstyle_covered_java_targets)

# Having non-empty [3] is an error
if [[ ! -z "$NON_COVERED_JAVA_TARGETS" ]]; then
  echo "$(tput setaf 1)[!] These java targets do not have accompanying checkstyle targets:"$(tput sgr0)
  echo ${NON_COVERED_JAVA_TARGETS}
  exit 1
fi
