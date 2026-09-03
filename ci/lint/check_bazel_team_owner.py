"""Used to check bazel output for team's test owner tags

The bazel output looks like
<?xml version="1.1" encoding="UTF-8" standalone="no"?>
<query version="2">
    <rule class="cc_test"
          location="/Users/simonmo/Desktop/ray/ray/streaming/BUILD.bazel:312:8"
          name="//streaming:streaming_util_tests"
    >
        <string name="name" value="streaming_util_tests"/>
        <list name="tags">
            <string value="team:ant-group"/>
        </list>
        <list name="deps">
...

"""
import json
import sys
import xml.etree.ElementTree as ET

TEAM_PREFIX = "team:"

# The set of teams a test may be assigned to.
#
# Before adding a team here, make sure something actually runs its tests,
# either a `test_in_docker ... <team>` invocation in .buildkite/*.rayci.yml, or
# a dedicated pipeline script.
VALID_TEAMS = frozenset(
    {
        "core",
        "data",
        "ml",
        "rllib",
        "serve",
        "llm",
        # ci+release tooling, run by .buildkite/cicd.rayci.yml.
        "ci",
        # Run by ci/k8s/*.sh rather than by team-tag query.
        "kuberay",
        # Docs / doctest targets that no product team owns.
        "none",
    }
)


def perform_check(raw_xml_string: str):
    tree = ET.fromstring(raw_xml_string)
    owners = {}
    missing_owners = []
    multiple_owners = []
    unknown_owners = []
    for rule in tree.findall("rule"):
        test_name = rule.attrib["name"]
        location = rule.attrib.get("location", test_name)
        tags = []
        for lst in rule.findall("list"):
            if lst.attrib["name"] != "tags":
                continue
            tags = [child.attrib["value"] for child in lst]
            break
        team_owner = [t for t in tags if t.startswith(TEAM_PREFIX)]
        if len(team_owner) == 0:
            missing_owners.append(location)
        elif len(team_owner) > 1:
            multiple_owners.append(f"{location}: {', '.join(sorted(team_owner))}")
        elif team_owner[0][len(TEAM_PREFIX) :] not in VALID_TEAMS:
            unknown_owners.append(f"{location}: {team_owner[0]}")
        owners[test_name] = team_owner

    errors = []
    if missing_owners:
        errors.append(
            "Cannot find an owner for these tests, please add a `team:*` tag "
            "from the list above:\n  " + "\n  ".join(missing_owners)
        )
    if multiple_owners:
        errors.append(
            "These tests have more than one `team:*` tag. A test must have "
            "exactly one owner, otherwise it runs once per team in CI and is "
            "reported twice on the flaky-test dashboard:\n  "
            + "\n  ".join(multiple_owners)
        )
    if unknown_owners:
        errors.append(
            "These tests have a `team:*` tag that no CI job matches, so they "
            "never run and never report to the flaky-test dashboard. Fix the "
            "tag, or add the team to VALID_TEAMS in "
            "ci/lint/check_bazel_team_owner.py once a pipeline runs it:\n  "
            + "\n  ".join(unknown_owners)
        )
    if errors:
        valid = ", ".join(f"{TEAM_PREFIX}{t}" for t in sorted(VALID_TEAMS))
        raise Exception(f"Valid team tags are: {valid}\n\n" + "\n\n".join(errors))

    print(json.dumps(owners, indent="  "))


if __name__ == "__main__":
    if "--print-teams" in sys.argv[1:]:
        # Lets other lint scripts (ci/lint/check-pytest-format.sh) share this
        # list instead of keeping a second copy that drifts out of sync.
        print("\n".join(sorted(VALID_TEAMS)))
        sys.exit(0)
    raw_xml_string = sys.stdin.read()
    perform_check(raw_xml_string)
