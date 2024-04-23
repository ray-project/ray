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
import sys
import xml.etree.ElementTree as ET


def perform_check(raw_xml_string: str):
    tree = ET.fromstring(raw_xml_string)
    owners = {}
    missing_owners = []
    for rule in tree.findall("rule"):
        test_name = rule.attrib["name"]
        tags = []
        for lst in rule.findall("list"):
            if lst.attrib["name"] != "tags":
                continue
            tags = [child.attrib["value"] for child in lst.getchildren()]
            break
        team_owner = [t for t in tags if t.startswith("team:")]
        if len(team_owner) == 0:
            missing_owners.append(test_name)
        owners[test_name] = team_owner

    if len(missing_owners):
        raise Exception(
            f"Cannot find owner for tests {missing_owners}, please add "
            "`team:*` to the tags."
        )

    print(owners)


if __name__ == "__main__":
    raw_xml_string = sys.stdin.read()
    perform_check(raw_xml_string)
