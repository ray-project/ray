"Used to check bazel output for team's test owner tags"
import sys
import xml.etree.ElementTree as ET


def perform_check(raw_xml_string: str):
    tree = ET.fromstring(raw_xml_string)
    owners = {}
    missing_owners = []
    for rule in tree.findall("rule"):
        test_name = rule.attrib["name"]
        tags = [
            child.attrib["value"] for child in rule.find("list").getchildren()
        ]
        team_owner = [t for t in tags if t.startswith("team")]
        if len(team_owner) == 0:
            missing_owners.append(test_name)
        owners[test_name] = team_owner

    if len(missing_owners):
        raise Exception(
            f"Cannot find ownder for tests {missing_owners}, please add "
            "`team:*` to the tags.")

    print(owners)


if __name__ == "__main__":
    raw_xml_string = sys.stdin.read()
    perform_check(raw_xml_string)
