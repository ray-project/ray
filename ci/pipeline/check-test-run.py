"""Make sure tests will be run by CI.
"""

import glob
import subprocess
import xml.etree.ElementTree as ET

if __name__ == "__main__":
    # Make sure python unit tests have corresponding bazel targets that will run them.
    xml_string = subprocess.run(
        ["bazel", "query", 'kind("py_test", //...)', "--output=xml"],
        stdout=subprocess.PIPE,
    ).stdout.decode("utf-8")
    root_element = ET.fromstring(xml_string)
    src_files = set()
    for src_element in root_element.findall(".//*[@name='srcs']/label"):
        src_file = src_element.attrib["value"][2:].replace(":", "/")
        src_files.add(src_file)

    missing_bazel_targets = []
    for f in glob.glob("python/**/tests/test_*.py", recursive=True):
        if f.startswith("python/build/") or f.startswith(
            "python/ray/thirdparty_files/"
        ):
            continue
        # TODO(jiaodong) Remove this once experimental module is tested
        if f.startswith("python/ray/experimental"):
            continue
        if f not in src_files:
            missing_bazel_targets.append(f)

    if missing_bazel_targets:
        raise Exception(
            f"Cannot find bazel targets for tests {missing_bazel_targets} "
            f"so they won't be run automatically by CI, "
            f"please add them to BUILD files."
        )
