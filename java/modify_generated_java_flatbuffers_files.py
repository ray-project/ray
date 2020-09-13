import os
import sys
"""
This script is used for modifying the generated java flatbuffer
files for the reason: The package declaration in Java is different
from python and C++, and there is no option in the flatc command
to specify package(namepsace) for Java specially.

USAGE:
    python modify_generated_java_flatbuffers_file.py RAY_HOME

RAY_HOME: The root directory of Ray project.
"""

# constants declarations
PACKAGE_DECLARATION = "package io.ray.runtime.generated;"


def add_package(file):
    with open(file, "r") as file_handler:
        lines = file_handler.readlines()

    if "FlatBuffers" not in lines[0]:
        return

    lines.insert(1, PACKAGE_DECLARATION + os.linesep)
    with open(file, "w") as file_handler:
        for line in lines:
            file_handler.write(line)


def add_package_declarations(generated_root_path):
    file_names = os.listdir(generated_root_path)
    for file_name in file_names:
        if not file_name.endswith(".java"):
            continue
        full_name = os.path.join(generated_root_path, file_name)
        add_package(full_name)


if __name__ == "__main__":
    ray_home = sys.argv[1]
    root_path = os.path.join(
        ray_home, "java/runtime/src/main/java/io/ray/runtime/generated")
    add_package_declarations(root_path)
