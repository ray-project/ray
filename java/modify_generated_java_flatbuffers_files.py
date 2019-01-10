from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys

"""
This script is used for modifying the generated java flatbuffer files for the reason:
The package declaration in Java is different from python and C++, and there is
no option in the flatc command to specify package(namepsace) for Java specially.

USAGE:
    python modify_generated_java_flatbuffers_file.py RAY_HOME

RAY_HOME: The root directory of Ray project.
"""

# constants declarations
PACKAGE_DECLARATION = "package org.ray.runtime.generated;"


def add_new_line(file, line_num, text):
    with open(file, "r") as file_handler:
        lines = file_handler.readlines()
        if (line_num <= 0) or (line_num > len(lines) + 1):
            return False

    lines.insert(line_num - 1, text + os.linesep)
    with open(file, "w") as file_handler:
        for line in lines:
            file_handler.write(line)

    return True


def add_package_declarations(generated_root_path):
    file_names = os.listdir(generated_root_path)
    for file_name in file_names:
        if not file_name.endswith(".java"):
            continue
        full_name = os.path.join(generated_root_path, file_name)
        success = add_new_line(full_name, 2, PACKAGE_DECLARATION)
        if not success:
            raise RuntimeError("Failed to add package declarations, "
                               "file name is %s" % full_name)


if __name__ == "__main__":
    ray_home = sys.argv[1]
    root_path = os.path.join(
        ray_home,
        "java/runtime/src/main/java/org/ray/runtime/generated")
    add_package_declarations(root_path)
