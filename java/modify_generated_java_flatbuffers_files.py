from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import re
import sys

"""This script is used for modifying the generated java flatbuffer files for 2 reasons:

1) In `gcs.fbs`, some fields are defined as `[string]`. And currently flatbuffers
   doesn't have an API to get a single element in the list as raw bytes.
   See https://github.com/google/flatbuffers/issues/5092 for more details.

2) The package declaration in Java is different from python and C++, and there is
   no option in the flatc command to specify package(namepsace) for Java specially.

USAGE:
    python modify_generated_java_flatbuffers_file.py RAY_HOME

RAY_HOME: The root directory of Ray project.
"""

# constants declarations
PACKAGE_DECLARATION = "package org.ray.runtime.generated;"

TEMPLATE_FOR_BYTE_BUFFER_GETTER = """
  public ByteBuffer %s(int j) {
    int o = __offset(%d);
    if (o == 0) {
      return null;
    }

    int offset = __vector(o) + j * 4;
    offset += bb.getInt(offset);
    ByteBuffer src = bb.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    int length = src.getInt(offset);
    src.position(offset + 4);
    src.limit(offset + 4 + length);
    return src;
  }
"""


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


def get_offset(file, field):
    with open(file, "r") as file_handler:
        lines = file_handler.readlines()

        for line in lines:
            re_str = ".*%s\(int j\) \{ int o = __offset\((.*)\);.*" % field
            results = re.findall(re_str, line)
            if len(results) == 0:
                continue
            elif len(results) == 1:
                return int(results[0])
            else:
                return -1

        return -1


def genrate_byte_buffer_getter(file, field):
    index_to_be_inserted = -1
    with open(file, "r") as file_handler:
        lines = file_handler.readlines()

        for index in range(len(lines) - 1, -1, -1):
            if lines[index] == "}\n":
                index_to_be_inserted = index

    if index_to_be_inserted >= 0:
        offset = get_offset(file, field)
        if offset == -1:
            raise RuntimeError("Failed to get offset: field is %s" % field)

        method_name = "%sAsByteBuffer" % field
        text = TEMPLATE_FOR_BYTE_BUFFER_GETTER % (method_name, offset)
        success = add_new_line(file, index_to_be_inserted + 1, text)

        if not success:
            raise RuntimeError("Failed to generate and insert method, "
                               "file is %s, field is %s." % (file, field))


def modify_generated_java_flatbuffers_files(ray_home, class_and_field_pairs):
    root_path = os.path.join(
        ray_home,
        "java/runtime/src/main/java/org/ray/runtime/generated")
    add_package_declarations(root_path)

    for class_and_field in class_and_field_pairs:
        file_name = os.path.join(root_path, "%s.java" % class_and_field[0])
        genrate_byte_buffer_getter(file_name, class_and_field[1])


if __name__ == "__main__":

    class_and_field_pairs = [
        ("TaskInfo", "returns"),
        ("Arg", "objectIds"),
    ]

    modify_generated_java_flatbuffers_files(sys.argv[1], class_and_field_pairs)
