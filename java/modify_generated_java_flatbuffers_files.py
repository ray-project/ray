import os
import re
import sys


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
        if not file_name.endswith('.java'):
            continue
        full_name = generated_root_path + '/' + file_name
        add_new_line(full_name, 2, 'package org.ray.runtime.generated;')


def get_offset(file, field):
    with open(file, "r") as file_handler:
        lines = file_handler.readlines()

        for line in lines:
            re_str = '.*%s\(int j\) \{ int o = __offset\((.*)\);.*' % field
            results = re.findall(re_str, line)
            if len(results) == 0:
                continue
            elif len(results) == 1:
                return int(results[0])
            else:
                return -1

        return -1


template_for_byte_buffer_getter = '''
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
'''


def generate_and_insert_method(file, field, method_name):
    index_to_be_inserted = -1
    with open(file, 'r') as file_handler:
        lines = file_handler.readlines()

        for index in range(len(lines) - 1, -1, -1):
            if lines[index] == '}\n':
                index_to_be_inserted = index

    if index_to_be_inserted >= 0:
        offset = get_offset(file, field)
        if offset == -1:
            raise RuntimeError('Failed to get offset: field is %s' % field)
        text = template_for_byte_buffer_getter % (method_name, offset)
        add_new_line(file, index_to_be_inserted + 1, text)


def modify_generated_java_flatbuffers_files(ray_home, tuples):
    root_path = '%s/java/runtime/src/main/java/org/ray/runtime/generated' % ray_home
    add_package_declarations(root_path)

    for tuple in tuples:
        file_name = '%s/%s.java' % (root_path, tuple[0])
        generate_and_insert_method(file_name,
                                   tuple[1],
                                   tuple[2])


if __name__ == '__main__':

    tuples = [
        ('TaskInfo', 'returns', 'returnsAsByteBuffer'),
        ('Arg', 'objectIds', 'objectIdsAsByteBuffer'),
    ]

    modify_generated_java_flatbuffers_files(sys.argv[1], tuples)
