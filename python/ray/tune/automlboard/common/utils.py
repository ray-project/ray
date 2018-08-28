from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import json
import os
import time


def dump_json(json_info, json_file, overwrite=True):
    """Dump a whole json record into the given file.

    Overwrite the file if the overwrite flag set.

    Args:
        json_info (dict): Information dict to be dumped.
        json_file (str): File path to be dumped to.
        overwrite(boolean)
    """
    if overwrite:
        mode = 'w'
    else:
        mode = 'w+'

    try:
        with open(json_file, mode) as f:
            f.write(json.dumps(json_info))
    except BaseException as e:
        logging.error(e.message)


def parse_json(json_file):
    """Parse a whole json record from the given file.

    Return None if the json file does not exists or exception occurs.

    Args:
        json_file (str): File path to be parsed.

    Returns:
        A dict of json info.
    """
    if not os.path.exists(json_file):
        return None

    try:
        with open(json_file, 'r') as f:
            info_str = f.readlines()
            info_str = "".join(info_str)
            json_info = json.loads(info_str)
            return unicode2str(json_info)
    except BaseException as e:
        logging.error(e.message)
        return None


def parse_multiple_json(json_file, offset=None):
    """Parse multiple json records from the given file.

    Seek to the offset as the start point before parsing
    if offset set. return empty list if the json file does
    not exists or exception occurs.

    Args:
        json_file (str): File path to be parsed.
        offset (int): Initial seek position of the file.

    Returns:
        A dict of json info.
        New offset after parsing.

    """
    json_info_list = []
    if not os.path.exists(json_file):
        return json_info_list

    try:
        with open(json_file, 'r') as f:
            if offset:
                f.seek(offset)
            for line in f:
                if line[-1] != '\n':
                    # Incomplete line
                    break
                json_info = json.loads(line)
                json_info_list.append(json_info)
                offset += len(line)
    except BaseException as e:
        logging.error(e.message)

    return json_info_list, offset


def timestamp2date(timestamp):
    """Convert a timestamp to date."""
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))


def unicode2str(content):
    """Convert the unicode element of the content to str recursively."""
    if isinstance(content, dict):
        result = {}
        for key in content.keys():
            result[unicode2str(key)] = unicode2str(content[key])
        return result
    elif isinstance(content, list):
        return [unicode2str(element) for element in content]
    elif isinstance(content, int) or isinstance(content, float):
        return content
    else:
        return content.encode('utf-8')
