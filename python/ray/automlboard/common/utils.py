import logging
import json
import os
import time


def dump_json(json_info, json_file, overwrite=True):
    """
    dumps a whole json record into the given file. Overwrite the
    file if the overwrite flag set.

    Args:
        json_info(dict)
        json_file(str)
        overwrite(boolean)
    """
    if overwrite:
        mode = 'w'
    else:
        mode = 'w+'

    try:
        with open(json_file, mode) as f:
            f.write(json.dumps(json_info))
    except StandardError, e:
        logging.error(e.message)


def parse_json(json_file):
    """
    parse a whole json record from the given file, return None if
    the json file does not exists or exception occurs.

    Args:
        json_file(str)

    Returns:
        a dict of json info
    """
    if not os.path.exists(json_file):
        return None

    try:
        with open(json_file, 'r') as f:
            info_str = f.readlines()
            info_str = "".join(info_str)
            json_info = json.loads(info_str)
            if isinstance(json_info, unicode):
                json_info = eval(str(json_info))
            return json_info
    except StandardError, e:
        logging.error(e.message)
        return None


def parse_multiple_json(json_file, offset=None):
    """
    parse multiple json records from the given file. Seek to the offset
    as the start point before parsing if offset set. return empty list
    if the json file does not exists or exception occurs.

    Args:
        json_file(str)
        offset(int)

    Returns:
        a dict of json info
        new offset after parsing
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
                if isinstance(json_info, unicode):
                    json_info = eval(str(json_info))
                json_info_list.append(json_info)
                offset += len(line)
    except StandardError, e:
        logging.error(e.message)

    return json_info_list, offset


def timestamp2date(timestamp):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
