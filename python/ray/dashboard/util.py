from base64 import b64decode
import datetime
import ray


def to_unix_time(dt):
    return (dt - datetime.datetime(1970, 1, 1)).total_seconds()


def format_reply_id(reply):
    if isinstance(reply, dict):
        for k, v in reply.items():
            if isinstance(v, dict) or isinstance(v, list):
                format_reply_id(v)
            else:
                if k.endswith("Id"):
                    v = b64decode(v)
                    reply[k] = ray.utils.binary_to_hex(v)
    elif isinstance(reply, list):
        for item in reply:
            format_reply_id(item)


def measures_to_dict(measures):
    measures_dict = {}
    for measure in measures:
        tags = measure["tags"].split(",")[-1]
        if "intValue" in measure:
            measures_dict[tags] = measure["intValue"]
        elif "doubleValue" in measure:
            measures_dict[tags] = measure["doubleValue"]
    return measures_dict
