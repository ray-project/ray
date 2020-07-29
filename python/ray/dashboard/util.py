import datetime
import random
import socket

from base64 import b64decode

import ray


def to_unix_time(dt):
    return (dt - datetime.datetime(1970, 1, 1)).total_seconds()


def round_resource_value(quantity):
    if quantity.is_integer():
        return int(quantity)
    else:
        return round(quantity, 2)


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


def format_resource(resource_name, quantity):
    if resource_name == "object_store_memory" or resource_name == "memory":
        # Convert to 50MiB chunks and then to GiB
        quantity = quantity * (50 * 1024 * 1024) / (1024 * 1024 * 1024)
        return "{} GiB".format(round_resource_value(quantity))
    return "{}".format(round_resource_value(quantity))


def measures_to_dict(measures):
    measures_dict = {}
    for measure in measures:
        tags = measure["tags"].split(",")[-1]
        if "intValue" in measure:
            measures_dict[tags] = measure["intValue"]
        elif "doubleValue" in measure:
            measures_dict[tags] = measure["doubleValue"]
    return measures_dict


def get_unused_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    port = s.getsockname()[1]

    # Try to generate a port that is far above the 'next available' one.
    # This solves issue #8254 where GRPC fails because the port assigned
    # from this method has been used by a different process.
    for _ in range(30):
        new_port = random.randint(port, 65535)
        new_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            new_s.bind(("", new_port))
        except OSError:
            new_s.close()
            continue
        s.close()
        new_s.close()
        return new_port
    print("Unable to succeed in selecting a random port.")
    s.close()
    return port
