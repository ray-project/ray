import json
import logging

from pygments import formatters, highlight, lexers

import ray


def _get_logger():
    logger = logging.getLogger("ray.serve")
    # TODO(simon): Make logging level configurable.
    logger.setLevel(logging.INFO)
    return logger


logger = _get_logger()


class BytesEncoder(json.JSONEncoder):
    """Allow bytes to be part of the JSON document.

    BytesEncoder will walk the JSON tree and decode bytes with utf-8 codec.

    Example:
    >>> json.dumps({b'a': b'c'}, cls=BytesEncoder)
    '{"a":"c"}'
    """

    def default(self, o):  # pylint: disable=E0202
        if isinstance(o, bytes):
            return o.decode("utf-8")
        return super().default(o)


def get_custom_object_id():
    """Use ray worker API to get computed ObjectID"""
    worker = ray.worker.global_worker
    object_id = ray._raylet.compute_put_id(worker.current_task_id,
                                           worker.task_context.put_index)
    worker.task_context.put_index += 1
    return object_id


def pformat_color_json(d):
    """Use pygments to pretty format and colroize dictionary"""
    formatted_json = json.dumps(d, sort_keys=True, indent=4)

    colorful_json = highlight(formatted_json, lexers.JsonLexer(),
                              formatters.TerminalFormatter())

    return colorful_json
