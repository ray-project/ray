from collections import defaultdict
from typing import Dict

import grpc

_in_flight_requests: Dict[str, Dict[str, grpc.aio.Call]] = defaultdict(dict)


def _get_in_flight_requests(parent_request_id):
    if parent_request_id in _in_flight_requests:
        return _in_flight_requests[parent_request_id]

    return {}


def _add_in_flight_request(parent_request_id, response_id, call):
    _in_flight_requests[parent_request_id][response_id] = call


def _remove_in_flight_request(parent_request_id, response_id):
    if response_id in _in_flight_requests[parent_request_id]:
        del _in_flight_requests[parent_request_id][response_id]

    if len(_in_flight_requests[parent_request_id]) == 0:
        del _in_flight_requests[parent_request_id]
