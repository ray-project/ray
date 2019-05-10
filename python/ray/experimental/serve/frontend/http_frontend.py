from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time


import ray


def unwrap(future):
    """Unwrap the result from ray.experimental.server router.
    Router returns a list of object ids when you call them.
    """

    return ray.get(future)[0]


@ray.remote
class HTTPFrontendActor:
    """HTTP API for an Actor. This exposes /{actor_name} endpoint for query.

    Request:
        GET /{actor_name} or POST /{actor_name}
        Content-type: application/json
        {
            "slo_ms": float,
            "input": any
        }
    Response:
        Content-type: application/json
        {
            "success": bool,
            "actor": str,
            "result": any
        }
    """

    def __init__(self, ip="0.0.0.0", port=8080, router="DefaultRouter"):
        self.ip = ip
        self.port = port
        self.router = ray.experimental.named_actors.get_actor(router)

    def start(self):
        # We have to import flask here to avoid Flask's
        # "Working outside of request context." error
        from flask import Flask, request, jsonify # noqa: E402
        default_app = Flask(__name__)

        @default_app.route("/<actor_name>", methods=["GET", "POST"])
        def dispatch_remote_function(actor_name):
            data = request.get_json()

            slo_seconds = data.pop("slo_ms") / 1000
            deadline = time.perf_counter() + slo_seconds

            inp = data.pop("input")

            result_future = unwrap(
                self.router.call.remote(actor_name, inp, deadline))
            result = ray.get(result_future)

            return jsonify({
                "success": True,
                "actor": actor_name,
                "result": result
            })

        default_app.run(host=self.ip, port=self.port)
