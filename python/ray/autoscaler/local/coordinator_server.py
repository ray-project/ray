"""Web server that runs on local/private clusters to coordinate and manage
different clusters for multiple users. It receives node provider function calls
through HTTP requests from remote CoordinatorSenderNodeProvider and runs them
locally in LocalNodeProvider. To start the webserver the user runs:
`python coordinator_server.py --ips <comma separated ips> --host <HOST> --port <PORT>`."""
import argparse
import json
import logging
import threading
import socket
from http.server import HTTPServer, SimpleHTTPRequestHandler

from ray.autoscaler._private.local.node_provider import LocalNodeProvider
from ray._common.network_utils import build_address

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def runner_handler(node_provider):
    class Handler(SimpleHTTPRequestHandler):
        """A custom handler for OnPremCoordinatorServer.

        Handles all requests and responses coming into and from the
        remote CoordinatorSenderNodeProvider.
        """

        def _do_header(self, response_code=200, headers=None):
            """Sends the header portion of the HTTP response.

            Args:
                response_code: Standard HTTP response code
                headers (list[tuples]): Standard HTTP response headers
            """
            if headers is None:
                headers = [("Content-type", "application/json")]

            self.send_response(response_code)
            for key, value in headers:
                self.send_header(key, value)
            self.end_headers()

        def do_HEAD(self):
            """HTTP HEAD handler method."""
            self._do_header()

        def do_GET(self):
            """Processes requests from remote CoordinatorSenderNodeProvider."""
            if self.headers["content-length"]:
                raw_data = (
                    self.rfile.read(int(self.headers["content-length"]))
                ).decode("utf-8")
                logger.info(
                    "OnPremCoordinatorServer received request: " + str(raw_data)
                )
                request = json.loads(raw_data)
                response = getattr(node_provider, request["type"])(*request["args"])
                logger.info(
                    "OnPremCoordinatorServer response content: " + str(raw_data)
                )
                response_code = 200
                message = json.dumps(response)
                self._do_header(response_code=response_code)
                self.wfile.write(message.encode())

    return Handler


class OnPremCoordinatorServer(threading.Thread):
    """Initializes HTTPServer and serves CoordinatorSenderNodeProvider forever.

    It handles requests from the remote CoordinatorSenderNodeProvider. The
    requests are forwarded to LocalNodeProvider function calls.
    """

    def __init__(self, list_of_node_ips, host, port):
        """Initialize HTTPServer and serve forever by invoking self.run()."""

        logger.info(
            "Running on prem coordinator server on address " + build_address(host, port)
        )
        threading.Thread.__init__(self)
        self._port = port
        self._list_of_node_ips = list_of_node_ips
        address = (host, self._port)
        config = {"list_of_node_ips": list_of_node_ips}
        self._server = HTTPServer(
            address,
            runner_handler(LocalNodeProvider(config, cluster_name=None)),
        )
        self.start()

    def run(self):
        self._server.serve_forever()

    def shutdown(self):
        """Shutdown the underlying server."""
        self._server.shutdown()
        self._server.server_close()


def main():
    parser = argparse.ArgumentParser(
        description="Please provide a list of node ips and port."
    )
    parser.add_argument(
        "--ips", required=True, help="Comma separated list of node ips."
    )
    parser.add_argument(
        "--host",
        type=str,
        required=False,
        help="The Host on which the coordinator listens.",
    )
    parser.add_argument(
        "--port",
        type=int,
        required=True,
        help="The port on which the coordinator listens.",
    )
    args = parser.parse_args()
    host = args.host or socket.gethostbyname(socket.gethostname())
    list_of_node_ips = args.ips.split(",")
    OnPremCoordinatorServer(
        list_of_node_ips=list_of_node_ips,
        host=host,
        port=args.port,
    )


if __name__ == "__main__":
    main()
