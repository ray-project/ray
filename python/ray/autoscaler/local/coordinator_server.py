import logging
import threading
from http.server import SimpleHTTPRequestHandler, HTTPServer
import json

from ray.autoscaler.local.node_provider import LocalNodeProvider

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def runner_handler(node_provider):
    class Handler(SimpleHTTPRequestHandler):
        """A custom handler for LocalNodeProviderServer.

        Handles all requests and responses coming into and from the
        remote "local" node provider.
        """

        def _do_header(self, response_code=200, headers=None):
            """Sends the header portion of the HTTP response.
            Args:
                response_code (int): Standard HTTP response code
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
            """Processes requests from CoordinatorNodeProvider."""
            if self.headers["content-length"]:
                raw_data = (self.rfile.read(
                    int(self.headers["content-length"]))).decode("utf-8")
                logger.info("OnPremCoordinatorServer received request: " +
                            str(raw_data))
                request = json.loads(raw_data)
                response = getattr(node_provider,
                                   request["type"])(*request["args"])
                logger.info("OnPremCoordinatorServer response content: " +
                            str(raw_data))
                response_code = 200
                message = json.dumps(response)
                self._do_header(response_code=response_code)
                self.wfile.write(message.encode())

    return Handler


class OnPremCoordinatorServer(threading.Thread):
    """Initializes the HTTPServer and serves the CoordinatorNodeProvider forever.

    It handles requests and responses from the remote CoordinatorNodeProvider.
    """

    def __init__(self, list_of_node_ips, host, port):
        """Initialize HTTPServer and serve forever by invoking self.run()."""

        logger.info("Running on prem coordinator server on address " + host +
                    ":" + str(port))
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
