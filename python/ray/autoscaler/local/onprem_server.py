import logging
import threading
from http.server import SimpleHTTPRequestHandler, HTTPServer
import yaml
import json

logger = logging.getLogger(__name__)


def runner_handler(node_scheduler):
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
            """Processes requests from local node providers."""
            if self.headers["content-length"]:
                raw_data = (self.rfile.read(
                    int(self.headers["content-length"]))).decode("utf-8")
                logger.info("I got a response, content: " + str(raw_data))
                request = json.loads(raw_data)
                if request["request_type"] == "get_node_ips":
                    response = node_scheduler.get_node_ips(
                        request["num_workers"], request["cluster_name"])
                elif request["request_type"] == "release_cluster":
                    response = node_scheduler.release_cluster(
                        request["cluster_name"])
                elif request["request_type"] == "still_valid_cluster":
                    response = node_scheduler.still_valid_cluster(
                        request["cluster_name"], request["provider_config"])
                elif request["request_type"] == "get_status":
                    response = node_scheduler.get_status()
                else:
                    logger.error("LocalNodeProviderServer does not support" +
                                 " request of type: " +
                                 request["request_type"] + ".")
            response_code = 200
            message = json.dumps(response)
            self._do_header(response_code=response_code)
            self.wfile.write(message.encode())

    return Handler


class LocalNodeProviderServer(threading.Thread):
    """Initializes the HTTPServer and serves the local node provider forever.

    It handles requests and responses from the remote local node provider.
    """

    def __init__(self, on_prem_server_config_path):
        """Initialize HTTPServer and serve forever by invoking self.run()."""

        host, port, node_ips = self.get_and_validate_config(
            on_prem_server_config_path)
        logger.info("Running on prem server on address " + host + ":" +
                    str(port))
        threading.Thread.__init__(self)
        self._port = port
        self._node_ips = node_ips
        address = (host, self._port)
        self._server = HTTPServer(address,
                                  runner_handler(NodeScheduler(node_ips)))
        self.start()

    def run(self):
        self._server.serve_forever()

    def shutdown(self):
        """Shutdown the underlying server."""
        self._server.shutdown()
        self._server.server_close()

    @staticmethod
    def get_and_validate_config(on_prem_server_config_path):
        """Parse the on_prem_server_config_path and validate it."""

        config = yaml.safe_load(open(on_prem_server_config_path).read())
        if sorted(config.keys()) != ["list_of_node_ips", "server_address"]:
            raise ValueError("on_prem_server_config_path should only include" +
                             " \"server_address\" and \"list_of_node_ips\".")
        try:
            host, port = config["server_address"].split(":")
            port = int(port)
        except ValueError:
            raise ValueError(
                "server_address should include host (str) and port (int)." +
                " E.g., 127.0.0.1:1234")
        return host, port, config["list_of_node_ips"]


class NodeScheduler:
    """Handles all the scheduling HTTP requests from "local" provider.

    Args:
        node_ips(List[str]): list of node ips that can be scheduled.
    """

    def __init__(self, node_ips):
        self.total_node_ips = node_ips
        self.available_node_ips = list(node_ips)
        self.cached_clusters = {}

    def get_node_ips(self, num_workers, cluster_name):
        """Get cluster worker and head node ips.

        Called to get the node ips when starting a new cluster.
        """

        if cluster_name in self.cached_clusters:
            num_cached_workers = len(
                self.cached_clusters[cluster_name]["worker_ips"])
            if num_workers == num_cached_workers:
                return self.cached_clusters[cluster_name]
            # Ameer: relevant when calling ray up, then another ray up with
            # less workers without calling ray down in the middle.
            elif num_workers < num_cached_workers:
                num_workers_to_delete = num_cached_workers - num_workers
                self.available_node_ips.extend(self.cached_clusters[
                    cluster_name]["worker_ips"][0:num_workers_to_delete])
                del self.cached_clusters[cluster_name]["worker_ips"][
                    0:num_workers_to_delete]
                return self.cached_clusters[cluster_name]
            # Ameer: relevant when calling ray up, then another ray up with
            # more workers without calling ray down in the middle.
            elif num_workers > num_cached_workers:
                num_workers_to_add = num_workers - num_cached_workers
                if len(self.available_node_ips) >= num_workers_to_add:
                    self.cached_clusters[cluster_name]["worker_ips"].extend(
                        self.available_node_ips[0:num_workers_to_add])
                    del self.available_node_ips[0:num_workers_to_add]
                    return self.cached_clusters[cluster_name]
                else:
                    # Not enough available nodes.
                    logger.error("The requsted number of additional nodes" +
                                 " is not available.")
                    return {}
        elif len(self.available_node_ips) < num_workers + 1:
            # Not enough available nodes.
            logger.error("The requsted number of nodes is not available.")
            return {}
        else:
            requested_node_ips = {"head_ip": self.available_node_ips.pop()}
            requested_node_ips["worker_ips"] = self.available_node_ips[
                0:num_workers]
            del self.available_node_ips[0:num_workers]
            self.cached_clusters[cluster_name] = requested_node_ips
            return requested_node_ips

    def release_cluster(self, cluster_name):
        """Releases the cluster node ips.

        Called when the cluster terminates its head node to free the node ips.
        """

        if cluster_name in self.cached_clusters:
            self.available_node_ips.append(
                self.cached_clusters[cluster_name]["head_ip"])
            self.available_node_ips.extend(
                self.cached_clusters[cluster_name]["worker_ips"])
            del self.cached_clusters[cluster_name]
        else:
            logger.error("Cluster " + cluster_name + " not cached.")

    def still_valid_cluster(self, cluster_name, provider_config):
        """Validates that the on prem server is aware of the cluster.

        Called before every node provider function call to verify that
        the cluster is still valid. This is important for synchronization
        when the on prem server crashes/gets killed.
        """

        if cluster_name in self.cached_clusters:
            return True
        else:
            logger.info("Trying to recover cluster " + cluster_name + " ...")
            # If the cluster is not valid but can be recovered, we recover
            # the head_ip and worker_ips
            if provider_config["head_ip"] in self.available_node_ips and set(
                    provider_config["worker_ips"]) <= set(
                        self.available_node_ips):
                self.available_node_ips.remove(provider_config["head_ip"])
                self.available_node_ips = list(
                    set(self.available_node_ips) -
                    set(provider_config["worker_ips"]))
                requested_node_ips = {
                    "head_ip": provider_config["head_ip"],
                    "worker_ips": provider_config["worker_ips"],
                }
                self.cached_clusters[cluster_name] = requested_node_ips
                logger.info("Successfully recovered cluster " + cluster_name +
                            " as the node ips are stil available.")
                return True
            else:
                logger.error("Could not recover cluster: " + cluster_name +
                             " as the node ips are no longer available.")
                return False

    def get_status(self):
        """Returns the server's available and total ips, and cached clusters.

        Useful for debugging and testing.
        """
        return (
            self.available_node_ips,
            self.total_node_ips,
            self.cached_clusters,
        )
