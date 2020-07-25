import argparse
import socket

from ray.autoscaler.local.coordinator_server import OnPremCoordinatorServer

parser = argparse.ArgumentParser(
    description="Please provide a list of node ips and port.")
parser.add_argument(
    "--ips", required=True, help="Comma separated list of node ips.")
parser.add_argument(
    "--port",
    type=int,
    required=True,
    help="The port on which the coordinator listens.")
args = parser.parse_args()
list_of_node_ips = args.ips.split(",")
OnPremCoordinatorServer(
    list_of_node_ips=list_of_node_ips,
    host=socket.gethostbyname(socket.gethostname()),
    port=args.port,
)
