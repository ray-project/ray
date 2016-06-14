import argparse
from ray.services import start_node
import time

parser = argparse.ArgumentParser(description="Starting workers on a node of the cluster (invoked locally on the node).")
parser.add_argument("--scheduler-address", type=str, help="Address of the scheduler running on the head node (ip + port).")
parser.add_argument("--node-ip", type=str, help="IP address of the current worker.")
parser.add_argument("--num-workers", type=int, default=20, help="Number of workers to be started on the node.")
parser.add_argument("--worker-path", type=str, help="Path to the worker file.")

if __name__ == "__main__":
  args = parser.parse_args()
  start_node(args.scheduler_address, args.node_ip, args.num_workers, worker_path=args.worker_path)

  time.sleep(1000000000) # TODO(pcm): Figure out why object store file handle is closed if we don't do this
