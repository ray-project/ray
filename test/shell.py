import argparse

import orchpy.services as services
import orchpy.worker as worker

from grpc.beta import implementations
import orchestra_pb2
import types_pb2

parser = argparse.ArgumentParser(description='Parse addresses for the worker to connect to.')
parser.add_argument("--ip_address", default="127.0.0.1", help="the IP address to use for both the scheduler and objstore")
parser.add_argument("--scheduler_port", default=10001, type=int, help="the scheduler's port")
parser.add_argument("--objstore_port", default=20001, type=int, help="the objstore's port")
parser.add_argument("--worker_port", default=40001, type=int, help="the worker's port")

@worker.distributed([str], [str])
def print_string(string):
  print "called print_string with", string
  f = open("asdfasdf.txt", "w")
  f.write("successfully called print_string with argument {}.".format(string))
  return string

@worker.distributed([int, int], [int, int])
def handle_int(a, b):
  return a + 1, b + 1

def connect_to_scheduler(host, port):
  channel = implementations.insecure_channel(host, port)
  return orchestra_pb2.beta_create_Scheduler_stub(channel)

def address(host, port):
  return host + ":" + str(port)

if __name__ == '__main__':
  args = parser.parse_args()
  scheduler_stub = connect_to_scheduler(args.ip_address, args.scheduler_port)
  worker.connect(address(args.ip_address, args.scheduler_port), address(args.ip_address, args.objstore_port), address(args.ip_address, args.worker_port))
  import IPython
  IPython.embed()
