import argparse

import orchpy.services as services
import orchpy.worker as worker

from grpc.beta import implementations
import orchestra_pb2
import types_pb2

parser = argparse.ArgumentParser(description='Parse addresses for the worker to connect to.')
parser.add_argument("--scheduler-address", default="127.0.0.1:10001", type=str, help="the scheduler's address")
parser.add_argument("--objstore-address", default="127.0.0.1:20001", type=str, help="the objstore's address")
parser.add_argument("--worker-address", default="127.0.0.1:40001", type=str, help="the worker's address")

@worker.distributed([str], [str])
def print_string(string):
  print "called print_string with", string
  f = open("asdfasdf.txt", "w")
  f.write("successfully called print_string with argument {}.".format(string))
  return string

@worker.distributed([int, int], [int, int])
def handle_int(a, b):
  return a + 1, b + 1

def connect_to_scheduler(address):
  channel = implementations.insecure_channel(address)
  return orchestra_pb2.beta_create_Scheduler_stub(channel)

def address(host, port):
  return host + ":" + str(port)

if __name__ == '__main__':
  args = parser.parse_args()
  scheduler_stub = connect_to_scheduler(args.scheduler_address)
  worker.connect(args.scheduler_address, args.objstore_address, args.worker_address))
  import IPython
  IPython.embed()
