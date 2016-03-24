import argparse

import orchpy
import orchpy.services as services
import orchpy.worker as worker

import arrays.single as single
import arrays.dist as dist

from grpc.beta import implementations
import orchestra_pb2
import types_pb2

TIMEOUT_SECONDS = 5

parser = argparse.ArgumentParser(description='Parse addresses for the worker to connect to.')
parser.add_argument("--scheduler-address", default="127.0.0.1:10001", type=str, help="the scheduler's address")
parser.add_argument("--objstore-address", default="127.0.0.1:20001", type=str, help="the objstore's address")
parser.add_argument("--worker-address", default="127.0.0.1:30001", type=str, help="the worker's address")

@orchpy.distributed([str], [str])
def print_string(string):
  print "called print_string with", string
  f = open("asdfasdf.txt", "w")
  f.write("successfully called print_string with argument {}.".format(string))
  return string

@orchpy.distributed([int, int], [int, int])
def handle_int(a, b):
  return a + 1, b + 1

def connect_to_scheduler(host, port):
  channel = implementations.insecure_channel(host, port)
  return orchestra_pb2.beta_create_Scheduler_stub(channel)

def connect_to_objstore(host, port):
  channel = implementations.insecure_channel(host, port)
  return orchestra_pb2.beta_create_ObjStore_stub(channel)

if __name__ == '__main__':
  args = parser.parse_args()
  scheduler_ip_address, scheduler_port = args.scheduler_address.split(":")
  scheduler_stub = connect_to_scheduler(scheduler_ip_address, int(scheduler_port))
  objstore_ip_address, objstore_port = args.objstore_address.split(":")
  objstore_stub = connect_to_objstore(objstore_ip_address, int(objstore_port))
  worker.connect(args.scheduler_address, args.objstore_address, args.worker_address)

  def scheduler_debug_info():
    return scheduler_stub.SchedulerDebugInfo(orchestra_pb2.SchedulerDebugInfoRequest(), TIMEOUT_SECONDS)

  def objstore_debug_info():
    return objstore_stub.ObjStoreDebugInfo(orchestra_pb2.ObjStoreDebugInfoRequest(), TIMEOUT_SECONDS)

  import IPython
  IPython.embed()
