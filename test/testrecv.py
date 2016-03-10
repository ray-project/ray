import argparse

import orchpy
import orchpy.services as services
import orchpy.worker as worker

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

def address(host, port):
  return host + ":" + str(port)

if __name__ == '__main__':
  args = parser.parse_args()
  worker.connect(address(args.ip_address, args.scheduler_port), address(args.ip_address, args.objstore_port), address(args.ip_address, args.worker_port))

  worker.global_worker.register_function(print_string)
  worker.global_worker.register_function(handle_int)

  worker.main_loop()
