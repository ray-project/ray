import sys

import orchpy.unison as unison
import orchpy.services as services
import orchpy.worker as worker

@worker.distributed([str], [str])
def print_string(string):
  print "called print_string with", string
  return string

@worker.distributed([int, int], [int, int])
def handle_int(a, b):
    return a+1, b+1

def address(host, port):
  return host + ":" + str(port)

if __name__ == '__main__':
  ip_address = sys.argv[1]
  scheduler_port = sys.argv[2]
  worker_port = sys.argv[3]
  objstore_port = sys.argv[4]

  worker.global_worker.connect(address(ip_address, scheduler_port), address(ip_address, worker_port), address(ip_address, objstore_port))

  worker.global_worker.register_function("hello_world", print_string, 1)
  worker.global_worker.register_function("handle_int", handle_int, 2)
  worker.global_worker.start_worker_service()

  worker.global_worker.main_loop()
