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
  return a + 1, b + 1

if __name__ == '__main__':
  ip_address = sys.argv[1]
  scheduler_port = sys.argv[2]
  worker_port = sys.argv[3]
  objstore_port = sys.argv[4]

  def address(host, port):
    return host + ":" + str(port)

  worker = worker.Worker()
  worker.connect(address(ip_address, scheduler_port), address(ip_address, worker_port), address(ip_address, objstore_port))
  worker.start_worker_service()

  worker.register_function("print_string", print_string, 0)
  worker.register_function("handle_int", handle_int, 0)

  name, args, returnref = worker.wait_for_next_task()
  print "received args ", args
  if args == ["hi"]:
    sys.exit(0)
  else:
    sys.exit(1)
