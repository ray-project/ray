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

if __name__ == '__main__':
  worker.global_worker.connect("127.0.0.1:22221", "127.0.0.1:40000", "127.0.0.1:22222")

  worker.global_worker.register_function("hello_world", print_string, 1)
  worker.global_worker.register_function("handle_int", handle_int, 2)
  worker.global_worker.start_worker_service()

  worker.global_worker.main_loop()
