import orchpy.unison as unison
import orchpy.services as services
import orchpy.worker as worker
import sys

if __name__ == '__main__':
  worker = worker.Worker()
  worker.connect("127.0.0.1:22221", "127.0.0.1:50000", "127.0.0.1:22222")
  worker.start_worker_service()
  worker.register_function("hello_world", None, 0)
  name, args, returnref = worker.wait_for_next_task()
  print "received args ", args
  if args == ["hi"]:
    sys.exit(0)
  else:
    sys.exit(1)
