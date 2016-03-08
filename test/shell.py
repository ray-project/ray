import orchpy.unison as unison
import orchpy.services as services
import orchpy.worker as worker

from grpc.beta import implementations
import orchestra_pb2
import types_pb2

def connect_to_scheduler(host, port):
  channel = implementations.insecure_channel(host, port)
  return orchestra_pb2.beta_create_Scheduler_stub(channel)

if __name__ == '__main__':
  scheduler_stub = connect_to_scheduler("127.0.0.1", 22221)
  worker = worker.Worker()
  worker.connect("127.0.0.1:22221", "127.0.0.1:10000", "127.0.0.1:22222")
  import IPython
  IPython.embed()
