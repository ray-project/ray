import orchpy

class Worker(object):

  def __init__(self):
    self.functions = {}

  def connect(self, scheduler_addr, objstore_addr, worker_addr):
    self.worker = orchpy.lib.create_worker(scheduler_addr, objstore_addr, worker_addr)

  def call(self, name, args):
    return orchpy.lib.remote_call(self.worker, name, args)

  def pull(self, objref):
    pass

  def push(self, objref):
    pass

  def main_loop(self):
    while True:
      call = self.worker.wait_for_next_task()
      name, args, results = orchpy.lib.deserialize_call(call)

	  # TODO: finish this
