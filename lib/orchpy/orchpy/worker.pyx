cdef extern void* orch_create_context(const char* server_addr);
cdef extern size_t orch_remote_call(void* context, const char* name, void* args);

cdef class Worker:
  cdef void* context

  def __cinit__(self):
    self.context = NULL

  def connect(self, server_addr):
    self.context = orch_create_context(server_addr)

  def call(self, name):
    return orch_remote_call(self.context, name, <void*>0)

global_worker = Worker()

def pull(objref, worker=global_worker):
  return 1
