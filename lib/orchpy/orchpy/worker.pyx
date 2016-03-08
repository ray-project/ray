from libc.stdint cimport uintptr_t
import orchpy.unison as unison

from libc.stdint cimport uint64_t, int64_t, uintptr_t
from libcpp cimport bool
from libcpp.string cimport string

try:
  import cPickle as pickle
except:
  import pickle

cdef struct Slice:
  char* ptr
  size_t size

cdef extern void* orch_create_context(const char* server_addr, const char* worker_addr, const char* objstore_addr)
cdef extern void orch_start_worker_service(void* worker)
cdef extern void* orch_wait_for_next_task(void* worker)
cdef extern void orch_register_function(void* worker, const char* name, size_t num_return_vals)
cdef extern size_t orch_remote_call(void* worker, void* request)
cdef extern size_t orch_push(void* worker, void* value)
cdef extern Slice orch_get_serialized_obj(void* worker, size_t objref)
cdef extern void orch_put_obj(void* worker, size_t objref, void* obj)

cdef extern from "Python.h":
    Py_ssize_t PyByteArray_GET_SIZE(object array)
    object PyUnicode_FromStringAndSize(char *buff, Py_ssize_t len)
    object PyBytes_FromStringAndSize(char *buff, Py_ssize_t len)
    object PyString_FromStringAndSize(char *buff, Py_ssize_t len)
    int PyByteArray_Resize(object self, Py_ssize_t size) except -1
    char* PyByteArray_AS_STRING(object bytearray)

cdef extern from "../../../build/generated/orchestra.pb.h":
  cdef cppclass RemoteCallRequest:
    RemoteCallRequest()
    Call* mutable_call()
    int arg_size() const;

cdef extern from "../../../build/generated/types.pb.h":
  cdef cppclass Values

  cdef cppclass Call:
    Value* add_arg();
    void set_name(const char* value)
    const string& name()
    Value* mutable_arg(int index);
    size_t result(int index);
    int result_size();
    int arg_size() const;

  ctypedef enum DataType:
    INT32
    INT64
    FLOAT32
    FLOAT64

  cdef cppclass Value:
    Value()
    void set_ref(uint64_t value)
    uint64_t ref()
    bool has_obj()
    Obj* mutable_obj()

  cdef cppclass Values:
    Values()
    int value_size()
    Value* add_value()
    Value* mutable_value(int index)


  cdef cppclass String:
    String()
    void set_data(const char* val)
    string* mutable_data()

  cdef cppclass Int:
    Int()
    void set_data(int64_t val)
    int64_t data()

  cdef cppclass Double:
    Double()
    void set_data(double val)
    double data()

  cdef cppclass PyObj:
    PyObj()
    void set_data(const char* val, size_t len)
    string* mutable_data()

  cdef cppclass Obj:
    Obj()
    String* mutable_string_data()
    Int* mutable_int_data()
    Double* mutable_double_data()
    PyObj* mutable_pyobj_data()
    bool has_string_data()
    bool has_int_data()
    bool has_double_data()

cdef serialize_into(val, Obj* obj):
  cdef String* string_data
  cdef Int* int_data
  cdef Double* double_data
  if type(val) == str:
    string_data = obj[0].mutable_string_data()
    string_data[0].set_data(val)
  elif type(val) == int or type(val) == long:
    int_data = obj[0].mutable_int_data()
    int_data[0].set_data(val)
  elif type(val) == float:
    double_data = obj[0].mutable_double_data()
    double_data[0].set_data(val)
  # else:
  #   data = pickle.dumps(val, pickle.HIGHEST_PROTOCOL)
  #   pyobj_data = obj[0].mutable_pyobj_data()
  #   pyobj_data[0].set_data(data, len(data))

"""
cpdef deserialize_call(PyValues args):
  cdef Values* vals = args.thisptr
  cdef Value* val
  cdef Obj* obj
  result = []
  for i in range(vals[0].value_size()):
    val = vals[0].mutable_value(i)
    if not val.has_obj():
      result.append(ObjRef(val.ref(), None)) # TODO: fix this
    else:
      obj = val[0].mutable_obj()
      if obj[0].has_string_data():
        result.append(obj[0].mutable_string_data()[0].mutable_data()[0])
      elif obj[0].has_int_data():
        result.append(obj[0].mutable_int_data()[0].data())
      elif obj[0].has_double_data():
        result.append(obj[0].mutable_double_data()[0].data())
      else:
        data = obj[0].mutable_pyobj_data()[0].mutable_data()[0]
        result.append(pickle.loads(data))
  return result
"""

cdef class ObjWrapper: # TODO: unify with the above
  cdef Obj *thisptr
  def __cinit__(self):
    self.thisptr = new Obj()
  # def __dealloc__(self):
  #   del self.thisptr
  def get_value(self):
    return <uintptr_t>self.thisptr

cpdef serialize_into_2(val, objptr):
  cdef uintptr_t ptr = <uintptr_t>objptr
  cdef Obj* obj = <Obj*>ptr
  cdef String* string_data
  cdef Int* int_data
  cdef Double* double_data
  if type(val) == str:
    string_data = obj[0].mutable_string_data()
    string_data[0].set_data(val)
  elif type(val) == int or type(val) == long:
    int_data = obj[0].mutable_int_data()
    int_data[0].set_data(val)
  elif type(val) == float:
    double_data = obj[0].mutable_double_data()
    double_data[0].set_data(val)
  # else:
  #   data = pickle.dumps(val, pickle.HIGHEST_PROTOCOL)
  #   pyobj_data = obj[0].mutable_pyobj_data()
  #   pyobj_data[0].set_data(data, len(data))

cdef serialize_args_into_call(args, Call* call):
  cdef Value* val
  cdef Obj* obj
  for arg in args:
    val = call.add_arg()
    if type(arg) == unison.ObjRef:
      val[0].set_ref(arg.get_id())
    else:
      obj = val[0].mutable_obj()
      objptr = <uintptr_t>obj
      unison.serialize_into(arg, objptr)

cdef deserialize_obj(Obj* obj):
  if obj[0].has_string_data():
    return obj[0].mutable_string_data()[0].mutable_data()[0]
  elif obj[0].has_int_data():
    return obj[0].mutable_int_data()[0].data()
  elif obj[0].has_double_data():
    return obj[0].mutable_double_data()[0].data()
  else:
    data = obj[0].mutable_pyobj_data()[0].mutable_data()[0]
    return pickle.loads(data)

# todo: unify with the above, at the moment this is copied
cdef deserialize_args_from_call(Call* call):
  cdef Value* val
  cdef Obj* obj
  result = []
  for i in range(call[0].arg_size()):
    val = call[0].mutable_arg(i)
    if not val.has_obj():
      result.append(unison.ObjRef(val.ref(), None)) # TODO: fix this
    else:
      obj = val[0].mutable_obj()
      if obj[0].has_string_data():
        result.append(obj[0].mutable_string_data()[0].mutable_data()[0])
      elif obj[0].has_int_data():
        result.append(obj[0].mutable_int_data()[0].data())
      elif obj[0].has_double_data():
        result.append(obj[0].mutable_double_data()[0].data())
      else:
        data = obj[0].mutable_pyobj_data()[0].mutable_data()[0]
        result.append(pickle.loads(data))
  return result

cdef class Worker:
  cdef void* context
  cdef dict functions

  def __cinit__(self):
    self.context = NULL
    self.functions = {}

  def connect(self, server_addr, worker_addr, objstore_addr):
    self.context = orch_create_context(server_addr, worker_addr, objstore_addr)

  def start_worker_service(self):
    orch_start_worker_service(self.context)

  cpdef call(self, name, args):
    cdef RemoteCallRequest* result = new RemoteCallRequest()
    cdef Call* call = result[0].mutable_call()
    call.set_name(name)
    serialize_args_into_call(args, call)
    orch_remote_call(self.context, result)
   # return <uintptr_t>result

  cpdef do_call(self, ptr):
    return orch_remote_call(self.context, <void*>ptr)

  cpdef push(self, val):
    result = unison.serialize(val)
    ptr = <uintptr_t>result.get_value()
    return unison.ObjRef(orch_push(self.context, <void*>ptr), None)

  cpdef get_serialized(self, objref):
    cdef Slice slice = orch_get_serialized_obj(self.context, objref)
    data = PyBytes_FromStringAndSize(slice.ptr, slice.size)
    return data

  cpdef put_obj(self, objref, obj):
    result = unison.serialize(obj)
    p = <uintptr_t>result.get_value()
    cdef void* ptr = <void*>p
    print "before put"
    orch_put_obj(self.context, objref, <void*>ptr)
    print "after put"

  cpdef do_pull(self, objref):
    cdef Slice slice = orch_get_serialized_obj(self.context, objref)

  cpdef pull(self, objref):
    print "before get_serialized_obj, getting", objref.get_id()
    cdef Slice slice = orch_get_serialized_obj(self.context, <size_t>objref.get_id())
    print "after get_serialized_ob"
    data = PyBytes_FromStringAndSize(slice.ptr, slice.size)
    print "after get data"
    return unison.deserialize_from_string(data)

  cpdef register_function(self, func_name, function, num_args):
    orch_register_function(self.context, func_name, num_args)
    self.functions[func_name] = function

  cpdef wait_for_next_task(self):
    result = []
    cdef Call* call = <Call*>orch_wait_for_next_task(self.context)
    cdef int size = call[0].arg_size()
    cdef Obj* obj
    print "hello from python"
    print "size", size
    args = deserialize_args_from_call(call)
    print "done deserializing"
    returnrefs = []
    for i in range(call[0].result_size()):
      returnrefs.append(call[0].result(i))
    return call[0].name(), args, returnrefs

  cpdef invoke_function(self, name, args):
    return self.functions[name].executor(args)

  cpdef main_loop(self):
    while True:
      name, args, returnrefs = self.wait_for_next_task()
      print "got returnref", returnrefs
      result = self.functions[name].executor(args, returnrefs)
      if len(returnrefs) == 1:
        self.put_obj(returnrefs[0], result)
      else:
        for i in range(len(returnrefs)):
          self.put_obj(returnrefs[i], result[i])

global_worker = Worker()

def distributed(types, return_types, worker=global_worker):
    def distributed_decorator(func):
        # deserialize arguments, execute function and return results
        def func_executor(args, returnrefs):
            arguments = []
            for (i, arg) in enumerate(args):
              print "pulling argument", i
              if type(arg) == unison.ObjRef:
                if i < len(types) - 1:
                  arguments.append(worker.pull(arg))
                elif i == len(types) - 1 and types[-1] is not None:
                  arguments.append(global_worker.pull(arg))
                elif types[-1] is None:
                  arguments.append(worker.pull(arg))
                else:
                  raise Exception("Passed in " + str(len(args)) + " arguments to function " + func.__name__ + ", which takes only " + str(len(types)) + " arguments.")
              else:
                arguments.append(arg)
            # print "done pulling argument", i
            # TODO
            # buf = bytearray()
            print "called with arguments", arguments
            result = func(*arguments)
            # check number of return values and return types
            if len(return_types) != 1 and len(result) != len(return_types):
              raise Exception("The @distributed decorator for function " + func.__name__ + " has " + str(len(return_types)) + " return values with types " + str(return_types) + " but " + func.__name__ + " returned " + str(len(result)) + " values.")
            return result
        # for remotely executing the function
        def func_call(*args, typecheck=False):
          return worker.call(func_call.func_name, func_call.module_name, args)
        func_call.func_name = func.__name__.encode() # why do we call encode()?
        func_call.module_name = func.__module__.encode() # why do we call encode()?
        func_call.is_distributed = True
        func_call.executor = func_executor
        func_call.types = types
        return func_call
    return distributed_decorator
