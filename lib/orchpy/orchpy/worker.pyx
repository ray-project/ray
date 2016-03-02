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
    o = ObjWrapper()
    ptr = <uintptr_t>result.get_value()
    serialize_into_2(0, ptr)
    return orch_push(self.context, <void*>ptr)

  cpdef get_serialized(self, objref):
    cdef Slice slice = orch_get_serialized_obj(self.context, objref)
    data = PyBytes_FromStringAndSize(slice.ptr, slice.size)
    return data

  cpdef do_pull(self, objref):
    cdef Slice slice = orch_get_serialized_obj(self.context, objref)

  cpdef pull(self, objref):
    cdef Slice slice = orch_get_serialized_obj(self.context, objref)
    data = PyBytes_FromStringAndSize(slice.ptr, slice.size)
    return unison.deserialize_from_string(data)

  cpdef register_function(self, func_name, function, num_args):
    orch_register_function(self.context, func_name, num_args)
    self.functions[func_name] = function

  cpdef main_loop(self):
    result = []
    cdef Call* call = <Call*>orch_wait_for_next_task(self.context)
    cdef int size = call[0].arg_size()
    cdef Obj* obj
    print "hello from python"
    print "size", size
    return call[0].name(), deserialize_args_from_call(call)

  cpdef invoke_function(self, name, args):
    return self.functions[name].executor(args)

global_worker = Worker()

def distributed(types, return_type, worker=global_worker):
    def distributed_decorator(func):
        # deserialize arguments, execute function and serialize result
        def func_executor(args):
            arguments = []
            for (i, arg) in enumerate(args):
              if type(arg) == unison.ObjRef:
                if i < len(types) - 1:
                  arguments.append(worker.get_object(arg, types[i]))
                elif i == len(types) - 1 and types[-1] is not None:
                  arguments.append(global_worker.get_object(arg, types[i]))
                elif types[-1] is None:
                  arguments.append(worker.get_object(arg, types[-2]))
                else:
                  raise Exception("Passed in " + str(len(args)) + " arguments to function " + func.__name__ + ", which takes only " + str(len(types)) + " arguments.")
              else:
                arguments.append(arg)
            # TODO
            # buf = bytearray()
            print "called with arguments", arguments
            result = func(*arguments)
            # if unison.unison_type(result) != return_type:
            #   raise Exception("Return type of " + func.func_name + " does not match the return type specified in the @distributed decorator, was expecting " + str(return_type) + " but received " + str(unison.unison_type(result)))
            # unison.serialize(buf, result)
            # return memoryview(buf).tobytes()
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

def pull(objref, worker=global_worker):
  return 1
