from libc.stdint cimport uintptr_t
import orchpy.unison as unison

from libc.stdint cimport uint64_t, int64_t, uintptr_t
from libcpp cimport bool
from libcpp.string cimport string

cdef struct Slice:
  char* ptr
  size_t size

cdef extern void* orch_create_context(const char* server_addr, const char* worker_addr, const char* objstore_addr);
cdef extern void orch_register_function(void* worker, const char* name, size_t num_return_vals)
cdef extern size_t orch_remote_call(void* context, void* request);
cdef extern size_t orch_push(void* context, void* value);
cdef extern void orch_main_loop(void* context);
cdef extern Slice orch_get_serialized_obj(void* context, size_t objref);

cdef extern from "Python.h":
    Py_ssize_t PyByteArray_GET_SIZE(object array)
    object PyUnicode_FromStringAndSize(char *buff, Py_ssize_t len)
    object PyBytes_FromStringAndSize(char *buff, Py_ssize_t len)
    object PyString_FromStringAndSize(char *buff, Py_ssize_t len)
    int PyByteArray_Resize(object self, Py_ssize_t size) except -1
    char* PyByteArray_AS_STRING(object bytearray)

# cdef extern from "../../../build/generated/orchestra.pb.h":
#   cdef cppclass RemoteCallRequest:
#     RemoteCallRequest()
#     void set_name(const char* value)
#     Call* mutable_call()

cdef extern from "../../../build/generated/types.pb.h":
  cdef cppclass Values

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

cdef class Worker:
  cdef void* context

  def __cinit__(self):
    self.context = NULL

  def connect(self, server_addr, worker_addr, objstore_addr):
    self.context = orch_create_context(server_addr, worker_addr, objstore_addr)

#  cpdef call(self, name, args):
#    cdef RemoteCallRequest* result = new RemoteCallRequest()
#    result[0].set_name(name)
#    unison.serialize_args_into(args, <uintptr_t>result[0].mutable_arg())
#    for i in range(10):
#      orch_remote_call(self.context, result)
#   # return <uintptr_t>result

  cpdef do_call(self, ptr):
    return orch_remote_call(self.context, <void*>ptr)

  cpdef do_push(self, val):
    print("before serialization")
    result = unison.serialize(val)
    print("before push")
    # ptr = result.get_value()
    # print "pointer is", ptr
    # cdef Obj* obj = new Obj()
    o = ObjWrapper()
    # serialize_into_2(0, <uintptr_t>obj)
    # cdef Obj* ptr = new Obj() # o.get_value()
    ## ptr = <uintptr_t>o.get_value()
    ptr = <uintptr_t>result.get_value()
    serialize_into_2(0, ptr)
    return orch_push(self.context, <void*>ptr)

  cpdef get_serialized(self, objref):
    cdef Slice slice = orch_get_serialized_obj(self.context, objref)
    data = PyBytes_FromStringAndSize(slice.ptr, slice.size)
    return data

  cpdef pull(self, objref):
    cdef Slice slice = orch_get_serialized_obj(self.context, objref)

  cpdef register_function(self, func_name, num_args):
    orch_register_function(self.context, func_name, num_args)

  cpdef main_loop(self):
    orch_main_loop(self.context)

global_worker = Worker()

def pull(objref, worker=global_worker):
  return 1
