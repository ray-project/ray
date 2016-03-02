# Will be rewritten in C++ for easier deployment once the API is stabilized

from libc.stdint cimport uint64_t, int64_t, uintptr_t
from libcpp cimport bool
from libcpp.string cimport string
import numpy as np

try:
  import cPickle as pickle
except:
  import pickle

cdef extern from "../../../build/generated/types.pb.h":

  cdef cppclass Call:
    Value* add_arg();
    void set_name(const char* value)
    Value* mutable_arg(int index);
    int arg_size() const;

cdef extern from "../../../build/generated/types.pb.h":
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
    bool ParseFromString(const string& data)

cdef class PyValue: # TODO: unify with the below
  cdef Value *thisptr
  def __cinit__(self):
    self.thisptr = new Value()
  def __dealloc__(self):
    del self.thisptr
  def get_value(self):
    return <uintptr_t>self.thisptr

cdef class ObjWrapper: # TODO: unify with the above
  cdef Obj *thisptr
  def __cinit__(self):
    self.thisptr = new Obj()
  # def __dealloc__(self):
  #   del self.thisptr
  def get_value(self):
    return <uintptr_t>self.thisptr

cdef class PythonCall:
  cdef Call* thisptr
  def __cinit__(self):
    self.thisptr = new Call()
  def __dealloc__(self):
    del self.thisptr
  def get_value(self):
    return <uintptr_t>self.thisptr

cdef class ObjRef:
  cdef size_t _id
  cdef object type

  def __cinit__(self, id, type):
    self._id = id

  def __init__(self, id, type):
    self.type = type

  def __richcmp__(self, other, int op):
    if op == 2:
      return self.get_id() == other.get_id()
    else:
      raise NotImplementedError("operator not implemented")

  cpdef get_id(self):
    return self._id

cpdef serialize_into(val, objptr):
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
  else:
    data = pickle.dumps(val, pickle.HIGHEST_PROTOCOL)
    pyobj_data = obj[0].mutable_pyobj_data()
    pyobj_data[0].set_data(data, len(data))

cpdef serialize(val):
  result = ObjWrapper()
  serialize_into(val, result.get_value())
  return result

cdef deserialize_from(Obj* obj):
  if obj[0].has_string_data():
    return obj[0].mutable_string_data()[0].mutable_data()[0]
  elif obj[0].has_int_data():
    return obj[0].mutable_int_data()[0].data()
  elif obj[0].has_double_data():
    return obj[0].mutable_double_data()[0].data()
  else:
    data = obj[0].mutable_pyobj_data()[0].mutable_data()[0]
    return pickle.loads(data)

cpdef deserialize_from_string(str):
  cdef string s = str
  cdef Obj* obj = new Obj() # TODO: memory leak
  obj[0].ParseFromString(s)
  return deserialize_from(obj)

# cpdef deserialize(str):
#     cdef string s = string(str)
#     return deserialize_from(obj.get_value())

# todo: unify with the above, at the moment this is copied
cdef deserialize_args_from_call(Call* call):
  cdef Value* val
  cdef Obj* obj
  result = []
  for i in range(call[0].arg_size()):
    val = call[0].mutable_arg(i)
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

cpdef deserialize_call(PythonCall pycall):
  cdef Call* call = pycall.thisptr
  return deserialize_args_from_call(call)


cdef int numpy_dtype_to_proto(dtype):
  if dtype == np.dtype('int32'):
    return INT32
  if dtype == np.dtype('int64'):
    return INT64
  if dtype == np.dtype('float32'):
    return FLOAT32
  if dtype == np.dtype('float64'):
    return FLOAT64

"""
cdef Value* ndarray_to_proto(array):
  result = PyValue()
  result.shape.extend(array.shape)
  result.data = np.getbuffer(array, 0, array.size * array.dtype.itemsize)
  result.dtype = numpy_dtype_to_proto(array.dtype)
  return result
"""
