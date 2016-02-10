from libc.stdint cimport uint64_t, int64_t
from libcpp cimport bool
from libcpp.string cimport string
import numpy as np

cdef extern from "types.pb.h":
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

  cdef cppclass Obj:
    Obj()
    String* mutable_string_data()
    Int* mutable_int_data()
    Double* mutable_double_data()
    bool has_string_data()
    bool has_int_data()
    bool has_double_data()

cdef class PyValues:
  cdef Values *thisptr
  def __cinit__(self):
    self.thisptr = new Values()
  def __dealloc__(self):
    del self.thisptr

cdef class PyValue:
  cdef Value *thisptr
  def __cinit__(self):
    self.thisptr = new Value()
  def __dealloc__(self):
    del self.thisptr

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

cpdef serialize_args(args):
  cdef Values* vals
  cdef Value* val
  cdef Obj* obj
  cdef String* string_data
  cdef Int* int_data
  cdef Double* double_data
  result = PyValues()
  vals = result.thisptr
  for arg in args:
    val = vals[0].add_value()
    if type(arg) == ObjRef:
      val[0].set_ref(arg.get_id())
    else:
      obj = val[0].mutable_obj()
      if type(arg) == str:
        string_data = obj[0].mutable_string_data()
        string_data[0].set_data(arg)
      elif type(arg) == int or type(arg) == long:
        int_data = obj[0].mutable_int_data()
        int_data[0].set_data(arg)
      elif type(arg) == float:
        double_data = obj[0].mutable_double_data()
        double_data[0].set_data(arg)
  return result

cpdef deserialize_args(PyValues args):
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
  return result

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
