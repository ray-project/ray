// TODO: - Implement other datatypes for ndarray

#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION

#include <Python.h>
#include <structmember.h>
#define PY_ARRAY_UNIQUE_SYMBOL ORCHESTRA_ARRAY_API
#include <numpy/arrayobject.h>
#include <arrow/api.h>
#include <iostream>

#include "types.pb.h"
#include "worker.h"

#include "serialize.h"

extern "C" {

// Object references

typedef struct {
    PyObject_HEAD
    ObjRef val;
} PyObjRef;

static void PyObjRef_dealloc(PyObjRef *self) {
  self->ob_type->tp_free((PyObject*) self);
}

static PyObject* PyObjRef_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
  PyObjRef* self = (PyObjRef*) type->tp_alloc(type, 0);
  if (self != NULL) {
    self->val = 0;
  }
  return (PyObject*) self;
}

static int PyObjRef_init(PyObjRef *self, PyObject *args, PyObject *kwds) {
  if (!PyArg_ParseTuple(args, "i", &self->val)) {
    return -1;
  }
  return 0;
};

static int PyObjRef_compare(PyObject* a, PyObject* b) {
  PyObjRef* A = (PyObjRef*) a;
  PyObjRef* B = (PyObjRef*) b;
  if (A->val < B->val) {
    return -1;
  }
  if (A->val > B->val) {
    return 1;
  }
  return 0;
}

static PyMemberDef PyObjRef_members[] = {
  {"val", T_INT, offsetof(PyObjRef, val), 0, "object reference"},
  {NULL}
};

static PyTypeObject PyObjRefType = {
  PyObject_HEAD_INIT(NULL)
  0,                         /* ob_size */
  "orchpy.ObjRef",           /* tp_name */
  sizeof(PyObjRef),          /* tp_basicsize */
  0,                         /* tp_itemsize */
  0,                         /* tp_dealloc */
  0,                         /* tp_print */
  0,                         /* tp_getattr */
  0,                         /* tp_setattr */
  PyObjRef_compare,          /* tp_compare */
  0,                         /* tp_repr */
  0,                         /* tp_as_number */
  0,                         /* tp_as_sequence */
  0,                         /* tp_as_mapping */
  0,                         /* tp_hash */
  0,                         /* tp_call */
  0,                         /* tp_str */
  0,                         /* tp_getattro */
  0,                         /* tp_setattro */
  0,                         /* tp_as_buffer */
  Py_TPFLAGS_DEFAULT,        /* tp_flags */
  "OrchPy objects",          /* tp_doc */
  0,                         /* tp_traverse */
  0,                         /* tp_clear */
  0,                         /* tp_richcompare */
  0,                         /* tp_weaklistoffset */
  0,                         /* tp_iter */
  0,                         /* tp_iternext */
  0,                         /* tp_methods */
  PyObjRef_members,          /* tp_members */
  0,                         /* tp_getset */
  0,                         /* tp_base */
  0,                         /* tp_dict */
  0,                         /* tp_descr_get */
  0,                         /* tp_descr_set */
  0,                         /* tp_dictoffset */
  (initproc)PyObjRef_init,   /* tp_init */
  0,                         /* tp_alloc */
  PyObjRef_new,              /* tp_new */
};

// create PyObjRef from C++ (could be made more efficient if neccessary)
PyObject* make_pyobjref(ObjRef objref) {
  PyObject* arglist = Py_BuildValue("(i)", objref);
  PyObject* result = PyObject_CallObject((PyObject*) &PyObjRefType, arglist);
  Py_DECREF(arglist);
  return result;
}

// Error handling

static PyObject *OrchPyError;

int PyObjectToCall(PyObject* object, Call **call) {
  if (PyCapsule_IsValid(object, "call")) {
    *call = static_cast<Call*>(PyCapsule_GetPointer(object, "call"));
    return 1;
  } else {
    PyErr_SetString(PyExc_TypeError, "must be a 'call' capsule");
    return 0;
  }
}

int PyObjectToObj(PyObject* object, Obj **obj) {
  if (PyCapsule_IsValid(object, "obj")) {
    *obj = static_cast<Obj*>(PyCapsule_GetPointer(object, "obj"));
    return 1;
  } else {
    PyErr_SetString(PyExc_TypeError, "must be a 'obj' capsule");
    return 0;
  }
}

int PyObjectToWorker(PyObject* object, Worker **worker) {
  if (PyCapsule_IsValid(object, "worker")) {
    *worker = static_cast<Worker*>(PyCapsule_GetPointer(object, "worker"));
    return 1;
  } else {
    PyErr_SetString(PyExc_TypeError, "must be a 'worker' capsule");
    return 0;
  }
}

int PyObjectToObjRef(PyObject* object, ObjRef *objref) {
  if (PyObject_IsInstance(object, (PyObject*)&PyObjRefType)) {
    *objref = ((PyObjRef*) object)->val;
    return 1;
  } else {
    PyErr_SetString(PyExc_TypeError, "must be an object reference");
    return 0;
  }
}

// Serialization

// serialize will serialize the python object val into the protocol buffer
// object obj, returns 0 if successful and something else if not
// FIXME(pcm): This currently only works for contiguous arrays
int serialize(PyObject* val, Obj* obj) {
  if (PyInt_Check(val)) {
    Int* data = obj->mutable_int_data();
    long d = PyInt_AsLong(val);
    data->set_data(d);
  } else if (PyFloat_Check(val)) {
    Double* data = obj->mutable_double_data();
    double d = PyFloat_AsDouble(val);
    data->set_data(d);
  } else if (PyTuple_Check(val)) {
    Tuple* data = obj->mutable_tuple_data();
    for (size_t i = 0, size = PyTuple_Size(val); i < size; ++i) {
      Obj* elem = data->add_elem();
      if (serialize(PyTuple_GetItem(val, i), elem) != 0) {
        return -1;
      }
    }
  } else if (PyList_Check(val)) {
    List* data = obj->mutable_list_data();
    for (size_t i = 0, size = PyList_Size(val); i < size; ++i) {
      Obj* elem = data->add_elem();
      if (serialize(PyList_GetItem(val, i), elem) != 0) {
        return -1;
      }
    }
  } else if (PyDict_Check(val)) {
    PyObject *pykey, *pyvalue;
    Py_ssize_t pos = 0;
    Dict* data = obj->mutable_dict_data();
    while (PyDict_Next(val, &pos, &pykey, &pyvalue)) {
      DictEntry* elem = data->add_elem();
      Obj* key = elem->mutable_key();
      if (serialize(pykey, key) != 0) {
        return -1;
      }
      Obj* value = elem->mutable_value();
      if (serialize(pyvalue, value) != 0) {
        return -1;
      }
    }
  } else if (PyString_Check(val)) {
    char* buffer;
    Py_ssize_t length;
    PyString_AsStringAndSize(val, &buffer, &length); // creates pointer to internal buffer
    obj->mutable_string_data()->set_data(buffer, length);
  } else if (PyArray_Check(val)) {
    PyArrayObject* array = PyArray_GETCONTIGUOUS((PyArrayObject*) val);
    Array* data = obj->mutable_array_data();
    npy_intp size = PyArray_SIZE(array);
    for (int i = 0; i < PyArray_NDIM(array); ++i) {
      data->add_shape(PyArray_DIM(array, i));
    }
    int typ = PyArray_TYPE(array);
    data->set_dtype(typ);
    switch (typ) {
      case NPY_FLOAT: {
          npy_float* buffer = (npy_float*) PyArray_DATA(array);
          for (npy_intp i = 0; i < size; ++i) {
            data->add_float_data(buffer[i]);
          }
        }
        break;
      case NPY_DOUBLE: {
          npy_double* buffer = (npy_double*) PyArray_DATA(array);
          for (npy_intp i = 0; i < size; ++i) {
            data->add_double_data(buffer[i]);
          }
        }
        break;
      case NPY_INT8: {
          npy_int8* buffer = (npy_int8*) PyArray_DATA(array);
          for (npy_intp i = 0; i < size; ++i) {
            data->add_int_data(buffer[i]);
          }
        }
        break;
      case NPY_INT64: {
          npy_int64* buffer = (npy_int64*) PyArray_DATA(array);
          for (npy_intp i = 0; i < size; ++i) {
            data->add_int_data(buffer[i]);
          }
        }
        break;
      case NPY_UINT8: {
          npy_uint8* buffer = (npy_uint8*) PyArray_DATA(array);
          for (npy_intp i = 0; i < size; ++i) {
            data->add_uint_data(buffer[i]);
          }
        }
        break;
      case NPY_UINT64: {
          npy_uint64* buffer = (npy_uint64*) PyArray_DATA(array);
          for (npy_intp i = 0; i < size; ++i) {
            data->add_uint_data(buffer[i]);
          }
        }
        break;
      case NPY_OBJECT: { // FIXME(pcm): Support arbitrary python objects, not only objrefs
          PyArrayIterObject* iter = (PyArrayIterObject*) PyArray_IterNew((PyObject*)array);
          while (PyArray_ITER_NOTDONE(iter)) {
            PyObject** item = (PyObject**) PyArray_ITER_DATA(iter);
            ObjRef objref;
            if (PyObject_IsInstance(*item, (PyObject*) &PyObjRefType)) {
              objref = ((PyObjRef*) (*item))->val;
            } else {
              PyErr_SetString(PyExc_TypeError, "must be an object reference"); // TODO: improve error message
              return -1;
            }
            data->add_objref_data(objref);
            PyArray_ITER_NEXT(iter);
          }
          Py_XDECREF(iter);
        }
        break;
      default:
        PyErr_SetString(OrchPyError, "serialization: numpy datatype not know");
        return -1;
    }
  } else {
    PyErr_SetString(OrchPyError, "serialization: type not know");
    return -1;
  }
  return 0;
}

PyObject* deserialize(const Obj& obj) {
  if (obj.has_int_data()) {
    return PyInt_FromLong(obj.int_data().data());
  } else if (obj.has_double_data()) {
    return PyFloat_FromDouble(obj.double_data().data());
  } else if (obj.has_tuple_data()) {
    const Tuple& data = obj.tuple_data();
    size_t size = data.elem_size();
    PyObject* tuple = PyTuple_New(size);
    for (size_t i = 0; i < size; ++i) {
      PyTuple_SetItem(tuple, i, deserialize(data.elem(i)));
    }
    return tuple;
  } else if (obj.has_list_data()) {
    const List& data = obj.list_data();
    size_t size = data.elem_size();
    PyObject* list = PyList_New(size);
    for (size_t i = 0; i < size; ++i) {
      PyList_SetItem(list, i, deserialize(data.elem(i)));
    }
    return list;
  } else if (obj.has_dict_data()) {
    const Dict& data = obj.dict_data();
    PyObject* dict = PyDict_New();
    size_t size = data.elem_size();
    for (size_t i = 0; i < size; ++i) {
      PyDict_SetItem(dict, deserialize(data.elem(i).key()), deserialize(data.elem(i).value()));
    }
    return dict;
  } else if (obj.has_string_data()) {
    const char* buffer = obj.string_data().data().data();
    Py_ssize_t length = obj.string_data().data().size();
    return PyString_FromStringAndSize(buffer, length);
  } else if (obj.has_array_data()) {
    const Array& array = obj.array_data();
    std::vector<npy_intp> dims;
    for (int i = 0; i < array.shape_size(); ++i) {
      dims.push_back(array.shape(i));
    }
    PyArrayObject* pyarray = (PyArrayObject*) PyArray_SimpleNew(array.shape_size(), &dims[0], array.dtype());
    if (array.double_data_size() > 0) { // TODO: handle empty array
      npy_intp size = array.double_data_size();
      npy_double* buffer = (npy_double*) PyArray_DATA(pyarray);
      for (npy_intp i = 0; i < size; ++i) {
        buffer[i] = array.double_data(i);
      }
    } else if (array.float_data_size() > 0) {
      npy_intp size = array.float_data_size();
      npy_float* buffer = (npy_float*) PyArray_DATA(pyarray);
      for (npy_intp i = 0; i < size; ++i) {
        buffer[i] = array.float_data(i);
      }
    } else if (array.int_data_size() > 0) {
      npy_intp size = array.int_data_size();
      switch (array.dtype()) {
        case NPY_INT8: {
            npy_int8* buffer = (npy_int8*) PyArray_DATA(pyarray);
            for (npy_intp i = 0; i < size; ++i) {
              buffer[i] = array.int_data(i);
            }
          }
          break;
        case NPY_INT64: {
            npy_int64* buffer = (npy_int64*) PyArray_DATA(pyarray);
            for (npy_intp i = 0; i < size; ++i) {
              buffer[i] = array.int_data(i);
            }
          }
          break;
        default:
          PyErr_SetString(OrchPyError, "deserialization: internal error (array type not implemented)");
          return NULL;
      }
    } else if (array.uint_data_size() > 0) {
      npy_intp size = array.uint_data_size();
      switch (array.dtype()) {
        case NPY_UINT8: {
            npy_uint8* buffer = (npy_uint8*) PyArray_DATA(pyarray);
            for (npy_intp i = 0; i < size; ++i) {
              buffer[i] = array.uint_data(i);
            }
          }
          break;
        case NPY_UINT64: {
            npy_uint64* buffer = (npy_uint64*) PyArray_DATA(pyarray);
            for (npy_intp i = 0; i < size; ++i) {
              buffer[i] = array.uint_data(i);
            }
          }
          break;
        default:
          PyErr_SetString(OrchPyError, "deserialization: internal error (array type not implemented)");
          return NULL;
      }
    } else if (array.objref_data_size() > 0) {
      npy_intp size = array.objref_data_size();
      PyObject** buffer = (PyObject**) PyArray_DATA(pyarray);
      for (npy_intp i = 0; i < size; ++i) {
        buffer[i] = make_pyobjref(array.objref_data(i));
      }
    } else {
      PyErr_SetString(OrchPyError, "deserialization: internal error (array type not implemented)");
      return NULL;
    }
    return (PyObject*) pyarray;
  } else {
    PyErr_SetString(OrchPyError, "deserialization: internal error (type not implemented)");
    return NULL;
  }
}

PyObject* serialize_object(PyObject* self, PyObject* args) {
  Obj* obj = new Obj(); // TODO: to be freed in capsul destructor
  PyObject* pyval;
  if (!PyArg_ParseTuple(args, "O", &pyval)) {
    return NULL;
  }
  if (serialize(pyval, obj) != 0) {
    return NULL;
  }
  return PyCapsule_New(static_cast<void*>(obj), "obj", NULL);
}

PyObject* put_arrow(PyObject* self, PyObject* args) {
  Worker* worker;
  ObjRef objref;
  PyObject* value;
  if (!PyArg_ParseTuple(args, "O&O&O", &PyObjectToWorker, &worker, &PyObjectToObjRef, &objref, &value)) {
    return NULL;
  }
  if (!PyArray_Check(value)) {
    PyErr_SetString(PyExc_TypeError, "only support arrays at this point");
    return NULL;
  }
  PyArrayObject* array = PyArray_GETCONTIGUOUS((PyArrayObject*) value);
  worker->put_arrow(objref, array);
  Py_RETURN_NONE;
}

PyObject* get_arrow(PyObject* self, PyObject* args) {
  Worker* worker;
  ObjRef objref;
  if (!PyArg_ParseTuple(args, "O&O&", &PyObjectToWorker, &worker, &PyObjectToObjRef, &objref)) {
    return NULL;
  }
  return (PyObject*) worker->get_arrow(objref);
}

PyObject* is_arrow(PyObject* self, PyObject* args) {
  Worker* worker;
  ObjRef objref;
  if (!PyArg_ParseTuple(args, "O&O&", &PyObjectToWorker, &worker, &PyObjectToObjRef, &objref)) {
    return NULL;
  }
  if (worker->is_arrow(objref))
    Py_RETURN_TRUE;
  else
    Py_RETURN_FALSE;
}

PyObject* deserialize_object(PyObject* self, PyObject* args) {
  Obj* obj;
  if (!PyArg_ParseTuple(args, "O&", &PyObjectToObj, &obj)) {
    return NULL;
  }
  return deserialize(*obj);
}

PyObject* serialize_call(PyObject* self, PyObject* args) {
  Call* call = new Call(); // TODO: to be freed in capsul destructor
  char* name;
  int len;
  PyObject* arguments;
  if (!PyArg_ParseTuple(args, "s#O", &name, &len, &arguments)) {
    return NULL;
  }
  call->set_name(name, len);
  if (PyList_Check(arguments)) {
    for (size_t i = 0, size = PyList_Size(arguments); i < size; ++i) {
      PyObject* element = PyList_GetItem(arguments, i);
      if (PyObject_IsInstance(element, (PyObject*)&PyObjRefType)) {
        ObjRef objref = ((PyObjRef*) element)->val;
        call->add_arg()->set_ref(objref);
      } else {
        Obj* arg = call->add_arg()->mutable_obj();
        serialize(PyList_GetItem(arguments, i), arg);
      }
    }
  } else {
    PyErr_SetString(OrchPyError, "serialize_call: second argument needs to be a list");
    return NULL;
  }
  return PyCapsule_New(static_cast<void*>(call), "call", NULL);
}

PyObject* deserialize_call(PyObject* self, PyObject* args) {
  Call* call;
  if (!PyArg_ParseTuple(args, "O&", &PyObjectToCall, &call)) {
    return NULL;
  }
  PyObject* string = PyString_FromStringAndSize(call->name().c_str(), call->name().size());
  int argsize = call->arg_size();
  PyObject* arglist = PyList_New(argsize);
  for (int i = 0; i < argsize; ++i) {
    const Value& val = call->arg(i);
    if (!val.has_obj()) {
      PyList_SetItem(arglist, i, make_pyobjref(val.ref()));
    } else {
      PyList_SetItem(arglist, i, deserialize(val.obj()));
    }
  }
  int resultsize = call->result_size();
  PyObject* resultlist = PyList_New(resultsize);
  for (int i = 0; i < resultsize; ++i) {
    PyList_SetItem(resultlist, i, make_pyobjref(call->result(i)));
  }
  return PyTuple_Pack(3, string, arglist, resultlist);
}

// Orchestra Python API

PyObject* create_worker(PyObject* self, PyObject* args) {
  const char* scheduler_addr;
  const char* objstore_addr;
  const char* worker_addr;
  if (!PyArg_ParseTuple(args, "sss", &scheduler_addr, &objstore_addr, &worker_addr)) {
    return NULL;
  }
  auto scheduler_channel = grpc::CreateChannel(scheduler_addr, grpc::InsecureChannelCredentials());
  auto objstore_channel = grpc::CreateChannel(objstore_addr, grpc::InsecureChannelCredentials());
  Worker* worker = new Worker(std::string(worker_addr), scheduler_channel, objstore_channel);
  worker->register_worker(std::string(worker_addr), std::string(objstore_addr));
  return PyCapsule_New(static_cast<void*>(worker), "worker", NULL); // TODO: add destructor the deallocates worker
}

PyObject* wait_for_next_task(PyObject* self, PyObject* args) {
  Worker* worker;
  if (!PyArg_ParseTuple(args, "O&", &PyObjectToWorker, &worker)) {
    return NULL;
  }
  Call* call = worker->receive_next_task();
  return PyCapsule_New(static_cast<void*>(call), "call", NULL); // TODO: how is destruction going to be handled here?
}

PyObject* remote_call(PyObject* self, PyObject* args) {
  Worker* worker;
  Call* call;
  if (!PyArg_ParseTuple(args, "O&O&", &PyObjectToWorker, &worker, &PyObjectToCall, &call)) {
    return NULL;
  }
  RemoteCallRequest request;
  request.set_allocated_call(call);
  RemoteCallReply reply = worker->remote_call(&request);
  request.release_call(); // TODO: Make sure that call is not moved, otherwise capsule pointer needs to be updated
  int size = reply.result_size();
  PyObject* list = PyList_New(size);
  for (int i = 0; i < size; ++i) {
    PyList_SetItem(list, i, make_pyobjref(reply.result(i)));
  }
  return list;
}

PyObject* notify_task_completed(PyObject* self, PyObject* args) {
  Worker* worker;
  if (!PyArg_ParseTuple(args, "O&", &PyObjectToWorker, &worker)) {
    return NULL;
  }
  worker->notify_task_completed();
  Py_RETURN_NONE;
}

PyObject* register_function(PyObject* self, PyObject* args) {
  Worker* worker;
  const char* function_name;
  int num_return_vals;
  if (!PyArg_ParseTuple(args, "O&si", &PyObjectToWorker, &worker, &function_name, &num_return_vals)) {
    return NULL;
  }
  worker->register_function(std::string(function_name), num_return_vals);
  Py_RETURN_NONE;
}

PyObject* get_objref(PyObject* self, PyObject* args) {
  Worker* worker;
  if (!PyArg_ParseTuple(args, "O&", &PyObjectToWorker, &worker)) {
    return NULL;
  }
  ObjRef objref = worker->get_objref();
  return make_pyobjref(objref);
}

PyObject* put_object(PyObject* self, PyObject* args) {
  Worker* worker;
  ObjRef objref;
  Obj* obj;
  if (!PyArg_ParseTuple(args, "O&O&O&", &PyObjectToWorker, &worker, &PyObjectToObjRef, &objref, &PyObjectToObj, &obj)) {
    return NULL;
  }
  worker->put_object(objref, obj);
  Py_RETURN_NONE;
}

PyObject* get_object(PyObject* self, PyObject* args) {
  Worker* worker;
  ObjRef objref;
  if (!PyArg_ParseTuple(args, "O&O&", &PyObjectToWorker, &worker, &PyObjectToObjRef, &objref)) {
    return NULL;
  }
  slice s = worker->get_object(objref);
  Obj* obj = new Obj(); // TODO: Make sure this will get deleted
  obj->ParseFromString(std::string(reinterpret_cast<char*>(s.data), s.len));
  return PyCapsule_New(static_cast<void*>(obj), "obj", NULL);
}

PyObject* request_object(PyObject* self, PyObject* args) {
  Worker* worker;
  ObjRef objref;
  if (!PyArg_ParseTuple(args, "O&O&", &PyObjectToWorker, &worker, &PyObjectToObjRef, &objref)) {
    return NULL;
  }
  worker->request_object(objref);
  Py_RETURN_NONE;
}

PyObject* start_worker_service(PyObject* self, PyObject* args) {
  Worker* worker;
  if (!PyArg_ParseTuple(args, "O&", &PyObjectToWorker, &worker)) {
    return NULL;
  }
  worker->start_worker_service();
  Py_RETURN_NONE;
}

static PyMethodDef OrchPyLibMethods[] = {
 { "serialize_object", serialize_object, METH_VARARGS, "serialize an object to protocol buffers" },
 { "deserialize_object", deserialize_object, METH_VARARGS, "deserialize an object from protocol buffers" },
 { "put_arrow", put_arrow, METH_VARARGS, "put an arrow array on the local object store"},
 { "get_arrow", get_arrow, METH_VARARGS, "get an arrow array from the local object store"},
 { "is_arrow", is_arrow, METH_VARARGS, "is the object in the local object store an arrow object?"},
 { "serialize_call", serialize_call, METH_VARARGS, "serialize a call to protocol buffers" },
 { "deserialize_call", deserialize_call, METH_VARARGS, "deserialize a call from protocol buffers" },
 { "create_worker", create_worker, METH_VARARGS, "connect to the scheduler and the object store" },
 { "register_function", register_function, METH_VARARGS, "register a function with the scheduler" },
 { "put_object", put_object, METH_VARARGS, "put a protocol buffer object (given as a capsule) on the local object store" },
 { "get_object", get_object, METH_VARARGS, "get protocol buffer object from the local object store" },
 { "get_objref", get_objref, METH_VARARGS, "register a new object reference with the scheduler" },
 { "request_object" , request_object, METH_VARARGS, "request an object to be delivered to the local object store" },
 { "wait_for_next_task", wait_for_next_task, METH_VARARGS, "get next task from scheduler (blocking)" },
 { "remote_call", remote_call, METH_VARARGS, "call a remote function" },
 { "notify_task_completed", notify_task_completed, METH_VARARGS, "notify the scheduler that a task has been completed" },
 { "start_worker_service", start_worker_service, METH_VARARGS, "start the worker service" },
 { NULL, NULL, 0, NULL }
};

PyMODINIT_FUNC initliborchpylib(void) {
  PyObject* m;
  PyObjRefType.tp_new = PyType_GenericNew;
  if (PyType_Ready(&PyObjRefType) < 0) {
    return;
  }
  m = Py_InitModule3("liborchpylib", OrchPyLibMethods, "Python C Extension for Orchestra");
  Py_INCREF(&PyObjRefType);
  PyModule_AddObject(m, "ObjRef", (PyObject *)&PyObjRefType);
  OrchPyError = PyErr_NewException("orchpy.error", NULL, NULL);
  Py_INCREF(OrchPyError);
  PyModule_AddObject(m, "error", OrchPyError);
  import_array();
}

}
