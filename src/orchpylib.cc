// TODO: - Implement other datatypes for ndarray

#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION

#include <Python.h>
#include <structmember.h>
#include <numpy/arrayobject.h>
#include <iostream>

#include "types.pb.h"
#include "worker.h"

// extracts a pointer from a python C API capsule
template<typename T>
T* get_pointer_or_fail(PyObject* capsule, const char* name) {
  if (PyCapsule_IsValid(capsule, name)) {
    T* result = static_cast<T*>(PyCapsule_GetPointer(capsule, name));
  } else {
    std::cout << "not a valid capsule" << std::endl;
  }
}

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
  0,                         /* tp_compare */
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
  PyObject* result = PyObjRef_new(&PyObjRefType, NULL, NULL);
  PyObjRef_init((PyObjRef*) result, Py_BuildValue("i", objref), NULL);
  return result;
}

// Error handling

static PyObject *OrchPyError;

// Serialization

// serialize will serialize the python object val into the protocol buffer
// object obj, returns 0 if successful and something else if not
int serialize(PyObject* val, Obj* obj) {
  if (PyInt_Check(val)) {
    Int* data = obj->mutable_int_data();
    long d = PyInt_AsLong(val);
    data->set_data(d);
  } else if (PyFloat_Check(val)) {
    Double* data = obj->mutable_double_data();
    double d = PyFloat_AsDouble(val);
    data->set_data(d);
  } else if (PyList_Check(val)) {
    List* data = obj->mutable_list_data();
    for (size_t i = 0, size = PyList_Size(val); i < size; ++i) {
      Obj* elem = data->add_elem();
      if (serialize(PyList_GetItem(val, i), elem) != 0)
        return -1;
    }
  } else if (PyString_Check(val)) {
    char* buffer;
    Py_ssize_t length;
    PyString_AsStringAndSize(val, &buffer, &length); // creates pointer to internal buffer
    obj->mutable_string_data()->set_data(buffer, length);
  } else if (PyArray_Check(val)) {
    PyArrayObject* array = PyArray_GETCONTIGUOUS((PyArrayObject*)val);
    Array* data = obj->mutable_array_data();
    npy_intp size = PyArray_SIZE(array);
    for (int i = 0; i < PyArray_NDIM(array); ++i) {
      data->add_shape(PyArray_DIM(array, i));
    }
    if (PyArray_ISFLOAT(array)) {
      double* buffer = (double*) PyArray_DATA(array);
      for (npy_intp i = 0; i < size; ++i) {
        data->add_double_data(buffer[i]);
      }
    }
  } else {
    return -1;
  }
  return 0;
}

PyObject* deserialize(const Obj& obj) {
  if (obj.has_int_data()) {
    return PyInt_FromLong(obj.int_data().data());
  } else if (obj.has_double_data()) {
    return PyFloat_FromDouble(obj.double_data().data());
  } else if (obj.has_list_data()) {
    const List& data = obj.list_data();
    size_t size = data.elem_size();
    PyObject* list = PyList_New(size);
    for (size_t i = 0; i < size; ++i) {
      PyList_SetItem(list, i, deserialize(data.elem(i)));
    }
    return list;
  } else if (obj.has_string_data()) {
    const char* buffer = obj.string_data().data().data();
    Py_ssize_t length = obj.string_data().data().size();
    return PyString_FromStringAndSize(buffer, length);
  } else if (obj.has_array_data()) {
    const Array& array = obj.array_data();
    if (array.double_data_size() > 0) { // TODO: this is not quite right
      npy_intp size = array.double_data_size();
      std::vector<npy_intp> dims;
      for (int i = 0; i < array.shape_size(); ++i) {
        dims.push_back(array.shape(i));
      }
      PyArrayObject* pyarray = (PyArrayObject*)PyArray_SimpleNew(array.shape_size(), &dims[0], NPY_DOUBLE);
      double* buffer = (double*) PyArray_DATA(pyarray);
      for (npy_intp i = 0; i < size; ++i) {
        buffer[i] = array.double_data(i);
      }
      return (PyObject*)pyarray;
    }
  } else {
    std::cout << "don't have object" << std::endl;
  }
}

PyObject* serialize_object(PyObject* self, PyObject* args) {
  Obj* obj = new Obj(); // TODO: to be freed in capsul destructor
  PyObject* val = PyTuple_GetItem(args, 0);
  if (serialize(val, obj) != 0) {
    PyErr_SetString(OrchPyError, "serialization: type not know"); // TODO: put a more expressive error message here
    return NULL;
  }
  return PyCapsule_New(static_cast<void*>(obj), NULL, NULL);
}

PyObject* deserialize_object(PyObject* self, PyObject* args) {
  PyObject* capsule = PyTuple_GetItem(args, 0);
  Obj* obj = get_pointer_or_fail<Obj>(capsule, NULL);
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
      Obj* arg = call->add_arg()->mutable_obj();
      serialize(PyList_GetItem(arguments, i), arg);
    }
  } else {
    std::cout << "second argument is not a list" << std::endl;
  }
  return PyCapsule_New(static_cast<void*>(call), "call", NULL);
}

PyObject* deserialize_call(PyObject* self, PyObject* args) {
  PyObject* capsule = PyTuple_GetItem(args, 0);
  Call* call = get_pointer_or_fail<Call>(capsule, "call");
  PyObject* string = PyString_FromStringAndSize(call->name().c_str(), call->name().size());
  int argsize = call->arg_size();
  PyObject* arglist = PyList_New(argsize);
  for (int i = 0; i < argsize; ++i) {
    const Value& val = call->arg(i);
    if (!val.has_obj()) {
      // TODO: Deserialize object reference here
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
  PyObject* capsule = PyTuple_GetItem(args, 0);
  Worker* worker = get_pointer_or_fail<Worker>(capsule, "worker");
  Call* call = worker->receive_next_task();
  return PyCapsule_New(static_cast<void*>(call), "call", NULL); // TODO: how is destruction going to be handled here?
}

PyObject* remote_call(PyObject* self, PyObject* args) {
  PyObject* worker_capsule;
  PyObject* call_capsule;
  if (!PyArg_ParseTuple(args, "OO", &worker_capsule, &call_capsule)) {
    return NULL;
  }
  Worker* worker = get_pointer_or_fail<Worker>(worker_capsule, "worker");
  Call* call = get_pointer_or_fail<Call>(call_capsule, "call");
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

PyObject* register_function(PyObject* self, PyObject* args) {
  PyObject* worker_capsule;
  const char* function_name;
  int num_return_vals;
  if (!PyArg_ParseTuple(args, "Osi", &worker_capsule, &function_name, &num_return_vals)) {
    return NULL;
  }
  Worker* worker = get_pointer_or_fail<Worker>(worker_capsule, "worker");
  worker->register_function(std::string(function_name), num_return_vals);
  return worker_capsule;
}

PyObject* pull_object(PyObject* self, PyObject* args) {
  PyObject* worker_capsule;
  PyObject* objref;
  if (!PyArg_ParseTuple(args, "OO", &worker_capsule, &objref)) {
    return NULL;
  }
  Worker* worker = get_pointer_or_fail<Worker>(worker_capsule, "worker");
  return worker_capsule;
}

static PyMethodDef SymphonyMethods[] = {
 { "serialize_object", serialize_object, METH_VARARGS, "serialize an object to protocol buffers" },
 { "deserialize_object", deserialize_object, METH_VARARGS, "deserialize an object from protocol buffers" },
 { "serialize_call", serialize_call, METH_VARARGS, "serialize a call to protocol buffers" },
 { "deserialize_call", deserialize_call, METH_VARARGS, "deserialize a call from protocol buffers" },
 { "create_worker", create_worker, METH_VARARGS, "connect to the scheduler and the object store" },
 { "register_function", register_function, METH_VARARGS, "register a function with the scheduler" },
 { "remote_call", remote_call, METH_VARARGS, "call a remote function" },
 { NULL, NULL, 0, NULL }
};

PyMODINIT_FUNC initliborchpylib(void) {
  PyObject* m;
  PyObjRefType.tp_new = PyType_GenericNew;
  if (PyType_Ready(&PyObjRefType) < 0) {
    return;
  }
  m = Py_InitModule3("liborchpylib", SymphonyMethods, "Python C Extension for Orchestra");
  Py_INCREF(&PyObjRefType);
  PyModule_AddObject(m, "ObjRef", (PyObject *)&PyObjRefType);
  OrchPyError = PyErr_NewException("orchpy.error", NULL, NULL);
  Py_INCREF(OrchPyError);
  PyModule_AddObject(m, "error", OrchPyError);
  import_array();
}

}
