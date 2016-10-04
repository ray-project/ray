#include "types.h"

int PyObjectToUniqueID(PyObject *object, object_id *objectid) {
  if (PyObject_IsInstance(object, (PyObject *) &PyObjectIDType)) {
    *objectid = ((PyObjectID *) object)->object_id;
    return 1;
  } else {
    PyErr_SetString(PyExc_TypeError, "must be an ObjectID");
    return 0;
  }
}

static int PyObjectID_init(PyObjectID *self, PyObject *args, PyObject *kwds) {
  const char *data;
  int size;
  if (!PyArg_ParseTuple(args, "s#", &data, &size)) {
    return -1;
  }
  if (size != UNIQUE_ID_SIZE) {
    PyErr_SetString(CommonError,
                    "ObjectID: object id string needs to have length 20");
    return -1;
  }
  memcpy(&self->object_id.id[0], data, UNIQUE_ID_SIZE);
  return 0;
}

/* create PyObjectID from C */
PyObject *PyObjectID_make(object_id object_id) {
  PyObjectID *result = PyObject_New(PyObjectID, &PyObjectIDType);
  result = (PyObjectID *) PyObject_Init((PyObject *) result, &PyObjectIDType);
  result->object_id = object_id;
  return (PyObject *) result;
}

static PyObject *PyObjectID_id(PyObject *self) {
  PyObjectID *s = (PyObjectID *) self;
  return PyString_FromStringAndSize((char *) &s->object_id.id[0],
                                    UNIQUE_ID_SIZE);
}

static PyMethodDef PyObjectID_methods[] = {
    {"id", (PyCFunction) PyObjectID_id, METH_NOARGS,
     "Return the hash associated with this ObjectID"},
    {NULL} /* Sentinel */
};

static PyMemberDef PyObjectID_members[] = {
    {NULL} /* Sentinel */
};

PyTypeObject PyObjectIDType = {
    PyObject_HEAD_INIT(NULL) 0, /* ob_size */
    "common.ObjectID",          /* tp_name */
    sizeof(PyObjectID),         /* tp_basicsize */
    0,                          /* tp_itemsize */
    0,                          /* tp_dealloc */
    0,                          /* tp_print */
    0,                          /* tp_getattr */
    0,                          /* tp_setattr */
    0,                          /* tp_compare */
    0,                          /* tp_repr */
    0,                          /* tp_as_number */
    0,                          /* tp_as_sequence */
    0,                          /* tp_as_mapping */
    0,                          /* tp_hash */
    0,                          /* tp_call */
    0,                          /* tp_str */
    0,                          /* tp_getattro */
    0,                          /* tp_setattro */
    0,                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,         /* tp_flags */
    "ObjectID object",          /* tp_doc */
    0,                          /* tp_traverse */
    0,                          /* tp_clear */
    0,                          /* tp_richcompare */
    0,                          /* tp_weaklistoffset */
    0,                          /* tp_iter */
    0,                          /* tp_iternext */
    PyObjectID_methods,         /* tp_methods */
    PyObjectID_members,         /* tp_members */
    0,                          /* tp_getset */
    0,                          /* tp_base */
    0,                          /* tp_dict */
    0,                          /* tp_descr_get */
    0,                          /* tp_descr_set */
    0,                          /* tp_dictoffset */
    (initproc) PyObjectID_init, /* tp_init */
    0,                          /* tp_alloc */
    PyType_GenericNew,          /* tp_new */
};
