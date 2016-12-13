#include <Python.h>

#include "common_extension.h"

static PyMethodDef common_methods[] = {
    {"check_simple_value", check_simple_value, METH_VARARGS,
     "Should the object be passed by value?"},
    {"compute_put_id", compute_put_id, METH_VARARGS,
     "Return the object ID for a put call within a task."},
    {NULL} /* Sentinel */
};

#ifndef PyMODINIT_FUNC /* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif

PyMODINIT_FUNC initcommon(void) {
  PyObject *m;

  if (PyType_Ready(&PyTaskType) < 0)
    return;

  if (PyType_Ready(&PyObjectIDType) < 0)
    return;

  m = Py_InitModule3("common", common_methods,
                     "A module for common types. This is used for testing.");

  init_pickle_module();

  Py_INCREF(&PyTaskType);
  PyModule_AddObject(m, "Task", (PyObject *) &PyTaskType);

  Py_INCREF(&PyObjectIDType);
  PyModule_AddObject(m, "ObjectID", (PyObject *) &PyObjectIDType);

  char common_error[] = "common.error";
  CommonError = PyErr_NewException(common_error, NULL, NULL);
  Py_INCREF(CommonError);
  PyModule_AddObject(m, "common_error", CommonError);
}
