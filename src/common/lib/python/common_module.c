#include <Python.h>

#include "common_extension.h"

static PyMethodDef common_methods[] = {
    {"check_simple_value", check_simple_value, METH_VARARGS,
     "Should the object be passed by value?"},
    {"compute_put_id", compute_put_id, METH_VARARGS,
     "Return the object ID for a put call within a task."},
    {"pytask_from_string", PyTask_from_string, METH_VARARGS,
      "creates a Python PyTask object from a string representation of task_spec"},
    {"pytask_to_string", PyTask_to_string, METH_VARARGS,
      "translates a PyTask python object to a byte string"},
    {NULL} /* Sentinel */
};

#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "common",                                               /* m_name */
    "A module for common types. This is used for testing.", /* m_doc */
    0,                                                      /* m_size */
    common_methods,                                         /* m_methods */
    NULL,                                                   /* m_reload */
    NULL,                                                   /* m_traverse */
    NULL,                                                   /* m_clear */
    NULL,                                                   /* m_free */
};
#endif

#if PY_MAJOR_VERSION >= 3
#define INITERROR return NULL
#else
#define INITERROR return
#endif

#ifndef PyMODINIT_FUNC /* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif

#if PY_MAJOR_VERSION >= 3
#define MOD_INIT(name) PyMODINIT_FUNC PyInit_##name(void)
#else
#define MOD_INIT(name) PyMODINIT_FUNC init##name(void)
#endif

MOD_INIT(common) {
  PyObject *m;

  if (PyType_Ready(&PyTaskType) < 0) {
    INITERROR;
  }

  if (PyType_Ready(&PyObjectIDType) < 0) {
    INITERROR;
  }

#if PY_MAJOR_VERSION >= 3
  m = PyModule_Create(&moduledef);
#else
  m = Py_InitModule3("common", common_methods,
                     "A module for common types. This is used for testing.");
#endif

  init_pickle_module();

  Py_INCREF(&PyTaskType);
  PyModule_AddObject(m, "Task", (PyObject *) &PyTaskType);

  Py_INCREF(&PyObjectIDType);
  PyModule_AddObject(m, "ObjectID", (PyObject *) &PyObjectIDType);

  char common_error[] = "common.error";
  CommonError = PyErr_NewException(common_error, NULL, NULL);
  Py_INCREF(CommonError);
  PyModule_AddObject(m, "common_error", CommonError);

#if PY_MAJOR_VERSION >= 3
  return m;
#endif
}
