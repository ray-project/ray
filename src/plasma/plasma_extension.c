#include <Python.h>

#include "common.h"
#include "plasma_client.h"

static int PyObjectToPlasmaConnection(PyObject *object,
                                      plasma_connection **conn) {
  if (PyCapsule_IsValid(object, "plasma")) {
    *conn = (plasma_connection *) PyCapsule_GetPointer(object, "plasma");
    return 1;
  } else {
    PyErr_SetString(PyExc_TypeError, "must be a 'plasma' capsule");
    return 0;
  }
}

static int PyObjectToUniqueID(PyObject *object, object_id *object_id) {
  if (PyString_Check(object)) {
    memcpy(&object_id->id[0], PyString_AsString(object), UNIQUE_ID_SIZE);
    return 1;
  } else {
    PyErr_SetString(PyExc_TypeError, "must be a 20 character string");
    return 0;
  }
}

PyObject *PyPlasma_connect(PyObject *self, PyObject *args) {
  const char *store_socket_name;
  const char *manager_socket_name;
  int release_delay;
  if (!PyArg_ParseTuple(args, "ssi", &store_socket_name, &manager_socket_name,
                        &release_delay)) {
    return NULL;
  }
  plasma_connection *conn;
  if (strlen(manager_socket_name) == 0) {
    conn = plasma_connect(store_socket_name, NULL, release_delay);
  } else {
    conn =
        plasma_connect(store_socket_name, manager_socket_name, release_delay);
  }
  return PyCapsule_New(conn, "plasma", NULL);
}

PyObject *PyPlasma_disconnect(PyObject *self, PyObject *args) {
  plasma_connection *conn;
  if (!PyArg_ParseTuple(args, "O&", PyObjectToPlasmaConnection, &conn)) {
    return NULL;
  }
  plasma_disconnect(conn);
  Py_RETURN_NONE;
}

PyObject *PyPlasma_create(PyObject *self, PyObject *args) {
  plasma_connection *conn;
  object_id object_id;
  long long size;
  PyObject *metadata;
  if (!PyArg_ParseTuple(args, "O&O&LO", PyObjectToPlasmaConnection, &conn,
                        PyObjectToUniqueID, &object_id, &size, &metadata)) {
    return NULL;
  }
  if (!PyByteArray_Check(metadata)) {
    PyErr_SetString(PyExc_TypeError, "metadata must be a bytearray");
    return NULL;
  }
  uint8_t *data;
  plasma_create(conn, object_id, size,
                (uint8_t *) PyByteArray_AsString(metadata),
                PyByteArray_Size(metadata), &data);
  return PyBuffer_FromReadWriteMemory((void *) data, (Py_ssize_t) size);
}

PyObject *PyPlasma_hash(PyObject *self, PyObject *args) {
  plasma_connection *conn;
  object_id object_id;
  if (!PyArg_ParseTuple(args, "O&O&", PyObjectToPlasmaConnection, &conn,
                        PyObjectToUniqueID, &object_id)) {
    return NULL;
  }
  unsigned char digest[DIGEST_SIZE + 1];
  plasma_compute_object_hash(conn, object_id, digest);
  digest[DIGEST_SIZE] = '\0';
  if (strcmp(digest, "") == 0) {
    Py_RETURN_NONE;
  }
  PyObject *digest_string = PyString_FromString(digest);
  return digest_string;
}

PyObject *PyPlasma_seal(PyObject *self, PyObject *args) {
  plasma_connection *conn;
  object_id object_id;
  if (!PyArg_ParseTuple(args, "O&O&", PyObjectToPlasmaConnection, &conn,
                        PyObjectToUniqueID, &object_id)) {
    return NULL;
  }
  plasma_seal(conn, object_id);
  Py_RETURN_NONE;
}

PyObject *PyPlasma_release(PyObject *self, PyObject *args) {
  plasma_connection *conn;
  object_id object_id;
  if (!PyArg_ParseTuple(args, "O&O&", PyObjectToPlasmaConnection, &conn,
                        PyObjectToUniqueID, &object_id)) {
    return NULL;
  }
  plasma_release(conn, object_id);
  Py_RETURN_NONE;
}

PyObject *PyPlasma_get(PyObject *self, PyObject *args) {
  plasma_connection *conn;
  object_id object_id;
  if (!PyArg_ParseTuple(args, "O&O&", PyObjectToPlasmaConnection, &conn,
                        PyObjectToUniqueID, &object_id)) {
    return NULL;
  }
  int64_t size;
  uint8_t *data;
  int64_t metadata_size;
  uint8_t *metadata;
  plasma_get(conn, object_id, &size, &data, &metadata_size, &metadata);
  PyObject *t = PyTuple_New(2);
  PyTuple_SetItem(t, 0, PyBuffer_FromMemory((void *) data, (Py_ssize_t) size));
  PyTuple_SetItem(t, 1, PyByteArray_FromStringAndSize(
                            (void *) metadata, (Py_ssize_t) metadata_size));
  return t;
}

PyObject *PyPlasma_contains(PyObject *self, PyObject *args) {
  plasma_connection *conn;
  object_id object_id;
  if (!PyArg_ParseTuple(args, "O&O&", PyObjectToPlasmaConnection, &conn,
                        PyObjectToUniqueID, &object_id)) {
    return NULL;
  }
  int has_object;
  plasma_contains(conn, object_id, &has_object);

  if (has_object)
    Py_RETURN_TRUE;
  else
    Py_RETURN_FALSE;
}

PyObject *PyPlasma_fetch(PyObject *self, PyObject *args) {
  plasma_connection *conn;
  PyObject *object_id_list;
  if (!PyArg_ParseTuple(args, "O&O", PyObjectToPlasmaConnection, &conn,
                        &object_id_list)) {
    return NULL;
  }
  if (!plasma_manager_is_connected(conn)) {
    PyErr_SetString(PyExc_RuntimeError, "Not connected to the plasma manager");
    return NULL;
  }
  Py_ssize_t n = PyList_Size(object_id_list);
  object_id *object_ids = malloc(sizeof(object_id) * n);
  for (int i = 0; i < n; ++i) {
    PyObjectToUniqueID(PyList_GetItem(object_id_list, i), &object_ids[i]);
  }
  /* Check that there are no duplicate object IDs. TODO(rkn): we should allow
   * this in the future. */
  if (!plasma_object_ids_distinct(n, object_ids)) {
    PyErr_SetString(PyExc_RuntimeError,
                    "The same object ID is used multiple times in this call to "
                    "fetch.");
    return NULL;
  }
  int *success_array = malloc(sizeof(int) * n);
  memset(success_array, 0, sizeof(int) * n);
  plasma_fetch(conn, (int) n, object_ids, success_array);
  PyObject *success_list = PyList_New(n);
  for (int i = 0; i < n; ++i) {
    if (success_array[i]) {
      Py_INCREF(Py_True);
      PyList_SetItem(success_list, i, Py_True);
    } else {
      Py_INCREF(Py_False);
      PyList_SetItem(success_list, i, Py_False);
    }
  }
  free(object_ids);
  free(success_array);
  return success_list;
}

PyObject *PyPlasma_fetch2(PyObject *self, PyObject *args) {
  plasma_connection *conn;
  PyObject *object_id_list;
  if (!PyArg_ParseTuple(args, "O&O", PyObjectToPlasmaConnection, &conn,
                        &object_id_list)) {
    return NULL;
  }
  if (!plasma_manager_is_connected(conn)) {
    PyErr_SetString(PyExc_RuntimeError, "Not connected to the plasma manager");
    return NULL;
  }
  Py_ssize_t n = PyList_Size(object_id_list);
  object_id *object_ids = malloc(sizeof(object_id) * n);
  for (int i = 0; i < n; ++i) {
    PyObjectToUniqueID(PyList_GetItem(object_id_list, i), &object_ids[i]);
  }
  plasma_fetch2(conn, (int) n, object_ids);
  free(object_ids);
  Py_RETURN_NONE;
}

PyObject *PyPlasma_wait(PyObject *self, PyObject *args) {
  plasma_connection *conn;
  PyObject *object_id_list;
  long long timeout;
  int num_returns;
  if (!PyArg_ParseTuple(args, "O&OLi", PyObjectToPlasmaConnection, &conn,
                        &object_id_list, &timeout, &num_returns)) {
    return NULL;
  }
  Py_ssize_t n = PyList_Size(object_id_list);

  if (!plasma_manager_is_connected(conn)) {
    PyErr_SetString(PyExc_RuntimeError, "Not connected to the plasma manager");
    return NULL;
  }
  if (num_returns < 0) {
    PyErr_SetString(PyExc_RuntimeError,
                    "The argument num_returns cannot be less than zero.");
    return NULL;
  }
  if (num_returns > n) {
    PyErr_SetString(
        PyExc_RuntimeError,
        "The argument num_returns cannot be greater than len(object_ids)");
    return NULL;
  }
  int64_t threshold = 1 << 30;
  if (timeout > threshold) {
    PyErr_SetString(PyExc_RuntimeError,
                    "The argument timeout cannot be greater than 2 ** 30.");
    return NULL;
  }

  object_id *object_ids = malloc(sizeof(object_id) * n);
  for (int i = 0; i < n; ++i) {
    PyObjectToUniqueID(PyList_GetItem(object_id_list, i), &object_ids[i]);
  }
  object_id *return_ids = malloc(sizeof(object_id) * num_returns);

  /* Drop the global interpreter lock while we are waiting, so other threads can
   * run. */
  int num_return_objects;
  Py_BEGIN_ALLOW_THREADS;
  num_return_objects = plasma_wait(conn, (int) n, object_ids,
                                   (uint64_t) timeout, num_returns, return_ids);
  Py_END_ALLOW_THREADS;

  PyObject *ready_ids = PyList_New(num_return_objects);
  PyObject *waiting_ids = PySet_New(object_id_list);
  for (int i = num_returns - num_return_objects; i < num_returns; ++i) {
    PyObject *ready =
        PyString_FromStringAndSize((char *) return_ids[i].id, UNIQUE_ID_SIZE);
    PyList_SetItem(ready_ids, i - (num_returns - num_return_objects), ready);
    PySet_Discard(waiting_ids, ready);
  }
  PyObject *t = PyTuple_New(2);
  PyTuple_SetItem(t, 0, ready_ids);
  PyTuple_SetItem(t, 1, waiting_ids);
  return t;
}

PyObject *PyPlasma_evict(PyObject *self, PyObject *args) {
  plasma_connection *conn;
  long long num_bytes;
  if (!PyArg_ParseTuple(args, "O&L", PyObjectToPlasmaConnection, &conn,
                        &num_bytes)) {
    return NULL;
  }
  int64_t evicted_bytes = plasma_evict(conn, (int64_t) num_bytes);
  return PyInt_FromLong((long) evicted_bytes);
}

PyObject *PyPlasma_delete(PyObject *self, PyObject *args) {
  plasma_connection *conn;
  object_id object_id;
  if (!PyArg_ParseTuple(args, "O&O&", PyObjectToPlasmaConnection, &conn,
                        PyObjectToUniqueID, &object_id)) {
    return NULL;
  }
  plasma_delete(conn, object_id);
  Py_RETURN_NONE;
}

PyObject *PyPlasma_transfer(PyObject *self, PyObject *args) {
  plasma_connection *conn;
  object_id object_id;
  const char *addr;
  int port;
  if (!PyArg_ParseTuple(args, "O&O&si", PyObjectToPlasmaConnection, &conn,
                        PyObjectToUniqueID, &object_id, &addr, &port)) {
    return NULL;
  }

  if (!plasma_manager_is_connected(conn)) {
    PyErr_SetString(PyExc_RuntimeError, "Not connected to the plasma manager");
    return NULL;
  }

  plasma_transfer(conn, addr, port, object_id);
  Py_RETURN_NONE;
}

PyObject *PyPlasma_subscribe(PyObject *self, PyObject *args) {
  plasma_connection *conn;
  if (!PyArg_ParseTuple(args, "O&", PyObjectToPlasmaConnection, &conn)) {
    return NULL;
  }
  int sock = plasma_subscribe(conn);
  return PyInt_FromLong(sock);
}

static PyMethodDef plasma_methods[] = {
    {"connect", PyPlasma_connect, METH_VARARGS, "Connect to plasma."},
    {"disconnect", PyPlasma_disconnect, METH_VARARGS,
     "Disconnect from plasma."},
    {"create", PyPlasma_create, METH_VARARGS, "Create a new plasma object."},
    {"hash", PyPlasma_hash, METH_VARARGS,
     "Compute the hash of a plasma object."},
    {"seal", PyPlasma_seal, METH_VARARGS, "Seal a plasma object."},
    {"get", PyPlasma_get, METH_VARARGS, "Get a plasma object."},
    {"contains", PyPlasma_contains, METH_VARARGS,
     "Does the plasma store contain this plasma object?"},
    {"fetch", PyPlasma_fetch, METH_VARARGS,
     "Fetch the object from another plasma manager instance."},
    {"fetch2", PyPlasma_fetch2, METH_VARARGS,
     "Fetch the object from another plasma manager instance."},
    {"wait", PyPlasma_wait, METH_VARARGS,
     "Wait until num_returns objects in object_ids are ready."},
    {"evict", PyPlasma_evict, METH_VARARGS,
     "Evict some objects until we recover some number of bytes."},
    {"release", PyPlasma_release, METH_VARARGS, "Release the plasma object."},
    {"delete", PyPlasma_delete, METH_VARARGS, "Deleta a plasma object."},
    {"transfer", PyPlasma_transfer, METH_VARARGS,
     "Transfer object to another plasma manager."},
    {"subscribe", PyPlasma_subscribe, METH_VARARGS,
     "Subscribe to the plasma notification socket."},
    {NULL} /* Sentinel */
};

#ifndef PyMODINIT_FUNC /* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif

PyMODINIT_FUNC initlibplasma(void) {
  Py_InitModule3("libplasma", plasma_methods,
                 "A Python client library for plasma");
}
