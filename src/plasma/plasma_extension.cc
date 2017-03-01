#include <Python.h>
#include "bytesobject.h"

#include "common_extension.h"
#include "common.h"
#include "io.h"
#include "plasma_protocol.h"
#include "plasma_client.h"
#include "object_info.h"

PyObject *PlasmaOutOfMemoryError;
PyObject *PlasmaObjectExistsError;

#include "plasma_extension.h"

PyObject *PyPlasma_connect(PyObject *self, PyObject *args) {
  const char *store_socket_name;
  const char *manager_socket_name;
  int release_delay;
  if (!PyArg_ParseTuple(args, "ssi", &store_socket_name, &manager_socket_name,
                        &release_delay)) {
    return NULL;
  }
  PlasmaConnection *conn;
  if (strlen(manager_socket_name) == 0) {
    conn = plasma_connect(store_socket_name, NULL, release_delay);
  } else {
    conn =
        plasma_connect(store_socket_name, manager_socket_name, release_delay);
  }
  return PyCapsule_New(conn, "plasma", NULL);
}

PyObject *PyPlasma_disconnect(PyObject *self, PyObject *args) {
  PyObject *conn_capsule;
  PlasmaConnection *conn;
  if (!PyArg_ParseTuple(args, "O", &conn_capsule)) {
    return NULL;
  }
  CHECK(PyObjectToPlasmaConnection(conn_capsule, &conn));
  plasma_disconnect(conn);
  /* We use the context of the connection capsule to indicate if the connection
   * is still active (if the context is NULL) or if it is closed (if the context
   * is (void*) 0x1). This is neccessary because the primary pointer of the
   * capsule cannot be NULL. */
  PyCapsule_SetContext(conn_capsule, (void *) 0x1);
  Py_RETURN_NONE;
}

PyObject *PyPlasma_create(PyObject *self, PyObject *args) {
  PlasmaConnection *conn;
  ObjectID object_id;
  long long size;
  PyObject *metadata;
  if (!PyArg_ParseTuple(args, "O&O&LO", PyObjectToPlasmaConnection, &conn,
                        PyStringToUniqueID, &object_id, &size, &metadata)) {
    return NULL;
  }
  if (!PyByteArray_Check(metadata)) {
    PyErr_SetString(PyExc_TypeError, "metadata must be a bytearray");
    return NULL;
  }
  uint8_t *data;
  int error_code = plasma_create(conn, object_id, size,
                                 (uint8_t *) PyByteArray_AsString(metadata),
                                 PyByteArray_Size(metadata), &data);
  if (error_code == PlasmaError_ObjectExists) {
    PyErr_SetString(PlasmaObjectExistsError,
                    "An object with this ID already exists in the plasma "
                    "store.");
    return NULL;
  }
  if (error_code == PlasmaError_OutOfMemory) {
    PyErr_SetString(PlasmaOutOfMemoryError,
                    "The plasma store ran out of memory and could not create "
                    "this object.");
    return NULL;
  }
  CHECK(error_code == PlasmaError_OK);

#if PY_MAJOR_VERSION >= 3
  return PyMemoryView_FromMemory((char *) data, (Py_ssize_t) size, PyBUF_WRITE);
#else
  return PyBuffer_FromReadWriteMemory((void *) data, (Py_ssize_t) size);
#endif
}

PyObject *PyPlasma_hash(PyObject *self, PyObject *args) {
  PlasmaConnection *conn;
  ObjectID object_id;
  if (!PyArg_ParseTuple(args, "O&O&", PyObjectToPlasmaConnection, &conn,
                        PyStringToUniqueID, &object_id)) {
    return NULL;
  }
  unsigned char digest[DIGEST_SIZE];
  bool success = plasma_compute_object_hash(conn, object_id, digest);
  if (success) {
    PyObject *digest_string =
        PyBytes_FromStringAndSize((char *) digest, DIGEST_SIZE);
    return digest_string;
  } else {
    Py_RETURN_NONE;
  }
}

PyObject *PyPlasma_seal(PyObject *self, PyObject *args) {
  PlasmaConnection *conn;
  ObjectID object_id;
  if (!PyArg_ParseTuple(args, "O&O&", PyObjectToPlasmaConnection, &conn,
                        PyStringToUniqueID, &object_id)) {
    return NULL;
  }
  plasma_seal(conn, object_id);
  Py_RETURN_NONE;
}

PyObject *PyPlasma_release(PyObject *self, PyObject *args) {
  PlasmaConnection *conn;
  ObjectID object_id;
  if (!PyArg_ParseTuple(args, "O&O&", PyObjectToPlasmaConnection, &conn,
                        PyStringToUniqueID, &object_id)) {
    return NULL;
  }
  plasma_release(conn, object_id);
  Py_RETURN_NONE;
}

PyObject *PyPlasma_get(PyObject *self, PyObject *args) {
  PlasmaConnection *conn;
  PyObject *object_id_list;
  long long timeout_ms;
  if (!PyArg_ParseTuple(args, "O&OL", PyObjectToPlasmaConnection, &conn,
                        &object_id_list, &timeout_ms)) {
    return NULL;
  }

  Py_ssize_t num_object_ids = PyList_Size(object_id_list);
  ObjectID object_ids[num_object_ids];
  ObjectBuffer object_buffers[num_object_ids];

  for (int i = 0; i < num_object_ids; ++i) {
    PyStringToUniqueID(PyList_GetItem(object_id_list, i), &object_ids[i]);
  }

  Py_BEGIN_ALLOW_THREADS;
  plasma_get(conn, object_ids, num_object_ids, timeout_ms, object_buffers);
  Py_END_ALLOW_THREADS;

  PyObject *returns = PyList_New(num_object_ids);
  for (int i = 0; i < num_object_ids; ++i) {
    if (object_buffers[i].data_size != -1) {
      /* The object was retrieved, so return the object. */
      PyObject *t = PyTuple_New(2);
#if PY_MAJOR_VERSION >= 3
      PyTuple_SetItem(
          t, 0, PyMemoryView_FromMemory(
                    (char *) object_buffers[i].data,
                    (Py_ssize_t) object_buffers[i].data_size, PyBUF_READ));
      PyTuple_SetItem(
          t, 1, PyMemoryView_FromMemory(
                    (char *) object_buffers[i].metadata,
                    (Py_ssize_t) object_buffers[i].metadata_size, PyBUF_READ));
#else
      PyTuple_SetItem(
          t, 0, PyBuffer_FromMemory((void *) object_buffers[i].data,
                                    (Py_ssize_t) object_buffers[i].data_size));
      PyTuple_SetItem(t, 1, PyBuffer_FromMemory(
                                (void *) object_buffers[i].metadata,
                                (Py_ssize_t) object_buffers[i].metadata_size));
#endif
      PyList_SetItem(returns, i, t);
    } else {
      /* The object was not retrieved, so just add None to the list of return
       * values. */
      Py_XINCREF(Py_None);
      PyList_SetItem(returns, i, Py_None);
    }
  }
  return returns;
}

PyObject *PyPlasma_contains(PyObject *self, PyObject *args) {
  PlasmaConnection *conn;
  ObjectID object_id;
  if (!PyArg_ParseTuple(args, "O&O&", PyObjectToPlasmaConnection, &conn,
                        PyStringToUniqueID, &object_id)) {
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
  PlasmaConnection *conn;
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
  ObjectID *object_ids = (ObjectID *) malloc(sizeof(ObjectID) * n);
  for (int i = 0; i < n; ++i) {
    PyStringToUniqueID(PyList_GetItem(object_id_list, i), &object_ids[i]);
  }
  plasma_fetch(conn, (int) n, object_ids);
  free(object_ids);
  Py_RETURN_NONE;
}

PyObject *PyPlasma_wait(PyObject *self, PyObject *args) {
  PlasmaConnection *conn;
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

  ObjectRequest *object_requests = (ObjectRequest *) malloc(sizeof(ObjectRequest) * n);
  for (int i = 0; i < n; ++i) {
    CHECK(PyStringToUniqueID(PyList_GetItem(object_id_list, i),
                             &object_requests[i].object_id) == 1);
    object_requests[i].type = PLASMA_QUERY_ANYWHERE;
  }
  /* Drop the global interpreter lock while we are waiting, so other threads can
   * run. */
  int num_return_objects;
  Py_BEGIN_ALLOW_THREADS;
  num_return_objects = plasma_wait(conn, (int) n, object_requests, num_returns,
                                   (uint64_t) timeout);
  Py_END_ALLOW_THREADS;

  int num_to_return = MIN(num_return_objects, num_returns);
  PyObject *ready_ids = PyList_New(num_to_return);
  PyObject *waiting_ids = PySet_New(object_id_list);
  int num_returned = 0;
  for (int i = 0; i < n; ++i) {
    if (num_returned == num_to_return) {
      break;
    }
    if (object_requests[i].status == ObjectStatus_Local ||
        object_requests[i].status == ObjectStatus_Remote) {
      PyObject *ready =
          PyBytes_FromStringAndSize((char *) object_requests[i].object_id.id,
                                    sizeof(object_requests[i].object_id));
      PyList_SetItem(ready_ids, num_returned, ready);
      PySet_Discard(waiting_ids, ready);
      num_returned += 1;
    } else {
      CHECK(object_requests[i].status == ObjectStatus_Nonexistent);
    }
  }
  CHECK(num_returned == num_to_return);
  /* Return both the ready IDs and the remaining IDs. */
  PyObject *t = PyTuple_New(2);
  PyTuple_SetItem(t, 0, ready_ids);
  PyTuple_SetItem(t, 1, waiting_ids);
  return t;
}

PyObject *PyPlasma_evict(PyObject *self, PyObject *args) {
  PlasmaConnection *conn;
  long long num_bytes;
  if (!PyArg_ParseTuple(args, "O&L", PyObjectToPlasmaConnection, &conn,
                        &num_bytes)) {
    return NULL;
  }
  int64_t evicted_bytes = plasma_evict(conn, (int64_t) num_bytes);
  return PyLong_FromLong((long) evicted_bytes);
}

PyObject *PyPlasma_delete(PyObject *self, PyObject *args) {
  PlasmaConnection *conn;
  ObjectID object_id;
  if (!PyArg_ParseTuple(args, "O&O&", PyObjectToPlasmaConnection, &conn,
                        PyStringToUniqueID, &object_id)) {
    return NULL;
  }
  plasma_delete(conn, object_id);
  Py_RETURN_NONE;
}

PyObject *PyPlasma_transfer(PyObject *self, PyObject *args) {
  PlasmaConnection *conn;
  ObjectID object_id;
  const char *addr;
  int port;
  if (!PyArg_ParseTuple(args, "O&O&si", PyObjectToPlasmaConnection, &conn,
                        PyStringToUniqueID, &object_id, &addr, &port)) {
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
  PlasmaConnection *conn;
  if (!PyArg_ParseTuple(args, "O&", PyObjectToPlasmaConnection, &conn)) {
    return NULL;
  }

  int sock = plasma_subscribe(conn);
  return PyLong_FromLong(sock);
}

PyObject *PyPlasma_receive_notification(PyObject *self, PyObject *args) {
  int plasma_sock;
  ObjectInfo object_info;

  if (!PyArg_ParseTuple(args, "i", &plasma_sock)) {
    return NULL;
  }
  /* Receive object notification from the plasma connection socket. If the
   * object was added, return a tuple of its fields: ObjectID, data_size,
   * metadata_size. If the object was deleted, data_size and metadata_size will
   * be set to -1. */
  int nbytes =
      read_bytes(plasma_sock, (uint8_t *) &object_info, sizeof(object_info));

  if (nbytes < 0) {
    PyErr_SetString(PyExc_RuntimeError,
                    "Failed to read object notification from Plasma socket");
    return NULL;
  }
  /* Construct a tuple from object_info and return. */
  PyObject *t = PyTuple_New(3);
  PyTuple_SetItem(t, 0, PyBytes_FromStringAndSize(
                            (char *) object_info.obj_id.id, UNIQUE_ID_SIZE));
  if (object_info.is_deletion) {
    PyTuple_SetItem(t, 1, PyLong_FromLong(-1));
    PyTuple_SetItem(t, 2, PyLong_FromLong(-1));
  } else {
    PyTuple_SetItem(t, 1, PyLong_FromLong(object_info.data_size));
    PyTuple_SetItem(t, 2, PyLong_FromLong(object_info.metadata_size));
  }

  return t;
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
    {"wait", PyPlasma_wait, METH_VARARGS,
     "Wait until num_returns objects in object_ids are ready."},
    {"evict", PyPlasma_evict, METH_VARARGS,
     "Evict some objects until we recover some number of bytes."},
    {"release", PyPlasma_release, METH_VARARGS, "Release the plasma object."},
    {"delete", PyPlasma_delete, METH_VARARGS, "Delete a plasma object."},
    {"transfer", PyPlasma_transfer, METH_VARARGS,
     "Transfer object to another plasma manager."},
    {"subscribe", PyPlasma_subscribe, METH_VARARGS,
     "Subscribe to the plasma notification socket."},
    {"receive_notification", PyPlasma_receive_notification, METH_VARARGS,
     "Receive next notification from plasma notification socket."},
    {NULL} /* Sentinel */
};

#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "libplasma",                           /* m_name */
    "A Python client library for plasma.", /* m_doc */
    0,                                     /* m_size */
    plasma_methods,                        /* m_methods */
    NULL,                                  /* m_reload */
    NULL,                                  /* m_traverse */
    NULL,                                  /* m_clear */
    NULL,                                  /* m_free */
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

MOD_INIT(libplasma) {
#if PY_MAJOR_VERSION >= 3
  PyObject *m = PyModule_Create(&moduledef);
#else
  PyObject *m = Py_InitModule3("libplasma", plasma_methods,
                               "A Python client library for plasma.");
#endif

  /* Create a custom exception for when an object ID is reused. */
  char plasma_object_exists_error[] = "plasma_object_exists.error";
  PlasmaObjectExistsError =
      PyErr_NewException(plasma_object_exists_error, NULL, NULL);
  Py_INCREF(PlasmaObjectExistsError);
  PyModule_AddObject(m, "plasma_object_exists_error", PlasmaObjectExistsError);
  /* Create a custom exception for when the plasma store is out of memory. */
  char plasma_out_of_memory_error[] = "plasma_out_of_memory.error";
  PlasmaOutOfMemoryError =
      PyErr_NewException(plasma_out_of_memory_error, NULL, NULL);
  Py_INCREF(PlasmaOutOfMemoryError);
  PyModule_AddObject(m, "plasma_out_of_memory_error", PlasmaOutOfMemoryError);

#if PY_MAJOR_VERSION >= 3
  return m;
#endif
}
