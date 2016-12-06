#ifndef PLASMA_EXTENSION_H
#define PLASMA_EXTENSION_H

/* If this is the context of the PyCapsule for plasma connections,
 * it means that the connection has been disconnected. */
#define CONN_CAPSULE_DISCONNECTED ((void *) 1)

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

#endif /* PLASMA_EXTENSION_H */
