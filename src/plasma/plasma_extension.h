#ifndef PLASMA_EXTENSION_H
#define PLASMA_EXTENSION_H

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

static int PyStringToUniqueID(PyObject *object, object_id *object_id) {
  if (PyBytes_Check(object)) {
    memcpy(&object_id->id[0], PyBytes_AsString(object), UNIQUE_ID_SIZE);
    return 1;
  } else {
    PyErr_SetString(PyExc_TypeError, "must be a 20 character string");
    return 0;
  }
}

#endif /* PLASMA_EXTENSION_H */
