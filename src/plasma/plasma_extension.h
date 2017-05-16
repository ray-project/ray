#ifndef PLASMA_EXTENSION_H
#define PLASMA_EXTENSION_H

static int PyObjectToPlasmaConnection(PyObject *object,
                                      PlasmaConnection **conn) {
  if (PyCapsule_IsValid(object, "plasma")) {
    *conn = (PlasmaConnection *) PyCapsule_GetPointer(object, "plasma");
    return 1;
  } else {
    PyErr_SetString(PyExc_TypeError, "must be a 'plasma' capsule");
    return 0;
  }
}

int PyStringToUniqueID(PyObject *object, ObjectID *object_id) {
  if (PyBytes_Check(object)) {
    memcpy(object_id, PyBytes_AsString(object), sizeof(ObjectID));
    return 1;
  } else {
    PyErr_SetString(PyExc_TypeError, "must be a 20 character string");
    return 0;
  }
}

#endif /* PLASMA_EXTENSION_H */
