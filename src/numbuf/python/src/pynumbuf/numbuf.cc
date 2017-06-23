#include <Python.h>
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#define PY_ARRAY_UNIQUE_SYMBOL arrow_ARRAY_API
#include <numpy/arrayobject.h>

#include "bytesobject.h"

#include <iostream>

#ifdef HAS_PLASMA
// This needs to be included before plasma_protocol. We cannot include it in
// plasma_protocol, because that file is used both with the store and the
// manager, the store uses it the ObjectID from plasma_common.h and the
// manager uses it with the ObjectID from common.h.
#include "plasma/common.h"

#include "plasma/client.h"
#include "plasma/protocol.h"

extern "C" {
PyObject* NumbufPlasmaOutOfMemoryError;
PyObject* NumbufPlasmaObjectExistsError;
}

#include "plasma/extension.h"

#endif

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/util.h>
#include <arrow/ipc/writer.h>
#include <arrow/python/numpy_convert.h>

#include "adapters/python.h"

using namespace arrow;
using namespace numbuf;

struct RayObject {
  std::shared_ptr<RecordBatch> batch;
  std::vector<PyObject*> arrays;
  std::vector<std::shared_ptr<Tensor>> tensors;
};

// Each arrow object is stored in the format
// | length of the object in bytes | object data |.
// LENGTH_PREFIX_SIZE is the number of bytes occupied by the
// object length field.
constexpr int64_t LENGTH_PREFIX_SIZE = sizeof(int64_t);

std::shared_ptr<RecordBatch> make_batch(std::shared_ptr<Array> data) {
  auto field = std::make_shared<Field>("list", data->type());
  std::shared_ptr<Schema> schema(new Schema({field}));
  return std::shared_ptr<RecordBatch>(new RecordBatch(schema, data->length(), {data}));
}

Status write_batch_and_tensors(io::OutputStream* stream,
    std::shared_ptr<RecordBatch> batch, const std::vector<PyObject*>& tensors,
    int64_t* batch_size, int64_t* total_size) {
  std::shared_ptr<arrow::ipc::FileWriter> writer;
  RETURN_NOT_OK(ipc::FileWriter::Open(stream, batch->schema(), &writer));
  RETURN_NOT_OK(writer->WriteRecordBatch(*batch, true));
  RETURN_NOT_OK(writer->Close());
  RETURN_NOT_OK(stream->Tell(batch_size));
  for (auto array : tensors) {
    int32_t metadata_length;
    int64_t body_length;
    std::shared_ptr<Tensor> tensor;
    auto contiguous = (PyObject*)PyArray_GETCONTIGUOUS((PyArrayObject*)array);
    RETURN_NOT_OK(py::NdarrayToTensor(NULL, contiguous, &tensor));
    RETURN_NOT_OK(ipc::WriteTensor(*tensor, stream, &metadata_length, &body_length));
    Py_XDECREF(contiguous);
  }
  RETURN_NOT_OK(stream->Tell(total_size));
  return Status::OK();
}

Status read_batch_and_tensors(uint8_t* data, int64_t size,
    std::shared_ptr<RecordBatch>* batch_out,
    std::vector<std::shared_ptr<Tensor>>& tensors_out) {
  std::shared_ptr<arrow::ipc::FileReader> reader;
  int64_t batch_size = *((int64_t*)data);
  auto source = std::make_shared<arrow::io::BufferReader>(
      LENGTH_PREFIX_SIZE + data, size - LENGTH_PREFIX_SIZE);
  RETURN_NOT_OK(arrow::ipc::FileReader::Open(source, batch_size, &reader));
  RETURN_NOT_OK(reader->GetRecordBatch(0, batch_out));
  int64_t offset = batch_size;
  while (true) {
    std::shared_ptr<Tensor> tensor;
    Status s = ipc::ReadTensor(offset, source.get(), &tensor);
    if (!s.ok()) { break; }
    tensors_out.push_back(tensor);
    RETURN_NOT_OK(source->Tell(&offset));
  }
  return Status::OK();
}

extern "C" {

#define CHECK_SERIALIZATION_ERROR(STATUS)                                             \
  do {                                                                                \
    Status _s = (STATUS);                                                             \
    if (!_s.ok()) {                                                                   \
      /* If this condition is true, there was an error in the callback that           \
       * needs to be passed through */                                                \
      if (!PyErr_Occurred()) { PyErr_SetString(NumbufError, _s.ToString().c_str()); } \
      return NULL;                                                                    \
    }                                                                                 \
  } while (0)

static PyObject* NumbufError;

PyObject* numbuf_serialize_callback = NULL;
PyObject* numbuf_deserialize_callback = NULL;

int PyObjectToArrow(PyObject* object, RayObject** result) {
  if (PyCapsule_IsValid(object, "arrow")) {
    *result = reinterpret_cast<RayObject*>(PyCapsule_GetPointer(object, "arrow"));
    return 1;
  } else {
    PyErr_SetString(PyExc_TypeError, "must be an 'arrow' capsule");
    return 0;
  }
}

static void ArrowCapsule_Destructor(PyObject* capsule) {
  delete reinterpret_cast<RayObject*>(PyCapsule_GetPointer(capsule, "arrow"));
}

/* Documented in doc/numbuf.rst in ray-core */
static PyObject* serialize_list(PyObject* self, PyObject* args) {
  PyObject* value;
  if (!PyArg_ParseTuple(args, "O", &value)) { return NULL; }
  std::shared_ptr<Array> array;
  if (PyList_Check(value)) {
    RayObject* object = new RayObject();
    int32_t recursion_depth = 0;
    Status s = SerializeSequences(
        std::vector<PyObject*>({value}), recursion_depth, &array, object->arrays);
    CHECK_SERIALIZATION_ERROR(s);

    for (auto array : object->arrays) {
      int32_t metadata_length;
      int64_t body_length;
      std::shared_ptr<Tensor> tensor;
      ARROW_CHECK_OK(py::NdarrayToTensor(NULL, array, &tensor));
      object->tensors.push_back(tensor);
    }

    object->batch = make_batch(array);

    int64_t data_size, total_size;
    auto mock = std::make_shared<arrow::ipc::MockOutputStream>();
    write_batch_and_tensors(
        mock.get(), object->batch, object->arrays, &data_size, &total_size);

    PyObject* r = PyTuple_New(2);
    PyTuple_SetItem(r, 0, PyLong_FromLong(LENGTH_PREFIX_SIZE + total_size));
    PyTuple_SetItem(r, 1, PyCapsule_New(reinterpret_cast<void*>(object), "arrow",
                              &ArrowCapsule_Destructor));
    return r;
  }
  return NULL;
}

/* Documented in doc/numbuf.rst in ray-core */
static PyObject* write_to_buffer(PyObject* self, PyObject* args) {
  RayObject* object;
  PyObject* memoryview;
  if (!PyArg_ParseTuple(args, "O&O", &PyObjectToArrow, &object, &memoryview)) {
    return NULL;
  }
  if (!PyMemoryView_Check(memoryview)) { return NULL; }
  Py_buffer* buffer = PyMemoryView_GET_BUFFER(memoryview);
  auto buf = std::make_shared<arrow::MutableBuffer>(
      LENGTH_PREFIX_SIZE + reinterpret_cast<uint8_t*>(buffer->buf),
      buffer->len - LENGTH_PREFIX_SIZE);
  auto target = std::make_shared<arrow::io::FixedSizeBufferWriter>(buf);
  target->set_memcopy_threads(8);
  int64_t batch_size, total_size;
  ARROW_CHECK_OK(write_batch_and_tensors(
      target.get(), object->batch, object->arrays, &batch_size, &total_size));
  *((int64_t*)buffer->buf) = buffer->len - LENGTH_PREFIX_SIZE;
  Py_RETURN_NONE;
}

/* Documented in doc/numbuf.rst in ray-core */
static PyObject* read_from_buffer(PyObject* self, PyObject* args) {
  PyObject* data_memoryview;
  if (!PyArg_ParseTuple(args, "O", &data_memoryview)) { return NULL; }

  Py_buffer* data_buffer = PyMemoryView_GET_BUFFER(data_memoryview);

  RayObject* object = new RayObject();
  ARROW_CHECK_OK(read_batch_and_tensors(reinterpret_cast<uint8_t*>(data_buffer->buf),
      data_buffer->len, &object->batch, object->tensors));

  return PyCapsule_New(
      reinterpret_cast<void*>(object), "arrow", &ArrowCapsule_Destructor);
}

/* Documented in doc/numbuf.rst in ray-core */
static PyObject* deserialize_list(PyObject* self, PyObject* args) {
  RayObject* object;
  PyObject* base = Py_None;
  if (!PyArg_ParseTuple(args, "O&|O", &PyObjectToArrow, &object, &base)) { return NULL; }
  PyObject* result;
  Status s = DeserializeList(object->batch->column(0), 0, object->batch->num_rows(), base,
      object->tensors, &result);
  CHECK_SERIALIZATION_ERROR(s);
  return result;
}

static PyObject* register_callbacks(PyObject* self, PyObject* args) {
  PyObject* result = NULL;
  PyObject* serialize_callback;
  PyObject* deserialize_callback;
  if (PyArg_ParseTuple(
          args, "OO:register_callbacks", &serialize_callback, &deserialize_callback)) {
    if (!PyCallable_Check(serialize_callback)) {
      PyErr_SetString(PyExc_TypeError, "serialize_callback must be callable");
      return NULL;
    }
    if (!PyCallable_Check(deserialize_callback)) {
      PyErr_SetString(PyExc_TypeError, "deserialize_callback must be callable");
      return NULL;
    }
    Py_XINCREF(serialize_callback);    // Add a reference to new serialization callback
    Py_XINCREF(deserialize_callback);  // Add a reference to new deserialization callback
    Py_XDECREF(numbuf_serialize_callback);    // Dispose of old serialization callback
    Py_XDECREF(numbuf_deserialize_callback);  // Dispose of old deserialization callback
    numbuf_serialize_callback = serialize_callback;
    numbuf_deserialize_callback = deserialize_callback;
    Py_INCREF(Py_None);
    result = Py_None;
  }
  return result;
}

#ifdef HAS_PLASMA

/**
 * Release the object when its associated PyCapsule goes out of scope.
 *
 * The PyCapsule is used as the base object for the Python object that
 * is stored with store_list and retrieved with retrieve_list. The base
 * object ensures that the reference count of the capsule is non-zero
 * during the lifetime of the Python object returned by retrieve_list.
 *
 * @param capsule The capsule that went out of scope.
 * @return Void.
 */
static void BufferCapsule_Destructor(PyObject* capsule) {
  ObjectID* id = reinterpret_cast<ObjectID*>(PyCapsule_GetPointer(capsule, "buffer"));
  auto context = reinterpret_cast<PyObject*>(PyCapsule_GetContext(capsule));
  /* We use the context of the connection capsule to indicate if the connection
   * is still active (if the context is NULL) or if it is closed (if the context
   * is (void*) 0x1). This is neccessary because the primary pointer of the
   * capsule cannot be NULL. */
  if (PyCapsule_GetContext(context) == NULL) {
    PlasmaClient* client;
    ARROW_CHECK(PyObjectToPlasmaClient(context, &client));
    ARROW_CHECK_OK(client->Release(*id));
  }
  Py_XDECREF(context);
  delete id;
}

/**
 * Store a PyList in the plasma store.
 *
 * This function converts the PyList into an arrow RecordBatch, constructs the
 * metadata (schema) of the PyList, creates a new plasma object, puts the data
 * into the plasma buffer and the schema into the plasma metadata. This raises
 *
 *
 * @param args Contains the object ID the list is stored under, the
 *        connection to the plasma store and the PyList we want to store.
 * @return None.
 */
static PyObject* store_list(PyObject* self, PyObject* args) {
  ObjectID obj_id;
  PlasmaClient* client;
  PyObject* value;
  if (!PyArg_ParseTuple(args, "O&O&O", PyStringToUniqueID, &obj_id,
          PyObjectToPlasmaClient, &client, &value)) {
    return NULL;
  }
  if (!PyList_Check(value)) { return NULL; }

  std::shared_ptr<Array> array;
  int32_t recursion_depth = 0;
  std::vector<PyObject*> tensors;
  Status s = SerializeSequences(
      std::vector<PyObject*>({value}), recursion_depth, &array, tensors);
  CHECK_SERIALIZATION_ERROR(s);

  std::shared_ptr<RecordBatch> batch = make_batch(array);

  int64_t data_size, total_size;
  auto mock = std::make_shared<arrow::ipc::MockOutputStream>();
  write_batch_and_tensors(mock.get(), batch, tensors, &data_size, &total_size);

  uint8_t* data;
  /* The arrow schema is stored as the metadata of the plasma object and
   * both the arrow data and the header end offset are
   * stored in the plasma data buffer. The header end offset is stored in
   * the first LENGTH_PREFIX_SIZE bytes of the data buffer. The RecordBatch
   * data is stored after that. */
  s = client->Create(obj_id, LENGTH_PREFIX_SIZE + total_size, NULL, 0, &data);
  if (s.IsPlasmaObjectExists()) {
    PyErr_SetString(NumbufPlasmaObjectExistsError,
        "An object with this ID already exists in the plasma "
        "store.");
    return NULL;
  }
  if (s.IsPlasmaStoreFull()) {
    PyErr_SetString(NumbufPlasmaOutOfMemoryError,
        "The plasma store ran out of memory and could not create "
        "this object.");
    return NULL;
  }
  ARROW_CHECK_OK(s);

  auto buf =
      std::make_shared<arrow::MutableBuffer>(LENGTH_PREFIX_SIZE + data, total_size);
  auto target = std::make_shared<arrow::io::FixedSizeBufferWriter>(buf);
  target->set_memcopy_threads(8);
  write_batch_and_tensors(target.get(), batch, tensors, &data_size, &total_size);
  *((int64_t*)data) = data_size;

  /* Do the plasma_release corresponding to the call to plasma_create. */
  ARROW_CHECK_OK(client->Release(obj_id));
  /* Seal the object. */
  ARROW_CHECK_OK(client->Seal(obj_id));
  Py_RETURN_NONE;
}

/**
 * Retrieve a PyList from the plasma store.
 *
 * This reads the arrow schema from the plasma metadata, constructs
 * Python objects from the plasma data according to the schema and
 * returns the object.
 *
 * @param args The arguments are, in order:
 *        1) A list of object IDs of the lists to be retrieved.
 *        2) The connection to the plasma store.
 *        3) A timeout in milliseconds that the call should return by. This is
 *           -1 if the call should block forever, or 0 if the call should
 *           return immediately.
 * @return A list of tuples, where the first element in the tuple is the object
 *         ID (appearing in the same order as in the argument to the method),
 *         and the second element in the tuple is the retrieved list (or None)
 *         if no value was retrieved.
 */
static PyObject* retrieve_list(PyObject* self, PyObject* args) {
  PyObject* object_id_list;
  PyObject* plasma_client;
  long long timeout_ms;
  if (!PyArg_ParseTuple(args, "OOL", &object_id_list, &plasma_client, &timeout_ms)) {
    return NULL;
  }
  PlasmaClient* client;
  if (!PyObjectToPlasmaClient(plasma_client, &client)) { return NULL; }

  Py_ssize_t num_object_ids = PyList_Size(object_id_list);
  ObjectID* object_ids = new ObjectID[num_object_ids];
  ObjectBuffer* object_buffers = new ObjectBuffer[num_object_ids];

  for (int i = 0; i < num_object_ids; ++i) {
    PyStringToUniqueID(PyList_GetItem(object_id_list, i), &object_ids[i]);
  }

  Py_BEGIN_ALLOW_THREADS;
  ARROW_CHECK_OK(client->Get(object_ids, num_object_ids, timeout_ms, object_buffers));
  Py_END_ALLOW_THREADS;

  PyObject* returns = PyList_New(num_object_ids);
  for (int i = 0; i < num_object_ids; ++i) {
    PyObject* obj_id = PyList_GetItem(object_id_list, i);
    PyObject* t = PyTuple_New(2);
    Py_XINCREF(obj_id);
    PyTuple_SetItem(t, 0, obj_id);

    if (object_buffers[i].data_size != -1) {
      /* The object was retrieved, so return the object. */
      ObjectID* buffer_obj_id = new ObjectID(object_ids[i]);
      /* This keeps a Plasma buffer in scope as long as an object that is backed by that
       * buffer is in scope. This prevents memory in the object store from getting
       * released while it is still being used to back a Python object. */
      PyObject* base = PyCapsule_New(buffer_obj_id, "buffer", BufferCapsule_Destructor);
      PyCapsule_SetContext(base, plasma_client);
      Py_XINCREF(plasma_client);

      auto batch = std::shared_ptr<RecordBatch>();
      std::vector<std::shared_ptr<Tensor>> tensors;
      ARROW_CHECK_OK(read_batch_and_tensors(
          object_buffers[i].data, object_buffers[i].data_size, &batch, tensors));

      PyObject* result;
      Status s =
          DeserializeList(batch->column(0), 0, batch->num_rows(), base, tensors, &result);
      CHECK_SERIALIZATION_ERROR(s);
      Py_XDECREF(base);

      PyTuple_SetItem(t, 1, result);
    } else {
      /* The object was not retrieved, so just add None to the list of return
       * values. */
      Py_XINCREF(Py_None);
      PyTuple_SetItem(t, 1, Py_None);
    }

    PyList_SetItem(returns, i, t);
  }

  delete[] object_ids;
  delete[] object_buffers;

  return returns;
}

#endif  // HAS_PLASMA

static PyMethodDef NumbufMethods[] = {
    {"serialize_list", serialize_list, METH_VARARGS, "serialize a Python list"},
    {"deserialize_list", deserialize_list, METH_VARARGS, "deserialize a Python list"},
    {"write_to_buffer", write_to_buffer, METH_VARARGS, "write serialized data to buffer"},
    {"read_from_buffer", read_from_buffer, METH_VARARGS,
        "read serialized data from buffer"},
    {"register_callbacks", register_callbacks, METH_VARARGS,
        "set serialization and deserialization callbacks"},
#ifdef HAS_PLASMA
    {"store_list", store_list, METH_VARARGS, "store a Python list in plasma"},
    {"retrieve_list", retrieve_list, METH_VARARGS, "retrieve a Python list from plasma"},
#endif
    {NULL, NULL, 0, NULL}};

// clang-format off
#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "libnumbuf",                     /* m_name */
    "Python C Extension for Numbuf", /* m_doc */
    0,                               /* m_size */
    NumbufMethods,                   /* m_methods */
    NULL,                            /* m_reload */
    NULL,                            /* m_traverse */
    NULL,                            /* m_clear */
    NULL,                            /* m_free */
};
#endif
// clang-format on

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

MOD_INIT(libnumbuf) {
#if PY_MAJOR_VERSION >= 3
  PyObject* m = PyModule_Create(&moduledef);
#else
  PyObject* m =
      Py_InitModule3("libnumbuf", NumbufMethods, "Python C Extension for Numbuf");
#endif

#if HAS_PLASMA
  /* Create a custom exception for when an object ID is reused. */
  char numbuf_plasma_object_exists_error[] = "numbuf_plasma_object_exists.error";
  NumbufPlasmaObjectExistsError =
      PyErr_NewException(numbuf_plasma_object_exists_error, NULL, NULL);
  Py_INCREF(NumbufPlasmaObjectExistsError);
  PyModule_AddObject(
      m, "numbuf_plasma_object_exists_error", NumbufPlasmaObjectExistsError);
  /* Create a custom exception for when the plasma store is out of memory. */
  char numbuf_plasma_out_of_memory_error[] = "numbuf_plasma_out_of_memory.error";
  NumbufPlasmaOutOfMemoryError =
      PyErr_NewException(numbuf_plasma_out_of_memory_error, NULL, NULL);
  Py_INCREF(NumbufPlasmaOutOfMemoryError);
  PyModule_AddObject(
      m, "numbuf_plasma_out_of_memory_error", NumbufPlasmaOutOfMemoryError);
#endif

  char numbuf_error[] = "numbuf.error";
  NumbufError = PyErr_NewException(numbuf_error, NULL, NULL);
  Py_INCREF(NumbufError);
  PyModule_AddObject(m, "numbuf_error", NumbufError);
  import_array();

#if PY_MAJOR_VERSION >= 3
  return m;
#endif
}
}
