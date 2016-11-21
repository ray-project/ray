#include <Python.h>
#include <arrow/api.h>
#include <arrow/ipc/adapter.h>
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#define PY_ARRAY_UNIQUE_SYMBOL NUMBUF_ARRAY_API
#include <numpy/arrayobject.h>

#include <iostream>

#include <arrow/ipc/metadata.h>

#include "adapters/python.h"
#include "memory.h"

#ifdef HAS_PLASMA
extern "C" {
  #include "plasma_client.h"
}
#endif

using namespace arrow;
using namespace numbuf;

int64_t make_schema_and_batch(std::shared_ptr<Array> data,
                              std::shared_ptr<Buffer>* metadata_out,
                              std::shared_ptr<RecordBatch> *batch_out) {
  auto field = std::make_shared<Field>("list", data->type());
  std::shared_ptr<Schema> schema(new Schema({field}));
  *batch_out = std::shared_ptr<RecordBatch>(new RecordBatch(schema, data->length(), {data}));
  int64_t size = 0;
  ARROW_CHECK_OK(ipc::GetRecordBatchSize(batch_out->get(), &size));
  ARROW_CHECK_OK(ipc::WriteSchema((*batch_out)->schema().get(), metadata_out));
  return size;
}

Status read_batch(std::shared_ptr<Buffer> schema_buffer,
                  int64_t header_end_offset,
                  uint8_t* data,
                  int64_t size,
                  std::shared_ptr<RecordBatch> *batch_out) {
  std::shared_ptr<ipc::Message> message;
  RETURN_NOT_OK(ipc::Message::Open(schema_buffer, &message));
  DCHECK_EQ(ipc::Message::SCHEMA, message->type());
  std::shared_ptr<ipc::SchemaMessage> schema_msg = message->GetSchema();
  std::shared_ptr<Schema> schema;
  RETURN_NOT_OK(schema_msg->GetSchema(&schema));
  auto source = std::make_shared<FixedBufferStream>(data, size);
  std::shared_ptr<arrow::ipc::RecordBatchReader> reader;
  RETURN_NOT_OK(
      ipc::RecordBatchReader::Open(source.get(), header_end_offset, &reader));
  RETURN_NOT_OK(reader->GetRecordBatch(schema, batch_out));
  return Status::OK();
}

extern "C" {

#define CHECK_SERIALIZATION_ERROR(STATUS) \
  do { \
    Status _s = (STATUS); \
    if (!_s.ok()) { \
      /* If this condition is true, there was an error in the callback that
       *needs to be passed through */ \
      if (!PyErr_Occurred()) { \
        PyErr_SetString(NumbufError, _s.ToString().c_str()); \
      } \
      return NULL; \
    } \
  } while (0)

static PyObject* NumbufError;

PyObject* numbuf_serialize_callback = NULL;
PyObject* numbuf_deserialize_callback = NULL;

int PyObjectToArrow(PyObject* object, std::shared_ptr<RecordBatch>** result) {
  if (PyCapsule_IsValid(object, "arrow")) {
    *result = reinterpret_cast<std::shared_ptr<RecordBatch>*>(
        PyCapsule_GetPointer(object, "arrow"));
    return 1;
  } else {
    PyErr_SetString(PyExc_TypeError, "must be an 'arrow' capsule");
    return 0;
  }
}

static void ArrowCapsule_Destructor(PyObject* capsule) {
  delete reinterpret_cast<std::shared_ptr<RecordBatch>*>(
      PyCapsule_GetPointer(capsule, "arrow"));
}

/* Documented in doc/numbuf.rst in ray-core */
static PyObject* serialize_list(PyObject* self, PyObject* args) {
  PyObject* value;
  if (!PyArg_ParseTuple(args, "O", &value)) { return NULL; }
  std::shared_ptr<Array> array;
  if (PyList_Check(value)) {
    int32_t recursion_depth = 0;
    Status s =
        SerializeSequences(std::vector<PyObject*>({value}), recursion_depth, &array);
    CHECK_SERIALIZATION_ERROR(s);

    auto batch = new std::shared_ptr<RecordBatch>();
    std::shared_ptr<Buffer> metadata;
    int64_t size = make_schema_and_batch(array, &metadata, batch);

    auto ptr = reinterpret_cast<const char*>(metadata->data());
    PyObject* r = PyTuple_New(3);
    PyTuple_SetItem(r, 0, PyByteArray_FromStringAndSize(ptr, metadata->size()));
    PyTuple_SetItem(r, 1, PyInt_FromLong(size));
    PyTuple_SetItem(r, 2,
        PyCapsule_New(reinterpret_cast<void*>(batch), "arrow", &ArrowCapsule_Destructor));
    return r;
  }
  return NULL;
}

/* Documented in doc/numbuf.rst in ray-core */
static PyObject* write_to_buffer(PyObject* self, PyObject* args) {
  std::shared_ptr<RecordBatch>* batch;
  PyObject* memoryview;
  if (!PyArg_ParseTuple(args, "O&O", &PyObjectToArrow, &batch, &memoryview)) {
    return NULL;
  }
  if (!PyMemoryView_Check(memoryview)) { return NULL; }
  Py_buffer* buffer = PyMemoryView_GET_BUFFER(memoryview);
  auto target = std::make_shared<FixedBufferStream>(
      reinterpret_cast<uint8_t*>(buffer->buf), buffer->len);
  int64_t body_end_offset;
  int64_t header_end_offset;
  ARROW_CHECK_OK(ipc::WriteRecordBatch((*batch)->columns(), (*batch)->num_rows(),
      target.get(), &body_end_offset, &header_end_offset));
  return PyInt_FromLong(header_end_offset);
}

/* Documented in doc/numbuf.rst in ray-core */
static PyObject* read_from_buffer(PyObject* self, PyObject* args) {
  PyObject* memoryview;
  PyObject* metadata;
  int64_t header_end_offset;
  if (!PyArg_ParseTuple(args, "OOL", &memoryview, &metadata, &header_end_offset)) {
    return NULL;
  }

  Py_buffer* buffer = PyMemoryView_GET_BUFFER(memoryview);
  auto ptr = reinterpret_cast<uint8_t*>(PyByteArray_AsString(metadata));
  auto schema_buffer = std::make_shared<Buffer>(ptr, PyByteArray_Size(metadata));
  auto batch = new std::shared_ptr<arrow::RecordBatch>();
  ARROW_CHECK_OK(read_batch(schema_buffer, header_end_offset, reinterpret_cast<uint8_t*>(buffer->buf), buffer->len, batch));
  return PyCapsule_New(reinterpret_cast<void*>(batch), "arrow", &ArrowCapsule_Destructor);
}

/* Documented in doc/numbuf.rst in ray-core */
static PyObject* deserialize_list(PyObject* self, PyObject* args) {
  std::shared_ptr<RecordBatch>* data;
  PyObject* base = Py_None;
  if (!PyArg_ParseTuple(args, "O&|O", &PyObjectToArrow, &data, &base)) { return NULL; }
  PyObject* result;
  Status s = DeserializeList((*data)->column(0), 0, (*data)->num_rows(), base, &result);
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

#include "plasma_extension.h"

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
  object_id *id = reinterpret_cast<object_id*>(PyCapsule_GetPointer(capsule, "buffer"));
  plasma_connection *conn = reinterpret_cast<plasma_connection*>(PyCapsule_GetContext(capsule));
  plasma_release(conn, *id);
  delete id;
}

/**
 * Store a PyList in the plasma store.
 *
 * This function converts the PyList into an arrow RecordBatch,
 * constructs the metadata (schema) of the PyList, creates a new plasma
 * object, puts the data into the plasma buffer and the schema into the
 * plasma metadata.
 *
 * @param args Contains the object ID the list is stored under, the
 *        connection to the plasma store and the PyList we want to store.
 * @return None.
 */
static PyObject* store_list(PyObject* self, PyObject* args) {
  object_id obj_id;
  plasma_connection *conn;
  PyObject *value;
  if (!PyArg_ParseTuple(args, "O&O&O", PyObjectToUniqueID, &obj_id, PyObjectToPlasmaConnection, &conn, &value)) {
    return NULL;
  }
  if (!PyList_Check(value)) {
    return NULL;
  }

  std::shared_ptr<Array> array;
  int32_t recursion_depth = 0;
  Status s = SerializeSequences(std::vector<PyObject*>({value}), recursion_depth, &array);
  CHECK_SERIALIZATION_ERROR(s);

  auto batch = new std::shared_ptr<RecordBatch>();
  std::shared_ptr<Buffer> metadata;
  int64_t size = make_schema_and_batch(array, &metadata, batch);

  uint8_t *data;
  /* The arrow schema is stored as the metadata of the plasma object and
   * both the arrow data and the header end offset are
   * stored in the plasma data buffer. The header end offset is stored in
   * the first sizeof(int64_t) bytes of the data buffer. The RecordBatch
   * data is stored after that. */
  plasma_create(conn, obj_id, sizeof(size) + size, (uint8_t *) metadata->data(), metadata->size(), &data);

  auto target = std::make_shared<FixedBufferStream>(sizeof(size) + data, size);
  int64_t body_end_offset;
  int64_t header_end_offset;
  ARROW_CHECK_OK(ipc::WriteRecordBatch((*batch)->columns(), (*batch)->num_rows(), target.get(), &body_end_offset, &header_end_offset));

  /* Save the header end offset at the beginning of the plasma data buffer. */
  *((int64_t *) data) = header_end_offset;
  plasma_seal(conn, obj_id);
  Py_RETURN_NONE;
}

/**
 * Retrieve a PyList from the plasma store.
 *
 * This reads the arrow schema from the plasma metadata, constructs
 * Python objects from the plasma data according to the schema and
 * returns the object.
 *
 * @param args Object ID of the PyList to be retrieved and connection to the
 *        plasma store.
 * @return The PyList.
 */
static PyObject* retrieve_list(PyObject* self, PyObject* args) {
  object_id obj_id;
  plasma_connection *conn;
  if (!PyArg_ParseTuple(args, "O&O&", PyObjectToUniqueID, &obj_id, PyObjectToPlasmaConnection, &conn)) {
    return NULL;
  }
  object_id *buffer_obj_id = new object_id(obj_id);
  /* This keeps a Plasma buffer in scope as long as an object that is backed by that
   * buffer is in scope. This prevents memory in the object store from getting
   * released while it is still being used to back a Python object. */
  PyObject* base = PyCapsule_New(buffer_obj_id, "buffer", BufferCapsule_Destructor);
  PyCapsule_SetContext(base, conn);

  int64_t size, metadata_size;
  uint8_t *data, *metadata;
  plasma_get(conn, obj_id, &size, &data, &metadata_size, &metadata);

  /* Remember: The metadata offset was written at the beginning of the plasma buffer. */
  int64_t header_end_offset = *((int64_t *) data);
  auto schema_buffer = std::make_shared<Buffer>(metadata, metadata_size);
  auto batch = std::shared_ptr<RecordBatch>();
  ARROW_CHECK_OK(read_batch(schema_buffer, header_end_offset, data + sizeof(size), size - sizeof(size), &batch));

  PyObject* result;
  Status s = DeserializeList(batch->column(0), 0, batch->num_rows(), base, &result);
  CHECK_SERIALIZATION_ERROR(s);
  Py_XDECREF(base);
  return result;
}

#endif // HAS_PLASMA

static PyMethodDef NumbufMethods[] = {
  {"serialize_list", serialize_list, METH_VARARGS, "serialize a Python list"},
  {"deserialize_list", deserialize_list, METH_VARARGS, "deserialize a Python list"},
  {"write_to_buffer", write_to_buffer, METH_VARARGS, "write serialized data to buffer"},
  {"read_from_buffer", read_from_buffer, METH_VARARGS,
    "read serialized data from buffer"},
  {"register_callbacks", register_callbacks, METH_VARARGS,
    "set serialization and deserialization callbacks"},
#ifdef HAS_PLASMA
  { "store_list", store_list, METH_VARARGS, "store a Python list in plasma" },
  { "retrieve_list", retrieve_list, METH_VARARGS, "retrieve a Python list from plasma" },
#endif
    {NULL, NULL, 0, NULL}};

PyMODINIT_FUNC initlibnumbuf(void) {
  PyObject* m;
  m = Py_InitModule3("libnumbuf", NumbufMethods, "Python C Extension for Numbuf");
  char numbuf_error[] = "numbuf.error";
  NumbufError = PyErr_NewException(numbuf_error, NULL, NULL);
  Py_INCREF(NumbufError);
  PyModule_AddObject(m, "numbuf_error", NumbufError);
  import_array();
}
}
