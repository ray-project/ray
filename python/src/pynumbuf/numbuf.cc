#include <Python.h>
#include <arrow/api.h>
#include <arrow/ipc/memory.h>
#include <arrow/ipc/adapter.h>
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#define PY_ARRAY_UNIQUE_SYMBOL NUMBUF_ARRAY_API
#include <numpy/arrayobject.h>

#include <iostream>

#include <arrow/ipc/metadata.h>

#include "adapters/python.h"
#include "memory.h"

using namespace arrow;
using namespace numbuf;

std::shared_ptr<RowBatch> make_row_batch(std::shared_ptr<Array> data) {
  auto field = std::make_shared<Field>("list", data->type());
  std::shared_ptr<Schema> schema(new Schema({field}));
  return std::shared_ptr<RowBatch>(new RowBatch(schema, data->length(), {data}));
}

extern "C" {

static PyObject *NumbufError;

int PyObjectToArrow(PyObject* object, std::shared_ptr<RowBatch> **result) {
  if (PyCapsule_IsValid(object, "arrow")) {
    *result = reinterpret_cast<std::shared_ptr<RowBatch>*>(PyCapsule_GetPointer(object, "arrow"));
    return 1;
  } else {
    PyErr_SetString(PyExc_TypeError, "must be an 'arrow' capsule");
    return 0;
  }
}

static void ArrowCapsule_Destructor(PyObject* capsule) {
  delete reinterpret_cast<std::shared_ptr<RowBatch>*>(PyCapsule_GetPointer(capsule, "arrow"));
}

/* Documented in doc/numbuf.rst in ray-core */
static PyObject* serialize_list(PyObject* self, PyObject* args) {
  PyObject* value;
  if (!PyArg_ParseTuple(args, "O", &value)) {
    return NULL;
  }
  std::shared_ptr<Array> array;
  if (PyList_Check(value)) {
    Status s = SerializeSequences(std::vector<PyObject*>({value}), &array);
    if (!s.ok()) {
      PyErr_SetString(NumbufError, s.ToString().c_str());
      return NULL;
    }

    auto batch = new std::shared_ptr<RowBatch>();
    *batch = make_row_batch(array);

    int64_t size = 0;
    ARROW_CHECK_OK(arrow::ipc::GetRowBatchSize(batch->get(), &size));

    std::shared_ptr<Buffer> buffer;
    ARROW_CHECK_OK(ipc::WriteSchema((*batch)->schema().get(), &buffer));
    auto ptr = reinterpret_cast<const char*>(buffer->data());

    PyObject* r = PyTuple_New(3);
    PyTuple_SetItem(r, 0, PyByteArray_FromStringAndSize(ptr, buffer->size()));
    PyTuple_SetItem(r, 1, PyInt_FromLong(size));
    PyTuple_SetItem(r, 2, PyCapsule_New(reinterpret_cast<void*>(batch),
                                        "arrow", &ArrowCapsule_Destructor));
    return r;
  }
  return NULL;
}

/* Documented in doc/numbuf.rst in ray-core */
static PyObject* write_to_buffer(PyObject* self, PyObject* args) {
  std::shared_ptr<RowBatch>* batch;
  PyObject* memoryview;
  if (!PyArg_ParseTuple(args, "O&O", &PyObjectToArrow, &batch, &memoryview)) {
    return NULL;
  }
  if (!PyMemoryView_Check(memoryview)) {
    return NULL;
  }
  Py_buffer* buffer = PyMemoryView_GET_BUFFER(memoryview);
  auto target = std::make_shared<BufferSource>(reinterpret_cast<uint8_t*>(buffer->buf), buffer->len);
  int64_t metadata_offset;
  ARROW_CHECK_OK(ipc::WriteRowBatch(target.get(), batch->get(), 0, &metadata_offset));
  return PyInt_FromLong(metadata_offset);
}

/* Documented in doc/numbuf.rst in ray-core */
static PyObject* read_from_buffer(PyObject* self, PyObject* args) {
  PyObject* memoryview;
  PyObject* metadata;
  int64_t metadata_offset;
  if (!PyArg_ParseTuple(args, "OOL", &memoryview, &metadata, &metadata_offset)) {
    return NULL;
  }

  auto ptr = reinterpret_cast<uint8_t*>(PyByteArray_AsString(metadata));
  auto schema_buffer = std::make_shared<Buffer>(ptr, PyByteArray_Size(metadata));
  std::shared_ptr<ipc::Message> message;
  ARROW_CHECK_OK(ipc::Message::Open(schema_buffer, &message));
  DCHECK_EQ(ipc::Message::SCHEMA, message->type());
  std::shared_ptr<ipc::SchemaMessage> schema_msg = message->GetSchema();
  std::shared_ptr<Schema> schema;
  ARROW_CHECK_OK(schema_msg->GetSchema(&schema));

  Py_buffer* buffer = PyMemoryView_GET_BUFFER(memoryview);
  auto source = std::make_shared<BufferSource>(reinterpret_cast<uint8_t*>(buffer->buf), buffer->len);
  std::shared_ptr<arrow::ipc::RowBatchReader> reader;
  ARROW_CHECK_OK(arrow::ipc::RowBatchReader::Open(source.get(), metadata_offset, &reader));
  auto batch = new std::shared_ptr<arrow::RowBatch>();
  ARROW_CHECK_OK(reader->GetRowBatch(schema, batch));

  return PyCapsule_New(reinterpret_cast<void*>(batch),
                       "arrow", &ArrowCapsule_Destructor);
}

/* Documented in doc/numbuf.rst in ray-core */
static PyObject* deserialize_list(PyObject* self, PyObject* args) {
  std::shared_ptr<RowBatch>* data;
  if (!PyArg_ParseTuple(args, "O&", &PyObjectToArrow, &data)) {
    return NULL;
  }
  PyObject* result;
  ARROW_CHECK_OK(DeserializeList((*data)->column(0), 0, (*data)->num_rows(), &result));
  return result;
}

static PyMethodDef NumbufMethods[] = {
 { "serialize_list", serialize_list, METH_VARARGS, "serialize a Python list" },
 { "deserialize_list", deserialize_list, METH_VARARGS, "deserialize a Python list" },
 { "write_to_buffer", write_to_buffer, METH_VARARGS, "write serialized data to buffer"},
 { "read_from_buffer", read_from_buffer, METH_VARARGS, "read serialized data from buffer"},
 { NULL, NULL, 0, NULL }
};

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
