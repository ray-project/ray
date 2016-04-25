#include "serialize.h"

using namespace arrow;

template <int TYPE>
struct npy_traits {
};

template <>
struct npy_traits<NPY_BOOL> {
  typedef uint8_t value_type;
  static const std::shared_ptr<BooleanType> primitive_type;
  using ArrayType = arrow::BooleanArray;
};

const std::shared_ptr<BooleanType> npy_traits<NPY_BOOL>::primitive_type = std::make_shared<BooleanType>();

#define NPY_INT_DECL(TYPE, CapType, T)                          \
  template <>                                                   \
  struct npy_traits<NPY_##TYPE> {                               \
    typedef T value_type;                                       \
    static const std::shared_ptr<CapType##Type> primitive_type; \
    using ArrayType = arrow::CapType##Array;                    \
  };                                                            \
                                                                \
  const std::shared_ptr<CapType##Type> npy_traits<NPY_##TYPE>::primitive_type = std::make_shared<CapType##Type>();

NPY_INT_DECL(INT8, Int8, int8_t);
NPY_INT_DECL(INT16, Int16, int16_t);
NPY_INT_DECL(INT32, Int32, int32_t);
NPY_INT_DECL(INT64, Int64, int64_t);
NPY_INT_DECL(UINT8, UInt8, uint8_t);
NPY_INT_DECL(UINT16, UInt16, uint16_t);
NPY_INT_DECL(UINT32, UInt32, uint32_t);
NPY_INT_DECL(UINT64, UInt64, uint64_t);

template <>
struct npy_traits<NPY_FLOAT32> {
  typedef float value_type;
  static const std::shared_ptr<FloatType> primitive_type;
  using ArrayType = arrow::FloatArray;
};

const std::shared_ptr<FloatType> npy_traits<NPY_FLOAT32>::primitive_type = std::make_shared<FloatType>();

template <>
struct npy_traits<NPY_FLOAT64> {
  typedef double value_type;
  static const std::shared_ptr<DoubleType> primitive_type;
  using ArrayType = arrow::DoubleArray;
};

const std::shared_ptr<DoubleType> npy_traits<NPY_FLOAT64>::primitive_type = std::make_shared<DoubleType>();

template <>
struct npy_traits<NPY_OBJECT> {
  typedef PyObject* value_type;
};

template<int NpyType>
std::shared_ptr<arrow::RowBatch> make_flat_array(const std::string& fieldname, size_t size, std::shared_ptr<arrow::Buffer> data) {
  auto field = std::make_shared<arrow::Field>(fieldname, npy_traits<NpyType>::primitive_type);
  std::shared_ptr<arrow::Schema> schema(new arrow::Schema({field}));
  auto array = std::make_shared<typename npy_traits<NpyType>::ArrayType>(size, data);
  return std::shared_ptr<arrow::RowBatch>(new RowBatch(schema, size, {array}));
}

const int64_t MAX_METADATA_SIZE = 5000;

#define SIZE_ARROW_CASE(TYPE)                                        \
  case TYPE:                                                         \
      return size * sizeof(npy_traits<TYPE>::value_type) + MAX_METADATA_SIZE;

size_t arrow_size(PyArrayObject* array) {
  npy_intp size = PyArray_SIZE(array);
  switch (PyArray_TYPE(array)) {
    SIZE_ARROW_CASE(NPY_INT8)
    SIZE_ARROW_CASE(NPY_INT16)
    SIZE_ARROW_CASE(NPY_INT32)
    SIZE_ARROW_CASE(NPY_INT64)
    SIZE_ARROW_CASE(NPY_UINT8)
    SIZE_ARROW_CASE(NPY_UINT16)
    SIZE_ARROW_CASE(NPY_UINT32)
    SIZE_ARROW_CASE(NPY_UINT64)
    SIZE_ARROW_CASE(NPY_FLOAT)
    SIZE_ARROW_CASE(NPY_DOUBLE)
    default:
      ORCH_LOG(ORCH_FATAL, "serialization: numpy datatype not know");
  }
}

#define SERIALIZE_ARROW_CASE(TYPE)                                        \
  case TYPE:                                                              \
    {                                                                     \
      data = std::make_shared<arrow::Buffer>(reinterpret_cast<uint8_t*>(PyArray_DATA(array)), sizeof(npy_traits<TYPE>::value_type) * size); \
      batch_size = size * sizeof(npy_traits<TYPE>::value_type) + MAX_METADATA_SIZE;    \
      batch = make_flat_array<TYPE>("data", size, data);                  \
    }                                                                     \
    break;

// TODO(pcm): At the moment, this assumes that arrays are consecutive in memory
void store_arrow(PyArrayObject* array, ObjHandle& location, MemorySegmentPool* pool) {
  npy_intp size = PyArray_SIZE(array);
  std::shared_ptr<arrow::Buffer> data;
  std::shared_ptr<arrow::RowBatch> batch;
  int64_t batch_size = 0;
  switch (PyArray_TYPE(array)) {
    SERIALIZE_ARROW_CASE(NPY_INT8)
    SERIALIZE_ARROW_CASE(NPY_INT16)
    SERIALIZE_ARROW_CASE(NPY_INT32)
    SERIALIZE_ARROW_CASE(NPY_INT64)
    SERIALIZE_ARROW_CASE(NPY_UINT8)
    SERIALIZE_ARROW_CASE(NPY_UINT16)
    SERIALIZE_ARROW_CASE(NPY_UINT32)
    SERIALIZE_ARROW_CASE(NPY_UINT64)
    SERIALIZE_ARROW_CASE(NPY_FLOAT)
    SERIALIZE_ARROW_CASE(NPY_DOUBLE)
    default:
      ORCH_LOG(ORCH_FATAL, "serialization: numpy datatype not know");
  }

  // int64_t data_batch_size = ipc::GetRowBatchSize(batch.get()); // FIXME(pcm): once GetRowBatchSize is implemented, use it

  size_t ndim = PyArray_NDIM(array);
  MemoryPool* default_pool = arrow::default_memory_pool();

  auto metadata = std::make_shared<PoolBuffer>(default_pool);
  size_t metadata_size = 1 + ndim + 1; // dtype, list of shapes, pointer to header of the data segment
  metadata->Resize(metadata_size * sizeof(int64_t));

  int64_t* buffer = reinterpret_cast<int64_t*>(metadata->mutable_data());
  buffer[0] = PyArray_TYPE(array);
  // serialize the shape information
  for (size_t i = 0; i < ndim; ++i) {
    buffer[i+1] = PyArray_DIM(array, i);
  }
  std::shared_ptr<arrow::RowBatch> metadata_batch = make_flat_array<NPY_UINT64>("metadata", metadata_size, metadata);

  // int64_t metadata_batch_size = ipc::GetRowBatchSize(metadata_batch.get()); // FIXME(pcm): once GetRowBatchSize is implemented, use it

  uint8_t* address = pool->get_address(location);
  auto source = std::make_shared<BufferMemorySource>(address, location.size());

  int64_t data_header_offset = 0;
  ipc::WriteRowBatch(source.get(), batch.get(), 0, &data_header_offset);

  buffer[1 + ndim] = data_header_offset;

  int64_t metadata_header_offset = 0;
  ipc::WriteRowBatch(source.get(), metadata_batch.get(), location.size() + MAX_METADATA_SIZE/2, &metadata_header_offset);
  location.set_metadata_offset(metadata_header_offset);
}

template<int NpyType>
std::shared_ptr<arrow::Array> read_flat_array(BufferMemorySource* source, int64_t metadata_offset) {
  std::shared_ptr<ipc::RowBatchReader> reader;
  Status s = ipc::RowBatchReader::Open(source, metadata_offset, &reader);
  if (!s.ok()) {
    ORCH_LOG(ORCH_FATAL, "Error in read_flat_array: " << s.ToString());
  }
  auto field = std::make_shared<arrow::Field>("data", npy_traits<NpyType>::primitive_type);
  std::shared_ptr<arrow::Schema> schema(new arrow::Schema({field}));
  std::shared_ptr<arrow::RowBatch> data;
  reader->GetRowBatch(schema, &data);
  return data->column(0);

}

#define DESERIALIZE_ARROW_CASE(TYPE)                                                                           \
  case TYPE:                                                                                                   \
    {                                                                                                          \
      auto array = read_flat_array<TYPE>(source.get(), buffer[metadata_array->length()-1]);                    \
      auto data_primitive_array = dynamic_cast<npy_traits<TYPE>::ArrayType*>(array.get());                     \
      return PyArray_SimpleNewFromData(dims.size(), &dims[0], TYPE, (void*)data_primitive_array->raw_data());  \
    }

PyObject* deserialize_array(ObjHandle handle, MemorySegmentPool* pool) {
  auto source = std::make_shared<BufferMemorySource>(pool->get_address(handle), handle.size());
  auto metadata_array = read_flat_array<NPY_UINT64>(source.get(), handle.metadata_offset());
  const uint64_t* buffer = dynamic_cast<UInt64Array*>(metadata_array.get())->raw_data();
  uint64_t type = buffer[0];
  std::vector<npy_intp> dims;
  for (int i = 1; i < metadata_array->length()-1; ++i) {
    dims.push_back(buffer[i]);
  }

  switch (type) {
    DESERIALIZE_ARROW_CASE(NPY_INT8)
    DESERIALIZE_ARROW_CASE(NPY_INT16)
    DESERIALIZE_ARROW_CASE(NPY_INT32)
    DESERIALIZE_ARROW_CASE(NPY_INT64)
    DESERIALIZE_ARROW_CASE(NPY_UINT8)
    DESERIALIZE_ARROW_CASE(NPY_UINT16)
    DESERIALIZE_ARROW_CASE(NPY_UINT32)
    DESERIALIZE_ARROW_CASE(NPY_UINT64)
    DESERIALIZE_ARROW_CASE(NPY_FLOAT)
    DESERIALIZE_ARROW_CASE(NPY_DOUBLE)
    default:
      ORCH_LOG(ORCH_FATAL, "deserialization: numpy datatype not know");
  }
}
