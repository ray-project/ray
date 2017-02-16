#include "sequence.h"

using namespace arrow;

namespace numbuf {

SequenceBuilder::SequenceBuilder(MemoryPool* pool)
    : pool_(pool),
      types_(pool, std::make_shared<Int8Type>()),
      offsets_(pool, std::make_shared<Int32Type>()),
      total_num_bytes_(0),
      nones_(pool, std::make_shared<NullType>()),
      bools_(pool, std::make_shared<BooleanType>()),
      ints_(pool, std::make_shared<Int64Type>()),
      bytes_(pool, std::make_shared<BinaryType>()),
      strings_(pool, std::make_shared<StringType>()),
      floats_(pool, std::make_shared<FloatType>()),
      doubles_(pool, std::make_shared<DoubleType>()),
      uint8_tensors_(std::make_shared<UInt8Type>(), pool),
      int8_tensors_(std::make_shared<Int8Type>(), pool),
      uint16_tensors_(std::make_shared<UInt16Type>(), pool),
      int16_tensors_(std::make_shared<Int16Type>(), pool),
      uint32_tensors_(std::make_shared<UInt32Type>(), pool),
      int32_tensors_(std::make_shared<Int32Type>(), pool),
      uint64_tensors_(std::make_shared<UInt64Type>(), pool),
      int64_tensors_(std::make_shared<Int64Type>(), pool),
      float_tensors_(std::make_shared<FloatType>(), pool),
      double_tensors_(std::make_shared<DoubleType>(), pool),
      list_offsets_({0}),
      tuple_offsets_({0}),
      dict_offsets_({0}) {}

/* We need to ensure that the number of bytes allocated by arrow
 * does not exceed 2**31 - 1. To make sure that is the case, allocation needs
 * to be capped at 2**29 - 1, because arrow calculates the next power of two
 * for allocations (see arrow::ArrayBuilder::Reserve).
 */
#define UPDATE(OFFSET, TAG)                                               \
  if (total_num_bytes_ >= 1 << 29 - 1) {                                  \
    return Status::NotImplemented("Sequence contains too many elements"); \
  }                                                                       \
  if (TAG == -1) {                                                        \
    TAG = num_tags;                                                       \
    num_tags += 1;                                                        \
  }                                                                       \
  RETURN_NOT_OK(offsets_.Append(OFFSET));                                 \
  RETURN_NOT_OK(types_.Append(TAG));                                      \
  RETURN_NOT_OK(nones_.AppendToBitmap(true));

Status SequenceBuilder::AppendNone() {
  total_num_bytes_ += sizeof(int32_t);
  RETURN_NOT_OK(offsets_.Append(0));
  RETURN_NOT_OK(types_.Append(0));
  return nones_.AppendToBitmap(false);
}

Status SequenceBuilder::AppendBool(bool data) {
  total_num_bytes_ += sizeof(bool);
  UPDATE(bools_.length(), bool_tag);
  return bools_.Append(data);
}

Status SequenceBuilder::AppendInt64(int64_t data) {
  total_num_bytes_ += sizeof(int64_t);
  UPDATE(ints_.length(), int_tag);
  return ints_.Append(data);
}

Status SequenceBuilder::AppendUInt64(uint64_t data) {
  total_num_bytes_ += sizeof(uint64_t);
  UPDATE(ints_.length(), int_tag);
  return ints_.Append(data);
}

Status SequenceBuilder::AppendBytes(const uint8_t* data, int32_t length) {
  total_num_bytes_ += length * sizeof(uint8_t);
  UPDATE(bytes_.length(), bytes_tag);
  return bytes_.Append(data, length);
}

Status SequenceBuilder::AppendString(const char* data, int32_t length) {
  total_num_bytes_ += length * sizeof(char);
  UPDATE(strings_.length(), string_tag);
  return strings_.Append(data, length);
}

Status SequenceBuilder::AppendFloat(float data) {
  total_num_bytes_ += sizeof(float);
  UPDATE(floats_.length(), float_tag);
  return floats_.Append(data);
}

Status SequenceBuilder::AppendDouble(double data) {
  total_num_bytes_ += sizeof(double);
  UPDATE(doubles_.length(), double_tag);
  return doubles_.Append(data);
}

#define DEF_TENSOR_APPEND(NAME, TYPE, TAG)                                             \
  Status SequenceBuilder::AppendTensor(const std::vector<int64_t>& dims, TYPE* data) { \
    if (TAG == -1) { NAME.Start(); }                                                   \
    int64_t size = 1;                                                                  \
    for (auto dim : dims) {                                                            \
      size *= dim;                                                                     \
    }                                                                                  \
    total_num_bytes_ += size * sizeof(TYPE);                                           \
    UPDATE(NAME.length(), TAG);                                                        \
    return NAME.Append(dims, data);                                                    \
  }

DEF_TENSOR_APPEND(uint8_tensors_, uint8_t, uint8_tensor_tag);
DEF_TENSOR_APPEND(int8_tensors_, int8_t, int8_tensor_tag);
DEF_TENSOR_APPEND(uint16_tensors_, uint16_t, uint16_tensor_tag);
DEF_TENSOR_APPEND(int16_tensors_, int16_t, int16_tensor_tag);
DEF_TENSOR_APPEND(uint32_tensors_, uint32_t, uint32_tensor_tag);
DEF_TENSOR_APPEND(int32_tensors_, int32_t, int32_tensor_tag);
DEF_TENSOR_APPEND(uint64_tensors_, uint64_t, uint64_tensor_tag);
DEF_TENSOR_APPEND(int64_tensors_, int64_t, int64_tensor_tag);
DEF_TENSOR_APPEND(float_tensors_, float, float_tensor_tag);
DEF_TENSOR_APPEND(double_tensors_, double, double_tensor_tag);

Status SequenceBuilder::AppendList(int32_t size) {
  // Increase number of bytes to account for offsets
  // (types and bitmaps are smaller)
  total_num_bytes_ += size * sizeof(int32_t);
  UPDATE(list_offsets_.size() - 1, list_tag);
  list_offsets_.push_back(list_offsets_.back() + size);
  return Status::OK();
}

Status SequenceBuilder::AppendTuple(int32_t size) {
  // Increase number of bytes to account for offsets
  // (types and bitmaps are smaller)
  total_num_bytes_ += size * sizeof(int32_t);
  UPDATE(tuple_offsets_.size() - 1, tuple_tag);
  tuple_offsets_.push_back(tuple_offsets_.back() + size);
  return Status::OK();
}

Status SequenceBuilder::AppendDict(int32_t size) {
  // Increase number of bytes to account for offsets
  // (types and bitmaps are smaller)
  total_num_bytes_ += size * sizeof(int32_t);
  UPDATE(dict_offsets_.size() - 1, dict_tag);
  dict_offsets_.push_back(dict_offsets_.back() + size);
  return Status::OK();
}

#define ADD_ELEMENT(VARNAME, TAG)                             \
  if (TAG != -1) {                                            \
    types[TAG] = std::make_shared<Field>("", VARNAME.type()); \
    RETURN_NOT_OK(VARNAME.Finish(&children[TAG]));            \
    RETURN_NOT_OK(nones_.AppendToBitmap(true));               \
  }

#define ADD_SUBSEQUENCE(DATA, OFFSETS, BUILDER, TAG, NAME)                    \
  if (DATA) {                                                                 \
    DCHECK(DATA->length() == OFFSETS.back());                                 \
    auto list_builder = std::make_shared<ListBuilder>(pool_, DATA);           \
    auto field = std::make_shared<Field>(NAME, list_builder->type());         \
    auto type = std::make_shared<StructType>(std::vector<FieldPtr>({field})); \
    auto lists = std::vector<std::shared_ptr<ArrayBuilder>>({list_builder});  \
    StructBuilder builder(pool_, type, lists);                                \
    OFFSETS.pop_back();                                                       \
    ARROW_CHECK_OK(list_builder->Append(OFFSETS.data(), OFFSETS.size()));     \
    builder.Append();                                                         \
    ADD_ELEMENT(builder, TAG);                                                \
  } else {                                                                    \
    DCHECK(OFFSETS.size() == 1);                                              \
  }

Status SequenceBuilder::Finish(std::shared_ptr<Array> list_data,
    std::shared_ptr<Array> tuple_data, std::shared_ptr<Array> dict_data,
    std::shared_ptr<Array>* out) {
  std::vector<std::shared_ptr<Field>> types(num_tags);
  std::vector<ArrayPtr> children(num_tags);

  ADD_ELEMENT(bools_, bool_tag);
  ADD_ELEMENT(ints_, int_tag);
  ADD_ELEMENT(strings_, string_tag);
  ADD_ELEMENT(bytes_, bytes_tag);
  ADD_ELEMENT(floats_, float_tag);
  ADD_ELEMENT(doubles_, double_tag);

  ADD_ELEMENT(uint8_tensors_, uint8_tensor_tag);

  ADD_ELEMENT(int8_tensors_, int8_tensor_tag);
  ADD_ELEMENT(uint16_tensors_, uint16_tensor_tag);
  ADD_ELEMENT(int16_tensors_, int16_tensor_tag);
  ADD_ELEMENT(uint32_tensors_, uint32_tensor_tag);

  ADD_ELEMENT(int32_tensors_, int32_tensor_tag);
  ADD_ELEMENT(uint64_tensors_, uint64_tensor_tag);
  ADD_ELEMENT(int64_tensors_, int64_tensor_tag);

  ADD_ELEMENT(float_tensors_, float_tensor_tag);
  ADD_ELEMENT(double_tensors_, double_tensor_tag);

  ADD_SUBSEQUENCE(list_data, list_offsets_, list_builder, list_tag, "list");
  ADD_SUBSEQUENCE(tuple_data, tuple_offsets_, tuple_builder, tuple_tag, "tuple");
  ADD_SUBSEQUENCE(dict_data, dict_offsets_, dict_builder, dict_tag, "dict");

  std::vector<uint8_t> type_ids = {};
  TypePtr type = TypePtr(new UnionType(types, type_ids, UnionMode::DENSE));
  out->reset(new UnionArray(type, types_.length(), children, types_.data(),
      offsets_.data(), nones_.null_count(), nones_.null_bitmap()));
  return Status::OK();
}
}
