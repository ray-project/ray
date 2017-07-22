#include "sequence.h"

using namespace arrow;

namespace numbuf {

SequenceBuilder::SequenceBuilder(MemoryPool* pool)
    : pool_(pool),
      types_(pool, std::make_shared<Int8Type>()),
      offsets_(pool, std::make_shared<Int32Type>()),
      nones_(pool, std::make_shared<NullType>()),
      bools_(pool, std::make_shared<BooleanType>()),
      ints_(pool, std::make_shared<Int64Type>()),
      bytes_(pool, std::make_shared<BinaryType>()),
      strings_(pool),
      floats_(pool, std::make_shared<FloatType>()),
      doubles_(pool, std::make_shared<DoubleType>()),
      tensor_indices_(pool, std::make_shared<Int32Type>()),
      list_offsets_({0}),
      tuple_offsets_({0}),
      dict_offsets_({0}) {}

#define UPDATE(OFFSET, TAG)               \
  if (TAG == -1) {                        \
    TAG = num_tags;                       \
    num_tags += 1;                        \
  }                                       \
  RETURN_NOT_OK(offsets_.Append(OFFSET)); \
  RETURN_NOT_OK(types_.Append(TAG));      \
  RETURN_NOT_OK(nones_.AppendToBitmap(true));

Status SequenceBuilder::AppendNone() {
  RETURN_NOT_OK(offsets_.Append(0));
  RETURN_NOT_OK(types_.Append(0));
  return nones_.AppendToBitmap(false);
}

Status SequenceBuilder::AppendBool(bool data) {
  UPDATE(bools_.length(), bool_tag);
  return bools_.Append(data);
}

Status SequenceBuilder::AppendInt64(int64_t data) {
  UPDATE(ints_.length(), int_tag);
  return ints_.Append(data);
}

Status SequenceBuilder::AppendUInt64(uint64_t data) {
  UPDATE(ints_.length(), int_tag);
  return ints_.Append(data);
}

Status SequenceBuilder::AppendBytes(const uint8_t* data, int32_t length) {
  UPDATE(bytes_.length(), bytes_tag);
  return bytes_.Append(data, length);
}

Status SequenceBuilder::AppendString(const char* data, int32_t length) {
  UPDATE(strings_.length(), string_tag);
  return strings_.Append(data, length);
}

Status SequenceBuilder::AppendFloat(float data) {
  UPDATE(floats_.length(), float_tag);
  return floats_.Append(data);
}

Status SequenceBuilder::AppendDouble(double data) {
  UPDATE(doubles_.length(), double_tag);
  return doubles_.Append(data);
}

Status SequenceBuilder::AppendTensor(int32_t tensor_index) {
  UPDATE(tensor_indices_.length(), tensor_tag);
  return tensor_indices_.Append(tensor_index);
}

Status SequenceBuilder::AppendList(int32_t size) {
  UPDATE(list_offsets_.size() - 1, list_tag);
  list_offsets_.push_back(list_offsets_.back() + size);
  return Status::OK();
}

Status SequenceBuilder::AppendTuple(int32_t size) {
  UPDATE(tuple_offsets_.size() - 1, tuple_tag);
  tuple_offsets_.push_back(tuple_offsets_.back() + size);
  return Status::OK();
}

Status SequenceBuilder::AppendDict(int32_t size) {
  UPDATE(dict_offsets_.size() - 1, dict_tag);
  dict_offsets_.push_back(dict_offsets_.back() + size);
  return Status::OK();
}

#define ADD_ELEMENT(VARNAME, TAG)                             \
  if (TAG != -1) {                                            \
    types[TAG] = std::make_shared<Field>("", VARNAME.type()); \
    RETURN_NOT_OK(VARNAME.Finish(&children[TAG]));            \
    RETURN_NOT_OK(nones_.AppendToBitmap(true));               \
    type_ids.push_back(TAG);                                  \
  }

#define ADD_SUBSEQUENCE(DATA, OFFSETS, BUILDER, TAG, NAME)                    \
  if (DATA) {                                                                 \
    DCHECK(DATA->length() == OFFSETS.back());                                 \
    std::shared_ptr<Array> offset_array;                                      \
    Int32Builder builder(pool_, std::make_shared<Int32Type>());               \
    RETURN_NOT_OK(builder.Append(OFFSETS.data(), OFFSETS.size()));            \
    RETURN_NOT_OK(builder.Finish(&offset_array));                             \
    std::shared_ptr<Array> list_array;                                        \
    ListArray::FromArrays(*offset_array, *DATA, pool_, &list_array);          \
    auto field = std::make_shared<Field>(NAME, list_array->type());           \
    auto type = std::make_shared<StructType>(std::vector<FieldPtr>({field})); \
    types[TAG] = std::make_shared<Field>("", type);                           \
    children[TAG] = std::shared_ptr<StructArray>(                             \
        new StructArray(type, list_array->length(), {list_array}));           \
    RETURN_NOT_OK(nones_.AppendToBitmap(true));                               \
    type_ids.push_back(TAG);                                                  \
  } else {                                                                    \
    DCHECK(OFFSETS.size() == 1);                                              \
  }

Status SequenceBuilder::Finish(std::shared_ptr<Array> list_data,
    std::shared_ptr<Array> tuple_data, std::shared_ptr<Array> dict_data,
    std::shared_ptr<Array>* out) {
  std::vector<std::shared_ptr<Field>> types(num_tags);
  std::vector<std::shared_ptr<Array>> children(num_tags);
  std::vector<uint8_t> type_ids;

  ADD_ELEMENT(bools_, bool_tag);
  ADD_ELEMENT(ints_, int_tag);
  ADD_ELEMENT(strings_, string_tag);
  ADD_ELEMENT(bytes_, bytes_tag);
  ADD_ELEMENT(floats_, float_tag);
  ADD_ELEMENT(doubles_, double_tag);

  ADD_ELEMENT(tensor_indices_, tensor_tag);

  ADD_SUBSEQUENCE(list_data, list_offsets_, list_builder, list_tag, "list");
  ADD_SUBSEQUENCE(tuple_data, tuple_offsets_, tuple_builder, tuple_tag, "tuple");
  ADD_SUBSEQUENCE(dict_data, dict_offsets_, dict_builder, dict_tag, "dict");

  TypePtr type = TypePtr(new UnionType(types, type_ids, UnionMode::DENSE));
  out->reset(new UnionArray(type, types_.length(), children, types_.data(),
      offsets_.data(), nones_.null_bitmap(), nones_.null_count()));
  return Status::OK();
}
}
