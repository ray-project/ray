#ifndef NUMBUF_LIST_H
#define NUMBUF_LIST_H

#include <arrow/api.h>
#include <arrow/util/logging.h>

namespace numbuf {

class NullArrayBuilder : public arrow::ArrayBuilder {
 public:
  explicit NullArrayBuilder(arrow::MemoryPool* pool, const arrow::TypePtr& type)
      : arrow::ArrayBuilder(pool, type) {}
  virtual ~NullArrayBuilder(){};
  arrow::Status Finish(std::shared_ptr<arrow::Array>* out) override {
    return arrow::Status::OK();
  }
};

/*! A Sequence is a heterogeneous collections of elements. It can contain
    scalar Python types, lists, tuples, dictionaries and tensors.
*/
class SequenceBuilder {
 public:
  SequenceBuilder(arrow::MemoryPool* pool = nullptr);

  //! Appending a none to the sequence
  arrow::Status AppendNone();

  //! Appending a boolean to the sequence
  arrow::Status AppendBool(bool data);

  //! Appending an int64_t to the sequence
  arrow::Status AppendInt64(int64_t data);

  //! Appending an uint64_t to the sequence
  arrow::Status AppendUInt64(uint64_t data);

  //! Append a list of bytes to the sequence
  arrow::Status AppendBytes(const uint8_t* data, int32_t length);

  //! Appending a string to the sequence
  arrow::Status AppendString(const char* data, int32_t length);

  //! Appending a float to the sequence
  arrow::Status AppendFloat(float data);

  //! Appending a double to the sequence
  arrow::Status AppendDouble(double data);

  /*! Appending a tensor to the sequence

      \param tensor_index Index of the tensor in the object.
  */
  arrow::Status AppendTensor(int32_t tensor_index);

  /*! Add a sublist to the sequence. The data contained in the sublist will be
     specified in the "Finish" method.

     To construct l = [[11, 22], 33, [44, 55]] you would for example run
     list = ListBuilder();
     list.AppendList(2);
     list.Append(33);
     list.AppendList(2);
     list.Finish([11, 22, 44, 55]);
     list.Finish();

     \param size
       The size of the sublist
  */
  arrow::Status AppendList(int32_t size);

  arrow::Status AppendTuple(int32_t size);

  arrow::Status AppendDict(int32_t size);

  //! Finish building the sequence and return the result
  arrow::Status Finish(std::shared_ptr<arrow::Array> list_data,
      std::shared_ptr<arrow::Array> tuple_data, std::shared_ptr<arrow::Array> dict_data,
      std::shared_ptr<arrow::Array>* out);

 private:
  arrow::MemoryPool* pool_;

  arrow::Int8Builder types_;
  arrow::Int32Builder offsets_;

  /* Total number of bytes needed to represent this sequence. */
  int64_t total_num_bytes_;

  NullArrayBuilder nones_;
  arrow::BooleanBuilder bools_;
  arrow::Int64Builder ints_;
  arrow::BinaryBuilder bytes_;
  arrow::StringBuilder strings_;
  arrow::FloatBuilder floats_;
  arrow::DoubleBuilder doubles_;

  // We use an Int32Builder here to distinguish the tensor indices from
  // the ints_ above (see the case Type::INT32 in get_value in python.cc).
  // TODO(pcm): Replace this by using the union tags to distinguish between
  // these two cases.
  arrow::Int32Builder tensor_indices_;

  std::vector<int32_t> list_offsets_;
  std::vector<int32_t> tuple_offsets_;
  std::vector<int32_t> dict_offsets_;

  int8_t bool_tag = -1;
  int8_t int_tag = -1;
  int8_t string_tag = -1;
  int8_t bytes_tag = -1;
  int8_t float_tag = -1;
  int8_t double_tag = -1;

  int8_t tensor_tag = -1;
  int8_t list_tag = -1;
  int8_t tuple_tag = -1;
  int8_t dict_tag = -1;

  int8_t num_tags = 0;
};

}  // namespace numbuf

#endif  // NUMBUF_LIST_H
