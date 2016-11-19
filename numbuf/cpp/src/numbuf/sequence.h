#ifndef NUMBUF_LIST_H
#define NUMBUF_LIST_H

#include <arrow/api.h>
#include <arrow/types/union.h>
#include "tensor.h"

namespace numbuf {

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

      \param dims
        A vector of dimensions

      \param data
        A pointer to the start of the data block. The length of the data block
        will be the product of the dimensions
  */
  arrow::Status AppendTensor(const std::vector<int64_t>& dims, uint8_t* data);
  arrow::Status AppendTensor(const std::vector<int64_t>& dims, int8_t* data);
  arrow::Status AppendTensor(const std::vector<int64_t>& dims, uint16_t* data);
  arrow::Status AppendTensor(const std::vector<int64_t>& dims, int16_t* data);
  arrow::Status AppendTensor(const std::vector<int64_t>& dims, uint32_t* data);
  arrow::Status AppendTensor(const std::vector<int64_t>& dims, int32_t* data);
  arrow::Status AppendTensor(const std::vector<int64_t>& dims, uint64_t* data);
  arrow::Status AppendTensor(const std::vector<int64_t>& dims, int64_t* data);
  arrow::Status AppendTensor(const std::vector<int64_t>& dims, float* data);
  arrow::Status AppendTensor(const std::vector<int64_t>& dims, double* data);

  /*! Add a sublist to the sequenc. The data contained in the sublist will be
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
  arrow::Status Finish(
    std::shared_ptr<arrow::Array> list_data,
    std::shared_ptr<arrow::Array> tuple_data,
    std::shared_ptr<arrow::Array> dict_data,
    std::shared_ptr<arrow::Array>* out);

 private:
  arrow::MemoryPool* pool_;

  arrow::Int8Builder types_;
  arrow::Int32Builder offsets_;

  arrow::NullArrayBuilder nones_;
  arrow::BooleanBuilder bools_;
  arrow::Int64Builder ints_;
  arrow::BinaryBuilder bytes_;
  arrow::StringBuilder strings_;
  arrow::FloatBuilder floats_;
  arrow::DoubleBuilder doubles_;

  UInt8TensorBuilder uint8_tensors_;
  Int8TensorBuilder int8_tensors_;
  UInt16TensorBuilder uint16_tensors_;
  Int16TensorBuilder int16_tensors_;
  UInt32TensorBuilder uint32_tensors_;
  Int32TensorBuilder int32_tensors_;
  UInt64TensorBuilder uint64_tensors_;
  Int64TensorBuilder int64_tensors_;
  FloatTensorBuilder float_tensors_;
  DoubleTensorBuilder double_tensors_;

  std::vector<int32_t> list_offsets_;
  std::vector<int32_t> tuple_offsets_;
  std::vector<int32_t> dict_offsets_;

  int8_t bool_tag = -1;
  int8_t int_tag = -1;
  int8_t string_tag = -1;
  int8_t bytes_tag = -1;
  int8_t float_tag = -1;
  int8_t double_tag = -1;

  int8_t uint8_tensor_tag = -1;
  int8_t int8_tensor_tag = -1;
  int8_t uint16_tensor_tag = -1;
  int8_t int16_tensor_tag = -1;
  int8_t uint32_tensor_tag = -1;
  int8_t int32_tensor_tag = -1;
  int8_t uint64_tensor_tag = -1;
  int8_t int64_tensor_tag = -1;
  int8_t float_tensor_tag = -1;
  int8_t double_tensor_tag = -1;

  int8_t list_tag = -1;
  int8_t tuple_tag = -1;
  int8_t dict_tag = -1;

  int8_t num_tags = 0;
};

} // namespace numbuf

#endif // NUMBUF_LIST_H
