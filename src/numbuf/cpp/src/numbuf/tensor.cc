#include "tensor.h"

#include <limits>
#include <sstream>

using namespace arrow;

namespace numbuf {

template <typename T>
TensorBuilder<T>::TensorBuilder(const TypePtr& dtype, MemoryPool* pool)
    : dtype_(dtype), pool_(pool) {}

template <typename T>
Status TensorBuilder<T>::Start() {
  dim_data_ = std::make_shared<Int64Builder>(pool_, std::make_shared<Int64Type>());
  dims_ = std::make_shared<ListBuilder>(pool_, dim_data_);
  value_data_ = std::make_shared<PrimitiveBuilder<T>>(pool_, dtype_);
  values_ = std::make_shared<ListBuilder>(pool_, value_data_);
  auto dims_field = std::make_shared<Field>("dims", dims_->type());
  auto values_field = std::make_shared<Field>("data", values_->type());
  auto type =
      std::make_shared<StructType>(std::vector<FieldPtr>({dims_field, values_field}));
  tensors_ = std::make_shared<StructBuilder>(
      pool_, type, std::vector<std::shared_ptr<ArrayBuilder>>({dims_, values_}));
  return Status::OK();
}

template <typename T>
Status TensorBuilder<T>::Append(const std::vector<int64_t>& dims, const elem_type* data) {
  DCHECK(tensors_);
  RETURN_NOT_OK(tensors_->Append());
  RETURN_NOT_OK(dims_->Append());
  RETURN_NOT_OK(values_->Append());
  int64_t size = 1;
  for (auto dim : dims) {
    size *= dim;
    RETURN_NOT_OK(dim_data_->Append(dim));
  }
  if (size * sizeof(T) >= std::numeric_limits<int32_t>::max()) {
    std::stringstream stream;
    stream << std::string("Numpy array of dimensions ");
    for (auto dim : dims) {
      stream << dim << std::string(" ");
    }
    stream << std::string("too large to be converted to arrow") << std::endl;
    return Status::NotImplemented(stream.str());
  }
  RETURN_NOT_OK(value_data_->Append(data, size));
  return Status::OK();  // tensors_->Append();
}

template <typename T>
Status TensorBuilder<T>::Finish(std::shared_ptr<Array>* out) {
  return tensors_->Finish(out);
}

template class TensorBuilder<UInt8Type>;
template class TensorBuilder<Int8Type>;
template class TensorBuilder<UInt16Type>;
template class TensorBuilder<Int16Type>;
template class TensorBuilder<UInt32Type>;
template class TensorBuilder<Int32Type>;
template class TensorBuilder<UInt64Type>;
template class TensorBuilder<Int64Type>;
template class TensorBuilder<FloatType>;
template class TensorBuilder<DoubleType>;
}
