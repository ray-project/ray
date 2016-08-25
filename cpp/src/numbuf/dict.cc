#include "dict.h"

using namespace arrow;

namespace numbuf {

std::shared_ptr<arrow::StructArray> DictBuilder::Finish(
    std::shared_ptr<Array> key_tuple_data,
    std::shared_ptr<Array> val_list_data,
    std::shared_ptr<Array> val_tuple_data,
    std::shared_ptr<Array> val_dict_data) {
  // lists and dicts can't be keys of dicts in Python, that is why for
  // the keys we do not need to collect sublists
  auto keys = keys_.Finish(nullptr, key_tuple_data, nullptr);
  auto vals = vals_.Finish(val_list_data, val_tuple_data, val_dict_data);
  auto keys_field = std::make_shared<Field>("keys", keys->type());
  auto vals_field = std::make_shared<Field>("vals", vals->type());
  auto type = std::make_shared<StructType>(std::vector<FieldPtr>({keys_field, vals_field}));
  std::vector<ArrayPtr> field_arrays({keys, vals});
  DCHECK(keys->length() == vals->length());
  return std::make_shared<StructArray>(type, keys->length(), field_arrays);
}

}
