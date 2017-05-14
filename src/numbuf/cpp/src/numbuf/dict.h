#ifndef NUMBUF_DICT_H
#define NUMBUF_DICT_H

#include <arrow/api.h>

#include "sequence.h"

namespace numbuf {

/*! Constructing dictionaries of key/value pairs. Sequences of
    keys and values are built separately using a pair of
    SequenceBuilders. The resulting Arrow representation
    can be obtained via the Finish method.
*/
class DictBuilder {
 public:
  DictBuilder(arrow::MemoryPool* pool = nullptr) : keys_(pool), vals_(pool) {}

  //! Builder for the keys of the dictionary
  SequenceBuilder& keys() { return keys_; }
  //! Builder for the values of the dictionary
  SequenceBuilder& vals() { return vals_; }

  /*! Construct an Arrow StructArray representing the dictionary.
      Contains a field "keys" for the keys and "vals" for the values.

      \param list_data
        List containing the data from nested lists in the value
        list of the dictionary

      \param dict_data
        List containing the data from nested dictionaries in the
        value list of the dictionary
  */
  arrow::Status Finish(std::shared_ptr<arrow::Array> key_tuple_data,
      std::shared_ptr<arrow::Array> key_dict_data,
      std::shared_ptr<arrow::Array> val_list_data,
      std::shared_ptr<arrow::Array> val_tuple_data,
      std::shared_ptr<arrow::Array> val_dict_data, std::shared_ptr<arrow::Array>* out);

 private:
  SequenceBuilder keys_;
  SequenceBuilder vals_;
};
}

#endif
