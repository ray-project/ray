#ifndef FUNCION_DESCRIPTOR_H
#define FUNCION_DESCRIPTOR_H

#include <string>
#include <vector>

#include "ray/constants.h"
#include "ray/id.h"

namespace ray {
/// The function descriptor.
/// For a Python function, it should be: [module_name, class_name,
/// function_name, [function_id]]. Function_id is optional for python.
/// For a Java function, it should be: [class_name, method_name,
/// type_descriptor].
class FunctionDescriptor {
 public:
  /// Create function descriptor from a function id.
  /// The other field will be empty strings.
  /// \param function_id The function_id to use (optional).
  FunctionDescriptor(
      const ray::FunctionID &function_id = ray::FunctionID::nil())
      : string_vec_(3) {
    if (!function_id.is_nil()) {
      string_vec_.push_back(function_id.binary());
    }
  }

  /// Create function descriptor from the fields needs by function descriptor.
  /// \param name1 The module_name in Python or class_name in Java.
  /// \param name2 The class_name in Python or method_name in Java.
  /// \param name3 The function_name in Python or type_descriptor in Java.
  /// \param function_id The function_id to use (optional).
  FunctionDescriptor(
      const std::string &name1,
      const std::string &name2,
      const std::string &name3,
      const ray::FunctionID &function_id = ray::FunctionID::nil()) {
    string_vec_.push_back(name1);
    string_vec_.push_back(name2);
    string_vec_.push_back(name3);
    if (!function_id.is_nil()) {
      string_vec_.push_back(function_id.binary());
    }
  }

  /// Get the function id from function descriptor.
  /// \return The function id which could be nil.
  ray::FunctionID GetFunctionId() const {
    if (string_vec_.size() > 3) {
      return ray::FunctionID::from_binary(string_vec_[3]);
    } else {
      return ray::FunctionID::nil();
    }
  }

  /// Get the function descriptor string vector.
  /// \return The string vector which will be put into GCS.
  const std::vector<std::string> &GetDescriptorVector() const {
    return string_vec_;
  }

  /// Set current function descriptor from a string vector.
  /// The caller is responsible to check the input vector using IsValidVector.
  /// \param function_descriptor_vec The string vector to save directly.
  void SetDescriptorVector(
      const std::vector<std::string> &function_descriptor_vec) {
    string_vec_ = function_descriptor_vec;
  }

  /// Check whether a string vector is a valid function descriptor vector.
  /// \param vec The input string vector to check.
  /// \return Whether or not input vector is valid.
  static bool IsValidVector(const std::vector<std::string> &vec) {
    if (vec.size() == 3) {
      return true;
    } else if (vec.size() == 4) {
      if (vec[3].size() == kUniqueIDSize) {
        return true;
      }
    }
    return false;
  }

  /// Check whether current task is a driver task. A driver task is a special
  /// task that is created when a driver started to create an entry for
  /// the driver task in the task table.
  bool IsDriverTask() const {
    if (string_vec_.size() == 4 &&
        FunctionID::from_binary(string_vec_[3]).is_nil()) {
      return true;
    }
    return false;
  }

  /// Create a fucntion decriptor that represent the special driver task.
  /// In ordinary FunctionDescriptor constructor, Nil function Id won't
  /// be output to the string vector. For driver task function descriptor,
  /// this Nil function id will be contained in the string vector.
  static FunctionDescriptor CreateDriverTask() {
    FunctionDescriptor function_descriptor;
    function_descriptor.string_vec_.push_back(FunctionID::nil().binary());
    return function_descriptor;
  }

  FunctionDescriptor(const FunctionDescriptor &other) = default;

  FunctionDescriptor &operator=(const FunctionDescriptor &other) = default;

 private:
  std::vector<std::string> string_vec_;
};

}  // namespace ray

#endif /* FUNCION_DESCRIPTOR_H */
