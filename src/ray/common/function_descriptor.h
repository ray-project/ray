// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef RAY_CORE_WORKER_FUNCTION_DESCRIPTOR_H
#define RAY_CORE_WORKER_FUNCTION_DESCRIPTOR_H

#include <string>

#include "ray/common/grpc_util.h"
#include "ray/protobuf/common.pb.h"

namespace ray {
/// See `common.proto` for definition of `FunctionDescriptor` oneof type.
using FunctionDescriptorType = rpc::FunctionDescriptor::FunctionDescriptorCase;
/// Wrap a protobuf message.
class FunctionDescriptorInterface : public MessageWrapper<rpc::FunctionDescriptor> {
 public:
  /// Construct an empty FunctionDescriptor.
  FunctionDescriptorInterface() : MessageWrapper() {}

  /// Construct from a protobuf message object.
  /// The input message will be **copied** into this object.
  ///
  /// \param message The protobuf message.
  FunctionDescriptorInterface(rpc::FunctionDescriptor message)
      : MessageWrapper(std::move(message)) {}

  ray::FunctionDescriptorType Type() const {
    return message_->function_descriptor_case();
  }

  virtual size_t Hash() const = 0;

  virtual std::string ToString() const = 0;

  // A one-word summary of the function call site (e.g., __main__.foo).
  virtual std::string CallSiteString() const { return ToString(); }

  template <typename Subtype>
  Subtype *As() {
    return reinterpret_cast<Subtype *>(this);
  }
};

class EmptyFunctionDescriptor : public FunctionDescriptorInterface {
 public:
  /// Construct from a protobuf message object.
  /// The input message will be **copied** into this object.
  ///
  /// \param message The protobuf message.
  explicit EmptyFunctionDescriptor() : FunctionDescriptorInterface() {
    RAY_CHECK(message_->function_descriptor_case() ==
              ray::FunctionDescriptorType::FUNCTION_DESCRIPTOR_NOT_SET);
  }

  virtual size_t Hash() const {
    return std::hash<int>()(ray::FunctionDescriptorType::FUNCTION_DESCRIPTOR_NOT_SET);
  }

  virtual std::string ToString() const { return "{type=EmptyFunctionDescriptor}"; }
};

class JavaFunctionDescriptor : public FunctionDescriptorInterface {
 public:
  /// Construct from a protobuf message object.
  /// The input message will be **copied** into this object.
  ///
  /// \param message The protobuf message.
  explicit JavaFunctionDescriptor(rpc::FunctionDescriptor message)
      : FunctionDescriptorInterface(std::move(message)) {
    RAY_CHECK(message_->function_descriptor_case() ==
              ray::FunctionDescriptorType::kJavaFunctionDescriptor);
    typed_message_ = &(message_->java_function_descriptor());
  }

  virtual size_t Hash() const {
    return std::hash<int>()(ray::FunctionDescriptorType::kJavaFunctionDescriptor) ^
           std::hash<std::string>()(typed_message_->class_name()) ^
           std::hash<std::string>()(typed_message_->function_name()) ^
           std::hash<std::string>()(typed_message_->signature());
  }

  virtual std::string ToString() const {
    return "{type=JavaFunctionDescriptor, class_name=" + typed_message_->class_name() +
           ", function_name=" + typed_message_->function_name() +
           ", signature=" + typed_message_->signature() + "}";
  }

  std::string ClassName() const { return typed_message_->class_name(); }

  std::string FunctionName() const { return typed_message_->function_name(); }

  std::string Signature() const { return typed_message_->signature(); }

 private:
  const rpc::JavaFunctionDescriptor *typed_message_;
};

class PythonFunctionDescriptor : public FunctionDescriptorInterface {
 public:
  /// Construct from a protobuf message object.
  /// The input message will be **copied** into this object.
  ///
  /// \param message The protobuf message.
  explicit PythonFunctionDescriptor(rpc::FunctionDescriptor message)
      : FunctionDescriptorInterface(std::move(message)) {
    RAY_CHECK(message_->function_descriptor_case() ==
              ray::FunctionDescriptorType::kPythonFunctionDescriptor);
    typed_message_ = &(message_->python_function_descriptor());
  }

  virtual size_t Hash() const {
    return std::hash<int>()(ray::FunctionDescriptorType::kPythonFunctionDescriptor) ^
           std::hash<std::string>()(typed_message_->module_name()) ^
           std::hash<std::string>()(typed_message_->class_name()) ^
           std::hash<std::string>()(typed_message_->function_name()) ^
           std::hash<std::string>()(typed_message_->function_hash());
  }

  virtual std::string ToString() const {
    return "{type=PythonFunctionDescriptor, module_name=" +
           typed_message_->module_name() +
           ", class_name=" + typed_message_->class_name() +
           ", function_name=" + typed_message_->function_name() +
           ", function_hash=" + typed_message_->function_hash() + "}";
  }

  virtual std::string CallSiteString() const {
    return typed_message_->module_name() + "." + typed_message_->class_name() + "." +
           typed_message_->function_name();
  }

  std::string ModuleName() const { return typed_message_->module_name(); }

  std::string ClassName() const { return typed_message_->class_name(); }

  std::string FunctionName() const { return typed_message_->function_name(); }

  std::string FunctionHash() const { return typed_message_->function_hash(); }

 private:
  const rpc::PythonFunctionDescriptor *typed_message_;
};

class CppFunctionDescriptor : public FunctionDescriptorInterface {
 public:
  /// Construct from a protobuf message object.
  /// The input message will be **copied** into this object.
  ///
  /// \param message The protobuf message.
  explicit CppFunctionDescriptor(rpc::FunctionDescriptor message)
      : FunctionDescriptorInterface(std::move(message)) {
    RAY_CHECK(message_->function_descriptor_case() ==
              ray::FunctionDescriptorType::kCppFunctionDescriptor);
    typed_message_ = &(message_->cpp_function_descriptor());
  }

  virtual size_t Hash() const {
    return std::hash<int>()(ray::FunctionDescriptorType::kCppFunctionDescriptor) ^
           std::hash<std::string>()(typed_message_->lib_name()) ^
           std::hash<std::string>()(typed_message_->function_offset()) ^
           std::hash<std::string>()(typed_message_->exec_function_offset());
  }

  virtual std::string ToString() const {
    return "{type=CppFunctionDescriptor, lib_name=" + typed_message_->lib_name() +
           ", function_offset=" + typed_message_->function_offset() +
           ", exec_function_offset=" + typed_message_->exec_function_offset() + "}";
  }

  std::string LibName() const { return typed_message_->lib_name(); }

  std::string FunctionOffset() const { return typed_message_->function_offset(); }

  std::string ExecFunctionOffset() const {
    return typed_message_->exec_function_offset();
  }

 private:
  const rpc::CppFunctionDescriptor *typed_message_;
};

typedef std::shared_ptr<FunctionDescriptorInterface> FunctionDescriptor;

inline bool operator==(const FunctionDescriptor &left, const FunctionDescriptor &right) {
  if (left.get() != nullptr && right.get() != nullptr && left->Type() == right->Type() &&
      left->ToString() == right->ToString()) {
    return true;
  }
  return left.get() == right.get();
}

inline bool operator!=(const FunctionDescriptor &left, const FunctionDescriptor &right) {
  return !(left == right);
}

/// Helper class for building a `FunctionDescriptor` object.
class FunctionDescriptorBuilder {
 public:
  /// Build an EmptyFunctionDescriptor.
  ///
  /// \return a ray::EmptyFunctionDescriptor
  static FunctionDescriptor Empty();

  /// Build a JavaFunctionDescriptor.
  ///
  /// \return a ray::JavaFunctionDescriptor
  static FunctionDescriptor BuildJava(const std::string &class_name,
                                      const std::string &function_name,
                                      const std::string &signature);

  /// Build a PythonFunctionDescriptor.
  ///
  /// \return a ray::PythonFunctionDescriptor
  static FunctionDescriptor BuildPython(const std::string &module_name,
                                        const std::string &class_name,
                                        const std::string &function_name,
                                        const std::string &function_hash);

  /// Build a CppFunctionDescriptor.
  ///
  /// \return a ray::CppFunctionDescriptor
  static FunctionDescriptor BuildCpp(const std::string &lib_name,
                                     const std::string &function_offset,
                                     const std::string &exec_function_offset);

  /// Build a ray::FunctionDescriptor according to input message.
  ///
  /// \return new ray::FunctionDescriptor
  static FunctionDescriptor FromProto(rpc::FunctionDescriptor message);

  /// Build a ray::FunctionDescriptor from language and vector.
  ///
  /// \return new ray::FunctionDescriptor
  static FunctionDescriptor FromVector(
      rpc::Language language, const std::vector<std::string> &function_descriptor_list);

  /// Build a ray::FunctionDescriptor from serialized binary.
  ///
  /// \return new ray::FunctionDescriptor
  static FunctionDescriptor Deserialize(const std::string &serialized_binary);
};
}  // namespace ray

#endif
