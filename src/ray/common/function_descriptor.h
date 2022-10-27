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

#pragma once

#include <string>

#include "ray/common/grpc_util.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {
/// See `common.proto` for definition of `FunctionDescriptor` oneof type.
using FunctionDescriptorType = rpc::FunctionDescriptor::FunctionDescriptorCase;
/// Wrap a protobuf message.
class FunctionDescriptorInterface : public MessageWrapper<rpc::FunctionDescriptor> {
 public:
  virtual ~FunctionDescriptorInterface() {}

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

  // DO NOT define operator==() or operator!=() in the base class.
  // Let the derived classes define and implement.
  // This is to avoid unexpected behaviors when comparing function descriptors of
  // different declard types, as in this case, the base class version is invoked.

  virtual std::string ToString() const = 0;

  // A one-word summary of the function call site (e.g., __main__.foo).
  virtual std::string CallSiteString() const { return CallString(); }

  // The function or method call, e.g. "foo()" or "Bar.foo()". This does not include the
  // module/library.
  virtual std::string CallString() const = 0;

  // The class name of the actor (e.g., "Bar"), or the empty string.
  virtual std::string ClassName() const = 0;

  // The default name for a task that executes this function.
  virtual std::string DefaultTaskName() const { return CallString(); }

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

  inline bool operator==(const EmptyFunctionDescriptor &other) const { return true; }

  inline bool operator!=(const EmptyFunctionDescriptor &other) const { return false; }

  virtual std::string ToString() const { return "{type=EmptyFunctionDescriptor}"; }

  virtual std::string ClassName() const { return ""; }

  virtual std::string CallString() const { return ""; }
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

  inline bool operator==(const JavaFunctionDescriptor &other) const {
    if (this == &other) {
      return true;
    }
    return this->ClassName() == other.ClassName() &&
           this->FunctionName() == other.FunctionName() &&
           this->Signature() == other.Signature();
  }

  inline bool operator!=(const JavaFunctionDescriptor &other) const {
    return !(*this == other);
  }

  virtual std::string ToString() const {
    return "{type=JavaFunctionDescriptor, class_name=" + typed_message_->class_name() +
           ", function_name=" + typed_message_->function_name() +
           ", signature=" + typed_message_->signature() + "}";
  }

  virtual std::string CallString() const {
    const std::string &class_name = typed_message_->class_name();
    const std::string &function_name = typed_message_->function_name();
    return class_name.empty() ? function_name : class_name + "." + function_name;
  }

  virtual std::string ClassName() const { return typed_message_->class_name(); }

  const std::string &FunctionName() const { return typed_message_->function_name(); }

  const std::string &Signature() const { return typed_message_->signature(); }

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

  inline bool operator==(const PythonFunctionDescriptor &other) const {
    if (this == &other) {
      return true;
    }
    return this->ModuleName() == other.ModuleName() &&
           this->ClassName() == other.ClassName() &&
           this->FunctionName() == other.FunctionName() &&
           this->FunctionHash() == other.FunctionHash();
  }

  inline bool operator!=(const PythonFunctionDescriptor &other) const {
    return !(*this == other);
  }

  virtual std::string ToString() const {
    return "{type=PythonFunctionDescriptor, module_name=" +
           typed_message_->module_name() +
           ", class_name=" + typed_message_->class_name() +
           ", function_name=" + typed_message_->function_name() +
           ", function_hash=" + typed_message_->function_hash() + "}";
  }

  virtual std::string CallSiteString() const {
    return typed_message_->module_name() + "." + CallString();
  }

  virtual std::string CallString() const {
    const std::string &class_name = typed_message_->class_name();
    const std::string &function_name = typed_message_->function_name();
    if (class_name.empty()) {
      return function_name.substr(function_name.find_last_of(".") + 1);
    } else {
      return class_name.substr(class_name.find_last_of(".") + 1) + "." +
             function_name.substr(function_name.find_last_of(".") + 1);
    }
  }

  virtual std::string ClassName() const { return typed_message_->class_name(); }

  const std::string &ModuleName() const { return typed_message_->module_name(); }

  const std::string &FunctionName() const { return typed_message_->function_name(); }

  const std::string &FunctionHash() const { return typed_message_->function_hash(); }

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
           std::hash<std::string>()(typed_message_->function_name()) ^
           std::hash<std::string>()(typed_message_->class_name());
  }

  inline bool operator==(const CppFunctionDescriptor &other) const {
    if (this == &other) {
      return true;
    }
    return this->FunctionName() == other.FunctionName() &&
           this->ClassName() == other.ClassName();
  }

  inline bool operator!=(const CppFunctionDescriptor &other) const {
    return !(*this == other);
  }

  virtual std::string ToString() const {
    std::string class_name = ClassName().empty() ? "" : ", class_name=" + ClassName();
    return "{type=CppFunctionDescriptor, function_name=" +
           typed_message_->function_name() + class_name + "}";
  }

  virtual std::string CallString() const { return typed_message_->function_name(); }

  virtual std::string DefaultTaskName() const { return CallString(); }

  virtual std::string ClassName() const { return typed_message_->class_name(); }

  const std::string &FunctionName() const { return typed_message_->function_name(); }

  const std::string &Caller() const { return typed_message_->caller(); }

 private:
  const rpc::CppFunctionDescriptor *typed_message_;
};

typedef std::shared_ptr<FunctionDescriptorInterface> FunctionDescriptor;

inline bool operator==(const FunctionDescriptor &left, const FunctionDescriptor &right) {
  if (left.get() == right.get()) {
    return true;
  }
  if (left.get() == nullptr || right.get() == nullptr) {
    return false;
  }
  if (left->Type() != right->Type()) {
    return false;
  }
  switch (left->Type()) {
  case ray::FunctionDescriptorType::FUNCTION_DESCRIPTOR_NOT_SET:
    return static_cast<const EmptyFunctionDescriptor &>(*left) ==
           static_cast<const EmptyFunctionDescriptor &>(*right);
  case ray::FunctionDescriptorType::kJavaFunctionDescriptor:
    return static_cast<const JavaFunctionDescriptor &>(*left) ==
           static_cast<const JavaFunctionDescriptor &>(*right);
  case ray::FunctionDescriptorType::kPythonFunctionDescriptor:
    return static_cast<const PythonFunctionDescriptor &>(*left) ==
           static_cast<const PythonFunctionDescriptor &>(*right);
  case ray::FunctionDescriptorType::kCppFunctionDescriptor:
    return static_cast<const CppFunctionDescriptor &>(*left) ==
           static_cast<const CppFunctionDescriptor &>(*right);
  default:
    RAY_LOG(FATAL) << "Unknown function descriptor type: " << left->Type();
    return false;
  }
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
  static FunctionDescriptor BuildCpp(const std::string &function_name,
                                     const std::string &caller = "",
                                     const std::string &class_name = "");

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
