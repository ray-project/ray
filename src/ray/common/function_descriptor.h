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

  template <typename Subtype>
  Subtype *As() {
    return reinterpret_cast<Subtype *>(this);
  }
};

class DriverFunctionDescriptor : public FunctionDescriptorInterface {
 public:
  /// Construct from a protobuf message object.
  /// The input message will be **copied** into this object.
  ///
  /// \param message The protobuf message.
  explicit DriverFunctionDescriptor(rpc::FunctionDescriptor message)
      : FunctionDescriptorInterface(std::move(message)) {
    RAY_CHECK(message_->function_descriptor_case() ==
              ray::FunctionDescriptorType::kDriverFunctionDescriptor);
  }

  virtual size_t Hash() const {
    return std::hash<int>()(ray::FunctionDescriptorType::kDriverFunctionDescriptor);
  }

  virtual std::string ToString() const { return "{type=DriverFunctionDescriptor}"; }
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

  size_t Hash() const {
    return std::hash<int>()(ray::FunctionDescriptorType::kPythonFunctionDescriptor) ^
           std::hash<std::string>()(typed_message_->module_name()) ^
           std::hash<std::string>()(typed_message_->class_name()) ^
           std::hash<std::string>()(typed_message_->function_name()) ^
           std::hash<std::string>()(typed_message_->function_hash());
  }

  std::string ToString() const {
    return "{type=PythonFunctionDescriptor, module_name=" +
           typed_message_->module_name() +
           ", class_name=" + typed_message_->class_name() +
           ", function_name=" + typed_message_->function_name() +
           ", function_hash=" + typed_message_->function_hash() + "}";
  }

  std::string ModuleName() const { return typed_message_->module_name(); }

  std::string ClassName() const { return typed_message_->class_name(); }

  std::string FunctionName() const { return typed_message_->function_name(); }

  std::string FunctionHash() const { return typed_message_->function_hash(); }

 private:
  const rpc::PythonFunctionDescriptor *typed_message_;
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
  static FunctionDescriptor BuildDriver() {
    rpc::FunctionDescriptor descriptor;
    descriptor.mutable_driver_function_descriptor();
    return std::shared_ptr<FunctionDescriptorInterface>(
        new DriverFunctionDescriptor(std::move(descriptor)));
  }

  static FunctionDescriptor BuildJava(const std::string &class_name,
                                      const std::string &function_name,
                                      const std::string &signature) {
    rpc::FunctionDescriptor descriptor;
    auto typed_descriptor = descriptor.mutable_java_function_descriptor();
    typed_descriptor->set_class_name(class_name);
    typed_descriptor->set_function_name(function_name);
    typed_descriptor->set_signature(signature);
    return std::shared_ptr<FunctionDescriptorInterface>(
        new JavaFunctionDescriptor(std::move(descriptor)));
  }

  static FunctionDescriptor BuildPython(const std::string &module_name,
                                        const std::string &class_name,
                                        const std::string &function_name,
                                        const std::string &function_hash) {
    rpc::FunctionDescriptor descriptor;
    auto typed_descriptor = descriptor.mutable_python_function_descriptor();
    typed_descriptor->set_module_name(module_name);
    typed_descriptor->set_class_name(class_name);
    typed_descriptor->set_function_name(function_name);
    typed_descriptor->set_function_hash(function_hash);
    return std::shared_ptr<FunctionDescriptorInterface>(
        new PythonFunctionDescriptor(std::move(descriptor)));
  }

  static FunctionDescriptor FromProto(rpc::FunctionDescriptor message) {
    switch (message.function_descriptor_case()) {
    case ray::FunctionDescriptorType::kDriverFunctionDescriptor:
      return ray::FunctionDescriptor(
          new ray::DriverFunctionDescriptor(std::move(message)));
    case ray::FunctionDescriptorType::kJavaFunctionDescriptor:
      return ray::FunctionDescriptor(new ray::JavaFunctionDescriptor(std::move(message)));
    case ray::FunctionDescriptorType::kPythonFunctionDescriptor:
      return ray::FunctionDescriptor(
          new ray::PythonFunctionDescriptor(std::move(message)));
    default:
      break;
    }
    RAY_LOG(FATAL) << "Unknown function descriptor case: "
                   << message.function_descriptor_case();
    return ray::FunctionDescriptor();
  }

  static FunctionDescriptor Deserialize(const std::string &serialized_binary) {
    rpc::FunctionDescriptor descriptor;
    descriptor.ParseFromString(serialized_binary);
    return FunctionDescriptorBuilder::FromProto(std::move(descriptor));
  }
};
}  // namespace ray

#endif
