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
  FunctionDescriptorInterface();

  /// Construct from a protobuf message object.
  /// The input message will be **copied** into this object.
  ///
  /// \param message The protobuf message.
  FunctionDescriptorInterface(rpc::FunctionDescriptor message);

  ray::FunctionDescriptorType Type() const;

  virtual size_t Hash() const = 0;

  virtual std::string ToString() const = 0;

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
  explicit EmptyFunctionDescriptor();

  virtual size_t Hash() const;

  virtual std::string ToString() const;
};

class JavaFunctionDescriptor : public FunctionDescriptorInterface {
 public:
  /// Construct from a protobuf message object.
  /// The input message will be **copied** into this object.
  ///
  /// \param message The protobuf message.
  explicit JavaFunctionDescriptor(rpc::FunctionDescriptor message);

  virtual size_t Hash() const;

  virtual std::string ToString() const;

  std::string ClassName() const;

  std::string FunctionName() const;

  std::string Signature() const;

 private:
  const rpc::JavaFunctionDescriptor *typed_message_;
};

class PythonFunctionDescriptor : public FunctionDescriptorInterface {
 public:
  /// Construct from a protobuf message object.
  /// The input message will be **copied** into this object.
  ///
  /// \param message The protobuf message.
  explicit PythonFunctionDescriptor(rpc::FunctionDescriptor message);

  virtual size_t Hash() const;

  virtual std::string ToString() const;

  std::string ModuleName() const;

  std::string ClassName() const;

  std::string FunctionName() const;

  std::string FunctionHash() const;

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
