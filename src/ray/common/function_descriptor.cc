#include "ray/common/function_descriptor.h"

namespace ray {
FunctionDescriptor FunctionDescriptorBuilder::Empty() {
  static ray::FunctionDescriptor empty =
      ray::FunctionDescriptor(new EmptyFunctionDescriptor());
  return empty;
}

FunctionDescriptor FunctionDescriptorBuilder::BuildJava(const std::string &class_name,
                                                        const std::string &function_name,
                                                        const std::string &signature) {
  rpc::FunctionDescriptor descriptor;
  auto typed_descriptor = descriptor.mutable_java_function_descriptor();
  typed_descriptor->set_class_name(class_name);
  typed_descriptor->set_function_name(function_name);
  typed_descriptor->set_signature(signature);
  return ray::FunctionDescriptor(new JavaFunctionDescriptor(std::move(descriptor)));
}

FunctionDescriptor FunctionDescriptorBuilder::BuildPython(
    const std::string &module_name, const std::string &class_name,
    const std::string &function_name, const std::string &function_hash) {
  rpc::FunctionDescriptor descriptor;
  auto typed_descriptor = descriptor.mutable_python_function_descriptor();
  typed_descriptor->set_module_name(module_name);
  typed_descriptor->set_class_name(class_name);
  typed_descriptor->set_function_name(function_name);
  typed_descriptor->set_function_hash(function_hash);
  return ray::FunctionDescriptor(new PythonFunctionDescriptor(std::move(descriptor)));
}

FunctionDescriptor FunctionDescriptorBuilder::FromProto(rpc::FunctionDescriptor message) {
  switch (message.function_descriptor_case()) {
  case ray::FunctionDescriptorType::kJavaFunctionDescriptor:
    return ray::FunctionDescriptor(new ray::JavaFunctionDescriptor(std::move(message)));
  case ray::FunctionDescriptorType::kPythonFunctionDescriptor:
    return ray::FunctionDescriptor(new ray::PythonFunctionDescriptor(std::move(message)));
  default:
    break;
  }
  RAY_LOG(DEBUG) << "Unknown function descriptor case: "
                 << message.function_descriptor_case();
  // When TaskSpecification() constructed without function_descriptor set,
  // we should return a valid ray::FunctionDescriptor instance.
  return FunctionDescriptorBuilder::Empty();
}

FunctionDescriptor FunctionDescriptorBuilder::Deserialize(
    const std::string &serialized_binary) {
  rpc::FunctionDescriptor descriptor;
  descriptor.ParseFromString(serialized_binary);
  return FunctionDescriptorBuilder::FromProto(std::move(descriptor));
}
}  // namespace ray
