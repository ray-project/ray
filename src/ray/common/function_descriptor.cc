#include "ray/common/function_descriptor.h"

namespace ray {
FunctionDescriptor FunctionDescriptorBuilder::BuildDriver() {
  rpc::FunctionDescriptor descriptor;
  descriptor.mutable_driver_function_descriptor();
  return std::shared_ptr<FunctionDescriptorInterface>(
      new DriverFunctionDescriptor(std::move(descriptor)));
}

FunctionDescriptor FunctionDescriptorBuilder::BuildJava(const std::string &class_name,
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

FunctionDescriptor FunctionDescriptorBuilder::BuildPython(
    const std::string &module_name, const std::string &class_name,
    const std::string &function_name, const std::string &function_hash) {
  rpc::FunctionDescriptor descriptor;
  auto typed_descriptor = descriptor.mutable_python_function_descriptor();
  typed_descriptor->set_module_name(module_name);
  typed_descriptor->set_class_name(class_name);
  typed_descriptor->set_function_name(function_name);
  typed_descriptor->set_function_hash(function_hash);
  return std::shared_ptr<FunctionDescriptorInterface>(
      new PythonFunctionDescriptor(std::move(descriptor)));
}

FunctionDescriptor FunctionDescriptorBuilder::FromProto(rpc::FunctionDescriptor message) {
  switch (message.function_descriptor_case()) {
  case ray::FunctionDescriptorType::kDriverFunctionDescriptor:
    return ray::FunctionDescriptor(new ray::DriverFunctionDescriptor(std::move(message)));
  case ray::FunctionDescriptorType::kJavaFunctionDescriptor:
    return ray::FunctionDescriptor(new ray::JavaFunctionDescriptor(std::move(message)));
  case ray::FunctionDescriptorType::kPythonFunctionDescriptor:
    return ray::FunctionDescriptor(new ray::PythonFunctionDescriptor(std::move(message)));
  default:
    break;
  }
  RAY_LOG(FATAL) << "Unknown function descriptor case: "
                 << message.function_descriptor_case();
  return ray::FunctionDescriptor();
}

FunctionDescriptor FunctionDescriptorBuilder::Deserialize(
    const std::string &serialized_binary) {
  rpc::FunctionDescriptor descriptor;
  descriptor.ParseFromString(serialized_binary);
  return FunctionDescriptorBuilder::FromProto(std::move(descriptor));
}
}  // namespace ray
