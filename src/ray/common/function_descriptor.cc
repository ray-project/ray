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
  // When TaskSpecification() constructed without function_descriptor set,
  // we should return a valid ray::FunctionDescriptor instance.
  return FunctionDescriptorBuilder::Empty();
}

FunctionDescriptor FunctionDescriptorBuilder::FromVector(
    rpc::Language language, const std::vector<std::string> &function_descriptor_list) {
  if (language == rpc::Language::JAVA) {
    RAY_CHECK(function_descriptor_list.size() == 3);
    return FunctionDescriptorBuilder::BuildJava(
        function_descriptor_list[0],  // class name
        function_descriptor_list[1],  // function name
        function_descriptor_list[2]   // signature
    );
  } else if (language == rpc::Language::PYTHON) {
    RAY_CHECK(function_descriptor_list.size() == 4);
    return FunctionDescriptorBuilder::BuildPython(
        function_descriptor_list[0],  // module name
        function_descriptor_list[1],  // class name
        function_descriptor_list[2],  // function name
        function_descriptor_list[3]   // function hash
    );
  } else {
    RAY_LOG(FATAL) << "Unspported language " << language;
    return FunctionDescriptorBuilder::Empty();
  }
}

FunctionDescriptor FunctionDescriptorBuilder::Deserialize(
    const std::string &serialized_binary) {
  rpc::FunctionDescriptor descriptor;
  descriptor.ParseFromString(serialized_binary);
  return FunctionDescriptorBuilder::FromProto(std::move(descriptor));
}

ray::FunctionDescriptorType ray::FunctionDescriptorInterface::Type() const {
  return message_->function_descriptor_case();
}

size_t ray::EmptyFunctionDescriptor::Hash() const {
  return std::hash<int>()(ray::FunctionDescriptorType::FUNCTION_DESCRIPTOR_NOT_SET);
}

std::string ray::EmptyFunctionDescriptor::ToString() const {
  return "{type=EmptyFunctionDescriptor}";
}

size_t ray::JavaFunctionDescriptor::Hash() const {
  return std::hash<int>()(ray::FunctionDescriptorType::kJavaFunctionDescriptor) ^
         std::hash<std::string>()(typed_message_->class_name()) ^
         std::hash<std::string>()(typed_message_->function_name()) ^
         std::hash<std::string>()(typed_message_->signature());
}

std::string ray::JavaFunctionDescriptor::ToString() const {
  return "{type=JavaFunctionDescriptor, class_name=" + typed_message_->class_name() +
         ", function_name=" + typed_message_->function_name() +
         ", signature=" + typed_message_->signature() + "}";
}

std::string ray::JavaFunctionDescriptor::ClassName() const {
  return typed_message_->class_name();
}

std::string ray::JavaFunctionDescriptor::FunctionName() const {
  return typed_message_->function_name();
}

std::string ray::JavaFunctionDescriptor::Signature() const {
  return typed_message_->signature();
}

size_t ray::PythonFunctionDescriptor::Hash() const {
  return std::hash<int>()(ray::FunctionDescriptorType::kPythonFunctionDescriptor) ^
         std::hash<std::string>()(typed_message_->module_name()) ^
         std::hash<std::string>()(typed_message_->class_name()) ^
         std::hash<std::string>()(typed_message_->function_name()) ^
         std::hash<std::string>()(typed_message_->function_hash());
}

std::string ray::PythonFunctionDescriptor::ToString() const {
  return "{type=PythonFunctionDescriptor, module_name=" + typed_message_->module_name() +
         ", class_name=" + typed_message_->class_name() +
         ", function_name=" + typed_message_->function_name() +
         ", function_hash=" + typed_message_->function_hash() + "}";
}

std::string ray::PythonFunctionDescriptor::ModuleName() const {
  return typed_message_->module_name();
}

std::string ray::PythonFunctionDescriptor::ClassName() const {
  return typed_message_->class_name();
}

std::string ray::PythonFunctionDescriptor::FunctionName() const {
  return typed_message_->function_name();
}

std::string ray::PythonFunctionDescriptor::FunctionHash() const {
  return typed_message_->function_hash();
}

FunctionDescriptorInterface::FunctionDescriptorInterface() : MessageWrapper() {}

FunctionDescriptorInterface::FunctionDescriptorInterface(rpc::FunctionDescriptor message)
    : MessageWrapper(std::move(message)) {}

EmptyFunctionDescriptor::EmptyFunctionDescriptor() : FunctionDescriptorInterface() {
  RAY_CHECK(message_->function_descriptor_case() ==
            ray::FunctionDescriptorType::FUNCTION_DESCRIPTOR_NOT_SET);
}

JavaFunctionDescriptor::JavaFunctionDescriptor(rpc::FunctionDescriptor message)
    : FunctionDescriptorInterface(std::move(message)) {
  RAY_CHECK(message_->function_descriptor_case() ==
            ray::FunctionDescriptorType::kJavaFunctionDescriptor);
  typed_message_ = &(message_->java_function_descriptor());
}

PythonFunctionDescriptor::PythonFunctionDescriptor(rpc::FunctionDescriptor message)
    : FunctionDescriptorInterface(std::move(message)) {
  RAY_CHECK(message_->function_descriptor_case() ==
            ray::FunctionDescriptorType::kPythonFunctionDescriptor);
  typed_message_ = &(message_->python_function_descriptor());
}

}  // namespace ray
