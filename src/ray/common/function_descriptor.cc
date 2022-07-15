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
    const std::string &module_name,
    const std::string &class_name,
    const std::string &function_name,
    const std::string &function_hash) {
  rpc::FunctionDescriptor descriptor;
  auto typed_descriptor = descriptor.mutable_python_function_descriptor();
  typed_descriptor->set_module_name(module_name);
  typed_descriptor->set_class_name(class_name);
  typed_descriptor->set_function_name(function_name);
  typed_descriptor->set_function_hash(function_hash);
  return ray::FunctionDescriptor(new PythonFunctionDescriptor(std::move(descriptor)));
}

FunctionDescriptor FunctionDescriptorBuilder::BuildCpp(const std::string &function_name,
                                                       const std::string &caller,
                                                       const std::string &class_name) {
  rpc::FunctionDescriptor descriptor;
  auto typed_descriptor = descriptor.mutable_cpp_function_descriptor();
  typed_descriptor->set_function_name(function_name);
  typed_descriptor->set_caller(caller);
  typed_descriptor->set_class_name(class_name);
  return ray::FunctionDescriptor(new CppFunctionDescriptor(std::move(descriptor)));
}

FunctionDescriptor FunctionDescriptorBuilder::FromProto(rpc::FunctionDescriptor message) {
  switch (message.function_descriptor_case()) {
  case ray::FunctionDescriptorType::kJavaFunctionDescriptor:
    return ray::FunctionDescriptor(new ray::JavaFunctionDescriptor(std::move(message)));
  case ray::FunctionDescriptorType::kPythonFunctionDescriptor:
    return ray::FunctionDescriptor(new ray::PythonFunctionDescriptor(std::move(message)));
  case ray::FunctionDescriptorType::kCppFunctionDescriptor:
    return ray::FunctionDescriptor(new ray::CppFunctionDescriptor(std::move(message)));
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
  } else if (language == rpc::Language::CPP) {
    RAY_CHECK(function_descriptor_list.size() == 3);
    return FunctionDescriptorBuilder::BuildCpp(
        function_descriptor_list[0],   // function name
        function_descriptor_list[1],   // caller
        function_descriptor_list[2]);  // class name
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
}  // namespace ray
