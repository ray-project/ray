#pragma once

namespace ray {

template <typename T>
class FunctionArgument {
 public:
  bool rayObjectFlag;
  T argument;

  FunctionArgument();
  FunctionArgument(bool flag, T &arg);
  FunctionArgument(bool flag, T &&arg);
};

template <typename T>
FunctionArgument<T>::FunctionArgument() {
  rayObjectFlag = false;
}

template <typename T>
FunctionArgument<T>::FunctionArgument(bool flag, T &arg) {
  rayObjectFlag = flag;
  argument = arg;
}

template <typename T>
FunctionArgument<T>::FunctionArgument(bool flag, T &&arg) {
  rayObjectFlag = flag;
  argument = std::move(arg);
}
}