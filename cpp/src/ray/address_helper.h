#pragma once
#include <dlfcn.h>
#include <stdint.h>

namespace ray {
namespace api {

/// A base address which is used to calculate function offset
extern uintptr_t dylib_base_addr;

/// A fixed C language function which help to get infomation from dladdr
extern "C" void AddressHelperInit();
}  // namespace api
}  // namespace ray