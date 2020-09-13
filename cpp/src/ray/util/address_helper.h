#pragma once
#include <dlfcn.h>
#include <stdint.h>

namespace ray {
namespace api {

/// A base address which is used to calculate function offset
extern uintptr_t dynamic_library_base_addr;

/// A fixed C language function which help to get infomation from dladdr
extern "C" void GenerateBaseAddressOfCurrentLibrary();
}  // namespace api
}  // namespace ray