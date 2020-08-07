#pragma once
#include <dlfcn.h>
#include <stdint.h>

namespace ray {
namespace api {

/// A base address which is used to calculate function offset
extern uintptr_t dynamic_library_base_addr;

/// Get the base address of libary which the function address belongs to.
uintptr_t GetBaseAddressOfLibraryFromAddr(void *addr);
}  // namespace api
}  // namespace ray