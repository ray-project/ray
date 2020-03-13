#pragma once
#include <dlfcn.h>
#include <stdint.h>

namespace ray {
namespace api {

/* tmp impl, mast define in cc source file of dylib */
extern uintptr_t dylib_base_addr;

extern "C" void RayAgentInit();
}  // namespace api
}  // namespace ray