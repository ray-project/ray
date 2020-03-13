#include <dlfcn.h>
#include <stdint.h>

namespace ray {
namespace api {

/* tmp impl, mast define in cc source file of dylib */
uintptr_t dylib_base_addr;

extern "C" void RayAgentInit() {
  Dl_info info;
  dladdr((void *)RayAgentInit, &info);
  dylib_base_addr = (uintptr_t)info.dli_fbase;
  return;
}
}  // namespace api
}  // namespace ray