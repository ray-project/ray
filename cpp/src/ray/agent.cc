#include <dlfcn.h>
#include <stdint.h>

namespace ray {

/* tmp impl, mast define in cc source file of dylib */
uintptr_t dylib_base_addr;

extern "C" void Ray_agent_init() {
  Dl_info info;
  dladdr((void *)Ray_agent_init, &info);
  dylib_base_addr = (uintptr_t)info.dli_fbase;
  return;
}
}  // namespace ray