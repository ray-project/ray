#include "ray/util/setproctitle.h"

extern "C" {
#include "ray/thirdparty/setproctitle/spt_setup.h"
#include "ray/thirdparty/setproctitle/spt_status.h"
}

namespace ray {
void setproctitle(const std::string &title) {
  spt_setup();
  set_ps_display(title.c_str(), true);
}
}  // namespace ray
