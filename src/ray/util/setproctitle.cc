#include "ray/util/setproctitle.h"

// Define macros for spt_config.h
#if defined(__APPLE__)
#define __darwin__
#elif defined(__linux__)
#define HAVE_SYS_PRCTL_H
#endif

extern "C" {
    #include "ray/thirdparty/setproctitle/spt_setup.h"
    #include "ray/thirdparty/setproctitle/spt_status.h"
}

namespace ray {
void setproctitle(const std::string &title) {
    spt_setup();
    set_ps_display(title.c_str(), true);
}
} // namespace ray
