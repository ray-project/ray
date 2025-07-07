#ifndef HEADER_DARWIN_SET_PROCESS_NAME_H_INCLUDED
#define HEADER_DARWIN_SET_PROCESS_NAME_H_INCLUDED

#include "spt_config.h"

#include <stdbool.h>

HIDDEN bool darwin_set_process_title(const char * title);

#endif
