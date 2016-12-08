#ifndef UNISTD_H
#define UNISTD_H

extern char *optarg;
extern int optind, opterr, optopt;
int getopt(int nargc, char *const nargv[], const char *ostr);

#include "../../src/Win32_Interop/Win32_FDAPI.h"
#include "../../src/Win32_Interop/Win32_APIs.h"
#define close(FD) FDAPI_close(FD)

#endif /* UNISTD_H */
