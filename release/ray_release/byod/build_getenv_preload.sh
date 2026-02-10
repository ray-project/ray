#!/bin/bash
# Builds getenv_trace_preload.so and installs to container home for use with
# LD_PRELOAD in runtime_env (to trace getenv calls from all libraries).
# Use with: cluster.byod.post_build_script: build_getenv_preload.sh
#           cluster.byod.runtime_env: [ "LD_PRELOAD=/home/ray/getenv_trace_preload.so" ]
# For tests that also use jemalloc: use a single LD_PRELOAD with both paths
# separated by colon, e.g. LD_PRELOAD=/home/ray/getenv_trace_preload.so:/usr/lib/x86_64-linux-gnu/libjemalloc.so

set -euo pipefail

INSTALL_DIR="${HOME:-/home/ray}"
cd "$INSTALL_DIR"

# Embed getenv_preload.c (from src/ray/util/getenv_preload.c)
cat > getenv_preload.c << 'GETENV_EOF'
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <execinfo.h>

#define MAX_FRAMES 64
#define SKIP_FRAMES 1

static void log_line(const char *line) {
  fputs(line, stderr);
  fflush(stderr);
  fputs(line, stdout);
  fflush(stdout);
}

typedef char *(*getenv_fn)(const char *);

static getenv_fn resolve_real_getenv(void) {
#ifdef __APPLE__
  void *libc = dlopen("/usr/lib/system/libsystem_c.dylib", RTLD_NOW);
  if (libc != NULL) {
    getenv_fn fn = (getenv_fn)dlsym(libc, "getenv");
    if (fn != NULL) return fn;
  }
  return NULL;
#else
  return (getenv_fn)dlsym(RTLD_NEXT, "getenv");
#endif
}

static char *getenv_real(const char *name) {
  static getenv_fn real_getenv = NULL;
  if (real_getenv == NULL) {
    real_getenv = resolve_real_getenv();
    if (real_getenv == NULL) return NULL;
  }
  return real_getenv(name);
}

char *getenv(const char *name) {
  void *buf[MAX_FRAMES];
  int n = backtrace(buf, MAX_FRAMES);
  char **syms = backtrace_symbols(buf, n);

  if (syms != NULL) {
    char line[4096];
    snprintf(line, sizeof(line), "[getenv_preload] getenv(%s) called from:\n",
             name ? name : "(null)");
    log_line(line);
    for (int i = SKIP_FRAMES; i < n && i < MAX_FRAMES; i++) {
      snprintf(line, sizeof(line), "  #%d %s\n", i - SKIP_FRAMES, syms[i]);
      log_line(line);
    }
    log_line("\n");
    free(syms);
  }

  return getenv_real(name);
}
GETENV_EOF

# Linux only (release BYOD images are Linux)
gcc -shared -fPIC -o getenv_trace_preload.so getenv_preload.c -ldl -Wall
rm -f getenv_preload.c
echo "Installed $INSTALL_DIR/getenv_trace_preload.so"
