#!/bin/bash
# Builds getenv_trace_preload.so and installs to container home for use with
# LD_PRELOAD in runtime_env (to trace getenv calls from all libraries).
# Use with: cluster.byod.post_build_script: build_getenv_preload.sh
#           cluster.byod.runtime_env: [ "LD_PRELOAD=/home/ray/getenv_trace_preload.so" ]
# Each getenv is logged as: caller=0x... lib=... symbol=... name_ptr=0x... then name=...
# so the last line(s) before a crash identify the caller (no addr2line needed).
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

static char *(*real_getenv_ptr)(const char *) = NULL;

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
  if (real_getenv_ptr == NULL) return NULL;
  return real_getenv_ptr(name);
}

__attribute__((constructor))
static void getenv_preload_init(void) {
  real_getenv_ptr = resolve_real_getenv();
}

char *getenv(const char *name) {
  /* Log caller/lib/symbol first (no dereference of name). Flush. Then log name
   * on a separate line so if name is a bad pointer and causes SIGSEGV, we
   * already have the caller info and name_ptr. */
  void *caller = __builtin_return_address(0);
  char line[1024];
  Dl_info info;
  if (dladdr(caller, &info) != 0 && (info.dli_sname != NULL || info.dli_fname != NULL)) {
    const char *lib = info.dli_fname ? info.dli_fname : "?";
    const char *sym = info.dli_sname ? info.dli_sname : "?";
    unsigned long offset = info.dli_saddr
        ? (unsigned long)caller - (unsigned long)info.dli_saddr
        : 0;
    snprintf(line, sizeof(line),
             "[getenv_preload] caller=%p lib=%s symbol=%s+0x%lx name_ptr=%p\n",
             caller, lib, sym, offset, (void *)name);
  } else {
    snprintf(line, sizeof(line), "[getenv_preload] caller=%p name_ptr=%p\n",
             caller, (void *)name);
  }
  log_line(line);
  /* Separate line for name (may fault if name is invalid). */
  snprintf(line, sizeof(line), "[getenv_preload] name=%s\n", name ? name : "(null)");
  log_line(line);

  return getenv_real(name);
}
GETENV_EOF

# Linux only (release BYOD images are Linux)
gcc -shared -fPIC -o getenv_trace_preload.so getenv_preload.c -ldl -Wall
rm -f getenv_preload.c
echo "Installed $INSTALL_DIR/getenv_trace_preload.so"
