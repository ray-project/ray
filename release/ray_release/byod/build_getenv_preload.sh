#!/bin/bash
# Builds getenv_trace_preload.so and installs to container home for use with
# LD_PRELOAD in runtime_env (to trace getenv/setenv/putenv/unsetenv from all libraries).
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
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <execinfo.h>

#define MAX_FRAMES 64
#define SKIP_FRAMES 1
#define LOG_BUF_SIZE (64 * 1024)

static char log_buf[LOG_BUF_SIZE];
static size_t log_pos;

static void buf_append(const char *fmt, ...) {
  if (log_pos >= LOG_BUF_SIZE) return;
  va_list ap;
  va_start(ap, fmt);
  int n = vsnprintf(log_buf + log_pos, LOG_BUF_SIZE - log_pos, fmt, ap);
  va_end(ap);
  if (n > 0) log_pos += (size_t)n;
}

static void buf_flush(void) {
  if (log_pos > 0) {
    fputs(log_buf, stderr);
    fflush(stderr);
  }
  log_pos = 0;
}

typedef char *(*getenv_fn)(const char *);

static char *getenv_real(const char *name) {
  static getenv_fn real = NULL;
  if (real == NULL) real = (getenv_fn)dlsym(RTLD_NEXT, "getenv");
  return real ? real(name) : NULL;
}

static void log_backtrace_append() {
  void *buf[MAX_FRAMES];
  int n = backtrace(buf, MAX_FRAMES);
  char **syms = backtrace_symbols(buf, n);
  if (syms != NULL) {
    for (int i = SKIP_FRAMES; i < n && i < MAX_FRAMES; i++)
      buf_append("  #%d %s\n", i - SKIP_FRAMES, syms[i]);
    buf_append("\n");
    free(syms);
  }
}

char *getenv(const char *name) {
  log_pos = 0;
  buf_append("[getenv] name=%s\n", name ? name : "(null)");
  log_backtrace_append();
  buf_flush();
  return getenv_real(name);
}

GETENV_EOF

# Linux only (release BYOD images are Linux)
gcc -shared -fPIC -o getenv_trace_preload.so getenv_preload.c -ldl -lpthread -Wall
rm -f getenv_preload.c
echo "Installed $INSTALL_DIR/getenv_trace_preload.so"
