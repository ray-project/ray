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
#include <pthread.h>

#define MAX_FRAMES 64
#define SKIP_FRAMES 1
#define LOG_BUF_SIZE (64 * 1024)

static char log_buf[LOG_BUF_SIZE];
static size_t log_pos;
static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;

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
typedef int (*setenv_fn)(const char *, const char *, int);
typedef int (*putenv_fn)(char *);
typedef int (*unsetenv_fn)(const char *);

static char *getenv_real(const char *name) {
  static getenv_fn real = NULL;
  if (real == NULL) real = (getenv_fn)dlsym(RTLD_NEXT, "getenv");
  return real ? real(name) : NULL;
}

static int setenv_real(const char *name, const char *value, int overwrite) {
  static setenv_fn real = NULL;
  if (real == NULL) real = (setenv_fn)dlsym(RTLD_NEXT, "setenv");
  return real ? real(name, value, overwrite) : -1;
}

static int putenv_real(char *string) {
  static putenv_fn real = NULL;
  if (real == NULL) real = (putenv_fn)dlsym(RTLD_NEXT, "putenv");
  return real ? real(string) : -1;
}

static int unsetenv_real(const char *name) {
  static unsetenv_fn real = NULL;
  if (real == NULL) real = (unsetenv_fn)dlsym(RTLD_NEXT, "unsetenv");
  return real ? real(name) : -1;
}

static void log_backtrace_append(const char *tag) {
  void *buf[MAX_FRAMES];
  int n = backtrace(buf, MAX_FRAMES);
  char **syms = backtrace_symbols(buf, n);
  if (syms != NULL) {
    buf_append("[getenv_preload] %s backtrace:\n", tag);
    for (int i = SKIP_FRAMES; i < n && i < MAX_FRAMES; i++)
      buf_append("  #%d %s\n", i - SKIP_FRAMES, syms[i]);
    buf_append("\n");
    free(syms);
  }
}

char *getenv(const char *name) {
  pthread_mutex_lock(&log_mutex);
  log_pos = 0;
  buf_append("[getenv_preload] getenv name=%s\n", name ? name : "(null)");
  log_backtrace_append("getenv");
  buf_flush();
  pthread_mutex_unlock(&log_mutex);
  return getenv_real(name);
}

int setenv(const char *name, const char *value, int overwrite) {
  pthread_mutex_lock(&log_mutex);
  log_pos = 0;
  buf_append("[getenv_preload] setenv name=%s value=%s overwrite=%d\n",
             name ? name : "(null)", value ? value : "(null)", overwrite);
  log_backtrace_append("setenv");
  buf_flush();
  pthread_mutex_unlock(&log_mutex);
  return setenv_real(name, value, overwrite);
}

int putenv(char *string) {
  pthread_mutex_lock(&log_mutex);
  log_pos = 0;
  buf_append("[getenv_preload] putenv string=%s\n", string ? string : "(null)");
  log_backtrace_append("putenv");
  buf_flush();
  pthread_mutex_unlock(&log_mutex);
  return putenv_real(string);
}

int unsetenv(const char *name) {
  pthread_mutex_lock(&log_mutex);
  log_pos = 0;
  buf_append("[getenv_preload] unsetenv name=%s\n", name ? name : "(null)");
  log_backtrace_append("unsetenv");
  buf_flush();
  pthread_mutex_unlock(&log_mutex);
  return unsetenv_real(name);
}
GETENV_EOF

# Linux only (release BYOD images are Linux)
gcc -shared -fPIC -o getenv_trace_preload.so getenv_preload.c -ldl -lpthread -Wall
rm -f getenv_preload.c
echo "Installed $INSTALL_DIR/getenv_trace_preload.so"
