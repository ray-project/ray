/*
 * Thread-safe getenv/setenv wrapper for Ray workers.
 *
 * glibc's getenv() is not thread-safe when called concurrently with setenv().
 * If setenv() reallocates the environ array while getenv() is iterating it,
 * getenv() can dereference freed memory → SIGSEGV.
 *
 * In Ray workers, gRPC and OpenTelemetry background threads call getenv()
 * (for proxy settings, DNS config, load-balancer policy, etc.) while user
 * code on the main thread calls setenv() via os.environ writes or C extension
 * imports (numpy, torch, etc.).
 *
 * This library wraps getenv/setenv/unsetenv/putenv with a pthread_rwlock so
 * concurrent reads (getenv) are allowed but writes (setenv) are exclusive.
 *
 * Usage: LD_PRELOAD=libray_threadsafe_env.so <command>
 *
 * Linux-only (glibc-specific issue; macOS libc handles this differently).
 */

#define _GNU_SOURCE
#include <dlfcn.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Read-write lock: multiple concurrent getenv (readers) are fine,
   setenv/unsetenv/putenv (writers) get exclusive access. */
static pthread_rwlock_t env_rwlock = PTHREAD_RWLOCK_INITIALIZER;

/* Real libc function pointers, resolved exactly once via pthread_once. */
static char *(*real_getenv)(const char *);
static int (*real_setenv)(const char *, const char *, int);
static int (*real_unsetenv)(const char *);
static int (*real_putenv)(char *);

static pthread_once_t real_funcs_init_once = PTHREAD_ONCE_INIT;

/* Thread-local ring buffer for getenv return values.
   We must copy the result before releasing the read lock, because a
   concurrent setenv could invalidate the pointer. A ring of 8 slots
   ensures consecutive getenv calls (e.g., getenv("HOME") then
   getenv("PATH")) don't clobber each other, matching glibc behavior
   where returned pointers remain valid until the next setenv. */
#define NUM_TLS_SLOTS 8
#define TLS_SLOT_SIZE 8192
static __thread char tls_ring[NUM_TLS_SLOTS][TLS_SLOT_SIZE];
static __thread int tls_ring_idx;

static void init_real_funcs(void) {
    real_getenv = (char *(*)(const char *))dlsym(RTLD_NEXT, "getenv");
    real_setenv = (int (*)(const char *, const char *, int))dlsym(RTLD_NEXT, "setenv");
    real_unsetenv = (int (*)(const char *))dlsym(RTLD_NEXT, "unsetenv");
    real_putenv = (int (*)(char *))dlsym(RTLD_NEXT, "putenv");
}

char *getenv(const char *name) {
    pthread_once(&real_funcs_init_once, init_real_funcs);
    if (!real_getenv) {
        fprintf(stderr,
                "libray_threadsafe_env: dlsym for getenv failed\n");
        abort();
    }

    pthread_rwlock_rdlock(&env_rwlock);
    char *val = real_getenv(name);

    if (val) {
        size_t vlen = strlen(val);
        if (vlen < TLS_SLOT_SIZE) {
            char *slot = tls_ring[tls_ring_idx++ % NUM_TLS_SLOTS];
            memcpy(slot, val, vlen + 1);
            val = slot;
        } else {
            /* Value too large for TLS buffer — return the original pointer.
               This is technically still racy, but values > 8KB are extremely
               rare and the window is very small. */
        }
    }

    pthread_rwlock_unlock(&env_rwlock);
    return val;
}

int setenv(const char *name, const char *value, int overwrite) {
    pthread_once(&real_funcs_init_once, init_real_funcs);
    if (!real_setenv) {
        fprintf(stderr,
                "libray_threadsafe_env: dlsym for setenv failed\n");
        abort();
    }
    pthread_rwlock_wrlock(&env_rwlock);
    int ret = real_setenv(name, value, overwrite);
    pthread_rwlock_unlock(&env_rwlock);
    return ret;
}

int unsetenv(const char *name) {
    pthread_once(&real_funcs_init_once, init_real_funcs);
    if (!real_unsetenv) {
        fprintf(stderr,
                "libray_threadsafe_env: dlsym for unsetenv failed\n");
        abort();
    }
    pthread_rwlock_wrlock(&env_rwlock);
    int ret = real_unsetenv(name);
    pthread_rwlock_unlock(&env_rwlock);
    return ret;
}

int putenv(char *string) {
    pthread_once(&real_funcs_init_once, init_real_funcs);
    if (!real_putenv) {
        fprintf(stderr,
                "libray_threadsafe_env: dlsym for putenv failed\n");
        abort();
    }
    pthread_rwlock_wrlock(&env_rwlock);
    int ret = real_putenv(string);
    pthread_rwlock_unlock(&env_rwlock);
    return ret;
}
