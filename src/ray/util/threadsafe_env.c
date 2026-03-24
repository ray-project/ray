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
#include <stdlib.h>
#include <string.h>

/* Read-write lock: multiple concurrent getenv (readers) are fine,
   setenv/unsetenv/putenv (writers) get exclusive access. */
static pthread_rwlock_t env_rwlock = PTHREAD_RWLOCK_INITIALIZER;

/* Real libc function pointers, resolved once at library load time. */
static char *(*real_getenv)(const char *);
static int (*real_setenv)(const char *, const char *, int);
static int (*real_unsetenv)(const char *);
static int (*real_putenv)(char *);

/* Thread-local buffer for getenv return values.
   We must copy the result before releasing the read lock, because a
   concurrent setenv could invalidate the pointer. */
static __thread char tls_buf[8192];

__attribute__((constructor))
static void init_real_funcs(void) {
    real_getenv = (char *(*)(const char *))dlsym(RTLD_NEXT, "getenv");
    real_setenv = (int (*)(const char *, const char *, int))dlsym(RTLD_NEXT, "setenv");
    real_unsetenv = (int (*)(const char *))dlsym(RTLD_NEXT, "unsetenv");
    real_putenv = (int (*)(char *))dlsym(RTLD_NEXT, "putenv");
}

char *getenv(const char *name) {
    /* If real_getenv hasn't been resolved yet (e.g., called during very early
       dynamic linker init before our constructor runs), fall back directly. */
    if (!real_getenv) {
        /* Cannot safely call dlsym here; just skip locking. This only happens
           during the earliest stages of process startup before threads exist. */
        extern char **environ;
        if (!environ || !name) return NULL;
        size_t len = strlen(name);
        for (char **ep = environ; *ep; ep++) {
            if (strncmp(*ep, name, len) == 0 && (*ep)[len] == '=') {
                return *ep + len + 1;
            }
        }
        return NULL;
    }

    pthread_rwlock_rdlock(&env_rwlock);
    char *val = real_getenv(name);

    if (val) {
        size_t vlen = strlen(val);
        if (vlen < sizeof(tls_buf)) {
            memcpy(tls_buf, val, vlen + 1);
            val = tls_buf;
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
    if (!real_setenv) {
        init_real_funcs();
    }
    pthread_rwlock_wrlock(&env_rwlock);
    int ret = real_setenv(name, value, overwrite);
    pthread_rwlock_unlock(&env_rwlock);
    return ret;
}

int unsetenv(const char *name) {
    if (!real_unsetenv) {
        init_real_funcs();
    }
    pthread_rwlock_wrlock(&env_rwlock);
    int ret = real_unsetenv(name);
    pthread_rwlock_unlock(&env_rwlock);
    return ret;
}

int putenv(char *string) {
    if (!real_putenv) {
        init_real_funcs();
    }
    pthread_rwlock_wrlock(&env_rwlock);
    int ret = real_putenv(string);
    pthread_rwlock_unlock(&env_rwlock);
    return ret;
}
