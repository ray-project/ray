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

/* Real libc function pointers, resolved once at library load time.
   We use __attribute__((constructor)) instead of pthread_once because
   dlsym(RTLD_NEXT, "getenv") can recursively call getenv (e.g., with
   LD_AUDIT), which would deadlock pthread_once. The constructor runs
   before main() and before threads exist, so no synchronization is
   needed. For the rare case where getenv is called before the
   constructor (during very early dynamic linker init), we fall back
   to direct environ iteration — safe because threads don't exist
   yet at that point. */
static volatile int init_done;
static char *(*real_getenv)(const char *);
static int (*real_setenv)(const char *, const char *, int);
static int (*real_unsetenv)(const char *);
static int (*real_putenv)(char *);

/* Thread-local ring buffer for getenv return values.
   We must copy the result before releasing the read lock, because a
   concurrent setenv could invalidate the pointer. A ring of 8 slots
   ensures consecutive getenv calls (e.g., getenv("HOME") then
   getenv("PATH")) don't clobber each other, matching glibc behavior
   where returned pointers remain valid until the next setenv. */
#define NUM_TLS_SLOTS 8
#define TLS_SLOT_SIZE 8192
static __thread char tls_ring[NUM_TLS_SLOTS][TLS_SLOT_SIZE];
static __thread unsigned int tls_ring_idx;

__attribute__((constructor))
static void init_real_funcs(void) {
    real_getenv =
        (char *(*)(const char *))dlsym(RTLD_NEXT, "getenv");
    real_setenv =
        (int (*)(const char *, const char *, int))
            dlsym(RTLD_NEXT, "setenv");
    real_unsetenv =
        (int (*)(const char *))dlsym(RTLD_NEXT, "unsetenv");
    real_putenv =
        (int (*)(char *))dlsym(RTLD_NEXT, "putenv");
    /* Store-release so any thread that sees init_done==1 also
       sees the function pointers. */
    __atomic_store_n(&init_done, 1, __ATOMIC_RELEASE);
}

/* Fallback for calls before constructor runs (pre-thread,
   during early dynamic linker init). No locking needed. */
static char *getenv_fallback(const char *name) {
    extern char **environ;
    if (!environ) {
        return NULL;
    }
    size_t len = strlen(name);
    for (char **ep = environ; *ep; ep++) {
        if (strncmp(*ep, name, len) == 0 && (*ep)[len] == '=') {
            return *ep + len + 1;
        }
    }
    return NULL;
}

char *getenv(const char *name) {
    if (!__atomic_load_n(&init_done, __ATOMIC_ACQUIRE)) {
        return getenv_fallback(name);
    }

    pthread_rwlock_rdlock(&env_rwlock);
    char *val = real_getenv(name);

    if (val) {
        size_t vlen = strlen(val);
        if (vlen < TLS_SLOT_SIZE) {
            char *slot =
                tls_ring[tls_ring_idx++ % NUM_TLS_SLOTS];
            memcpy(slot, val, vlen + 1);
            val = slot;
        }
        /* Value too large for TLS slot — return the original
           pointer. Values > 8KB are extremely rare. */
    }

    pthread_rwlock_unlock(&env_rwlock);
    return val;
}

int setenv(const char *name, const char *value, int overwrite) {
    if (!__atomic_load_n(&init_done, __ATOMIC_ACQUIRE)) {
        init_real_funcs();
    }
    pthread_rwlock_wrlock(&env_rwlock);
    int ret = real_setenv(name, value, overwrite);
    pthread_rwlock_unlock(&env_rwlock);
    return ret;
}

int unsetenv(const char *name) {
    if (!__atomic_load_n(&init_done, __ATOMIC_ACQUIRE)) {
        init_real_funcs();
    }
    pthread_rwlock_wrlock(&env_rwlock);
    int ret = real_unsetenv(name);
    pthread_rwlock_unlock(&env_rwlock);
    return ret;
}

int putenv(char *string) {
    if (!__atomic_load_n(&init_done, __ATOMIC_ACQUIRE)) {
        init_real_funcs();
    }
    pthread_rwlock_wrlock(&env_rwlock);
    int ret = real_putenv(string);
    pthread_rwlock_unlock(&env_rwlock);
    return ret;
}
