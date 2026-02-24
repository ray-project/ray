/*
 * Reproduction for SIGSEGV in getenv() due to glibc thread-safety bug.
 *
 * glibc's setenv() can reallocate the environ array while getenv() on another
 * thread is iterating it, causing a use-after-free / SIGSEGV.
 *
 * This is the root cause of the crash seen in Ray's OpenTelemetry metric
 * exporter thread, where gRPC calls grpc_core::GetEnv() -> getenv() on a
 * background thread while Python worker threads call os.environ[k]=v
 * (which calls setenv()).
 *
 * Build:  gcc -O2 -pthread -o repro_getenv_race repro_getenv_race.c
 * Run:    ./repro_getenv_race
 *
 * On vulnerable glibc (<2.40), this will typically SIGSEGV within seconds.
 * On patched glibc (>=2.40, e.g. Fedora 41, RHEL 10), it should run safely.
 */

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define NUM_READER_THREADS 4
#define NUM_ITERATIONS 10000000

/* Names used by setenv - use many unique names to force environ array growth */
static char setenv_names[512][32];

static void *setenv_thread(void *arg) {
    (void)arg;
    char value[64];
    for (int i = 0; i < NUM_ITERATIONS; i++) {
        /* Use rotating names to force environ array to grow and reallocate */
        int idx = i % 512;
        snprintf(value, sizeof(value), "value_%d", i);
        setenv(setenv_names[idx], value, 1);
    }
    return NULL;
}

static void *getenv_thread(void *arg) {
    int thread_id = *(int *)arg;
    (void)thread_id;
    for (int i = 0; i < NUM_ITERATIONS; i++) {
        /*
         * Read various env vars, mimicking what gRPC does internally.
         * grpc_core::GetEnv() calls getenv("GRPC_EXPERIMENTS"),
         * getenv("GRPC_SHUFFLE_PICK_FIRST"), etc.
         */
        volatile const char *val;
        val = getenv("PATH");
        (void)val;
        val = getenv("HOME");
        (void)val;
        val = getenv("GRPC_EXPERIMENTS");
        (void)val;
        val = getenv("NONEXISTENT_VAR_12345");
        (void)val;
    }
    return NULL;
}

int main(void) {
    /* Pre-generate setenv key names */
    for (int i = 0; i < 512; i++) {
        snprintf(setenv_names[i], sizeof(setenv_names[i]),
                 "REPRO_ENV_%03d", i);
    }

    printf("Reproducing getenv/setenv race condition (glibc thread-safety bug)\n");
    printf("  Writer threads: 1 (calling setenv in a loop)\n");
    printf("  Reader threads: %d (calling getenv in a loop)\n", NUM_READER_THREADS);
    printf("  Iterations: %d per thread\n", NUM_ITERATIONS);
    printf("\n");
    printf("On vulnerable glibc (<2.40), expect SIGSEGV within seconds.\n");
    printf("On patched glibc (>=2.40), this should complete without crashing.\n");
    printf("\nStarting...\n");
    fflush(stdout);

    pthread_t writer;
    pthread_t readers[NUM_READER_THREADS];
    int reader_ids[NUM_READER_THREADS];

    /* Start all threads */
    pthread_create(&writer, NULL, setenv_thread, NULL);
    for (int i = 0; i < NUM_READER_THREADS; i++) {
        reader_ids[i] = i;
        pthread_create(&readers[i], NULL, getenv_thread, &reader_ids[i]);
    }

    /* Wait for completion (if we get here without SIGSEGV, the glibc is safe) */
    pthread_join(writer, NULL);
    for (int i = 0; i < NUM_READER_THREADS; i++) {
        pthread_join(readers[i], NULL);
    }

    printf("\nCompleted without crash - your glibc appears to be safe.\n");
    return 0;
}
