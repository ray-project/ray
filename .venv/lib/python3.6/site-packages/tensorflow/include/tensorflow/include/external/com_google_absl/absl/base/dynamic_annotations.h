/*
 *  Copyright 2017 The Abseil Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/* This file defines dynamic annotations for use with dynamic analysis
   tool such as valgrind, PIN, etc.

   Dynamic annotation is a source code annotation that affects
   the generated code (that is, the annotation is not a comment).
   Each such annotation is attached to a particular
   instruction and/or to a particular object (address) in the program.

   The annotations that should be used by users are macros in all upper-case
   (e.g., ANNOTATE_THREAD_NAME).

   Actual implementation of these macros may differ depending on the
   dynamic analysis tool being used.

   This file supports the following configurations:
   - Dynamic Annotations enabled (with static thread-safety warnings disabled).
     In this case, macros expand to functions implemented by Thread Sanitizer,
     when building with TSan. When not provided an external implementation,
     dynamic_annotations.cc provides no-op implementations.

   - Static Clang thread-safety warnings enabled.
     When building with a Clang compiler that supports thread-safety warnings,
     a subset of annotations can be statically-checked at compile-time. We
     expand these macros to static-inline functions that can be analyzed for
     thread-safety, but afterwards elided when building the final binary.

   - All annotations are disabled.
     If neither Dynamic Annotations nor Clang thread-safety warnings are
     enabled, then all annotation-macros expand to empty. */

#ifndef ABSL_BASE_DYNAMIC_ANNOTATIONS_H_
#define ABSL_BASE_DYNAMIC_ANNOTATIONS_H_

#ifndef DYNAMIC_ANNOTATIONS_ENABLED
# define DYNAMIC_ANNOTATIONS_ENABLED 0
#endif

#if DYNAMIC_ANNOTATIONS_ENABLED != 0

  /* -------------------------------------------------------------
     Annotations that suppress errors.  It is usually better to express the
     program's synchronization using the other annotations, but these can
     be used when all else fails. */

  /* Report that we may have a benign race at "pointer", with size
     "sizeof(*(pointer))". "pointer" must be a non-void* pointer.  Insert at the
     point where "pointer" has been allocated, preferably close to the point
     where the race happens.  See also ANNOTATE_BENIGN_RACE_STATIC. */
  #define ANNOTATE_BENIGN_RACE(pointer, description) \
    AnnotateBenignRaceSized(__FILE__, __LINE__, pointer, \
                            sizeof(*(pointer)), description)

  /* Same as ANNOTATE_BENIGN_RACE(address, description), but applies to
     the memory range [address, address+size). */
  #define ANNOTATE_BENIGN_RACE_SIZED(address, size, description) \
    AnnotateBenignRaceSized(__FILE__, __LINE__, address, size, description)

  /* Enable (enable!=0) or disable (enable==0) race detection for all threads.
     This annotation could be useful if you want to skip expensive race analysis
     during some period of program execution, e.g. during initialization. */
  #define ANNOTATE_ENABLE_RACE_DETECTION(enable) \
    AnnotateEnableRaceDetection(__FILE__, __LINE__, enable)

  /* -------------------------------------------------------------
     Annotations useful for debugging. */

  /* Report the current thread name to a race detector. */
  #define ANNOTATE_THREAD_NAME(name) \
    AnnotateThreadName(__FILE__, __LINE__, name)

  /* -------------------------------------------------------------
     Annotations useful when implementing locks.  They are not
     normally needed by modules that merely use locks.
     The "lock" argument is a pointer to the lock object. */

  /* Report that a lock has been created at address "lock". */
  #define ANNOTATE_RWLOCK_CREATE(lock) \
    AnnotateRWLockCreate(__FILE__, __LINE__, lock)

  /* Report that a linker initialized lock has been created at address "lock".
   */
#ifdef THREAD_SANITIZER
  #define ANNOTATE_RWLOCK_CREATE_STATIC(lock) \
    AnnotateRWLockCreateStatic(__FILE__, __LINE__, lock)
#else
  #define ANNOTATE_RWLOCK_CREATE_STATIC(lock) ANNOTATE_RWLOCK_CREATE(lock)
#endif

  /* Report that the lock at address "lock" is about to be destroyed. */
  #define ANNOTATE_RWLOCK_DESTROY(lock) \
    AnnotateRWLockDestroy(__FILE__, __LINE__, lock)

  /* Report that the lock at address "lock" has been acquired.
     is_w=1 for writer lock, is_w=0 for reader lock. */
  #define ANNOTATE_RWLOCK_ACQUIRED(lock, is_w) \
    AnnotateRWLockAcquired(__FILE__, __LINE__, lock, is_w)

  /* Report that the lock at address "lock" is about to be released. */
  #define ANNOTATE_RWLOCK_RELEASED(lock, is_w) \
    AnnotateRWLockReleased(__FILE__, __LINE__, lock, is_w)

#else  /* DYNAMIC_ANNOTATIONS_ENABLED == 0 */

  #define ANNOTATE_RWLOCK_CREATE(lock) /* empty */
  #define ANNOTATE_RWLOCK_CREATE_STATIC(lock) /* empty */
  #define ANNOTATE_RWLOCK_DESTROY(lock) /* empty */
  #define ANNOTATE_RWLOCK_ACQUIRED(lock, is_w) /* empty */
  #define ANNOTATE_RWLOCK_RELEASED(lock, is_w) /* empty */
  #define ANNOTATE_BENIGN_RACE(address, description) /* empty */
  #define ANNOTATE_BENIGN_RACE_SIZED(address, size, description) /* empty */
  #define ANNOTATE_THREAD_NAME(name) /* empty */
  #define ANNOTATE_ENABLE_RACE_DETECTION(enable) /* empty */

#endif  /* DYNAMIC_ANNOTATIONS_ENABLED */

/* These annotations are also made available to LLVM's Memory Sanitizer */
#if DYNAMIC_ANNOTATIONS_ENABLED == 1 || defined(MEMORY_SANITIZER)
  #define ANNOTATE_MEMORY_IS_INITIALIZED(address, size) \
    AnnotateMemoryIsInitialized(__FILE__, __LINE__, address, size)

  #define ANNOTATE_MEMORY_IS_UNINITIALIZED(address, size) \
    AnnotateMemoryIsUninitialized(__FILE__, __LINE__, address, size)
#else
  #define ANNOTATE_MEMORY_IS_INITIALIZED(address, size) /* empty */
  #define ANNOTATE_MEMORY_IS_UNINITIALIZED(address, size) /* empty */
#endif  /* DYNAMIC_ANNOTATIONS_ENABLED || MEMORY_SANITIZER */
/* TODO(delesley) -- Replace __CLANG_SUPPORT_DYN_ANNOTATION__ with the
   appropriate feature ID. */
#if defined(__clang__) && (!defined(SWIG)) \
    && defined(__CLANG_SUPPORT_DYN_ANNOTATION__)

  #if DYNAMIC_ANNOTATIONS_ENABLED == 0
    #define ANNOTALYSIS_ENABLED
  #endif

  /* When running in opt-mode, GCC will issue a warning, if these attributes are
     compiled. Only include them when compiling using Clang. */
  #define ATTRIBUTE_IGNORE_READS_BEGIN \
      __attribute((exclusive_lock_function("*")))
  #define ATTRIBUTE_IGNORE_READS_END \
      __attribute((unlock_function("*")))
#else
  #define ATTRIBUTE_IGNORE_READS_BEGIN  /* empty */
  #define ATTRIBUTE_IGNORE_READS_END  /* empty */
#endif  /* defined(__clang__) && ... */

#if (DYNAMIC_ANNOTATIONS_ENABLED != 0) || defined(ANNOTALYSIS_ENABLED)
  #define ANNOTATIONS_ENABLED
#endif

#if (DYNAMIC_ANNOTATIONS_ENABLED != 0)

  /* Request the analysis tool to ignore all reads in the current thread
     until ANNOTATE_IGNORE_READS_END is called.
     Useful to ignore intentional racey reads, while still checking
     other reads and all writes.
     See also ANNOTATE_UNPROTECTED_READ. */
  #define ANNOTATE_IGNORE_READS_BEGIN() \
    AnnotateIgnoreReadsBegin(__FILE__, __LINE__)

  /* Stop ignoring reads. */
  #define ANNOTATE_IGNORE_READS_END() \
    AnnotateIgnoreReadsEnd(__FILE__, __LINE__)

  /* Similar to ANNOTATE_IGNORE_READS_BEGIN, but ignore writes instead. */
  #define ANNOTATE_IGNORE_WRITES_BEGIN() \
    AnnotateIgnoreWritesBegin(__FILE__, __LINE__)

  /* Stop ignoring writes. */
  #define ANNOTATE_IGNORE_WRITES_END() \
    AnnotateIgnoreWritesEnd(__FILE__, __LINE__)

/* Clang provides limited support for static thread-safety analysis
   through a feature called Annotalysis. We configure macro-definitions
   according to whether Annotalysis support is available. */
#elif defined(ANNOTALYSIS_ENABLED)

  #define ANNOTATE_IGNORE_READS_BEGIN() \
    StaticAnnotateIgnoreReadsBegin(__FILE__, __LINE__)

  #define ANNOTATE_IGNORE_READS_END() \
    StaticAnnotateIgnoreReadsEnd(__FILE__, __LINE__)

  #define ANNOTATE_IGNORE_WRITES_BEGIN() \
    StaticAnnotateIgnoreWritesBegin(__FILE__, __LINE__)

  #define ANNOTATE_IGNORE_WRITES_END() \
    StaticAnnotateIgnoreWritesEnd(__FILE__, __LINE__)

#else
  #define ANNOTATE_IGNORE_READS_BEGIN()  /* empty */
  #define ANNOTATE_IGNORE_READS_END()  /* empty */
  #define ANNOTATE_IGNORE_WRITES_BEGIN()  /* empty */
  #define ANNOTATE_IGNORE_WRITES_END()  /* empty */
#endif

/* Implement the ANNOTATE_IGNORE_READS_AND_WRITES_* annotations using the more
   primitive annotations defined above. */
#if defined(ANNOTATIONS_ENABLED)

  /* Start ignoring all memory accesses (both reads and writes). */
  #define ANNOTATE_IGNORE_READS_AND_WRITES_BEGIN() \
    do {                                           \
      ANNOTATE_IGNORE_READS_BEGIN();               \
      ANNOTATE_IGNORE_WRITES_BEGIN();              \
    }while (0)

  /* Stop ignoring both reads and writes. */
  #define ANNOTATE_IGNORE_READS_AND_WRITES_END()   \
    do {                                           \
      ANNOTATE_IGNORE_WRITES_END();                \
      ANNOTATE_IGNORE_READS_END();                 \
    }while (0)

#else
  #define ANNOTATE_IGNORE_READS_AND_WRITES_BEGIN()  /* empty */
  #define ANNOTATE_IGNORE_READS_AND_WRITES_END()  /* empty */
#endif

/* Use the macros above rather than using these functions directly. */
#include <stddef.h>
#ifdef __cplusplus
extern "C" {
#endif
void AnnotateRWLockCreate(const char *file, int line,
                          const volatile void *lock);
void AnnotateRWLockCreateStatic(const char *file, int line,
                          const volatile void *lock);
void AnnotateRWLockDestroy(const char *file, int line,
                           const volatile void *lock);
void AnnotateRWLockAcquired(const char *file, int line,
                            const volatile void *lock, long is_w);  /* NOLINT */
void AnnotateRWLockReleased(const char *file, int line,
                            const volatile void *lock, long is_w);  /* NOLINT */
void AnnotateBenignRace(const char *file, int line,
                        const volatile void *address,
                        const char *description);
void AnnotateBenignRaceSized(const char *file, int line,
                        const volatile void *address,
                        size_t size,
                        const char *description);
void AnnotateThreadName(const char *file, int line,
                        const char *name);
void AnnotateEnableRaceDetection(const char *file, int line, int enable);
void AnnotateMemoryIsInitialized(const char *file, int line,
                                 const volatile void *mem, size_t size);
void AnnotateMemoryIsUninitialized(const char *file, int line,
                                   const volatile void *mem, size_t size);

/* Annotations expand to these functions, when Dynamic Annotations are enabled.
   These functions are either implemented as no-op calls, if no Sanitizer is
   attached, or provided with externally-linked implementations by a library
   like ThreadSanitizer. */
void AnnotateIgnoreReadsBegin(const char *file, int line)
    ATTRIBUTE_IGNORE_READS_BEGIN;
void AnnotateIgnoreReadsEnd(const char *file, int line)
    ATTRIBUTE_IGNORE_READS_END;
void AnnotateIgnoreWritesBegin(const char *file, int line);
void AnnotateIgnoreWritesEnd(const char *file, int line);

#if defined(ANNOTALYSIS_ENABLED)
/* When Annotalysis is enabled without Dynamic Annotations, the use of
   static-inline functions allows the annotations to be read at compile-time,
   while still letting the compiler elide the functions from the final build.

   TODO(delesley) -- The exclusive lock here ignores writes as well, but
   allows IGNORE_READS_AND_WRITES to work properly. */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
static inline void StaticAnnotateIgnoreReadsBegin(const char *file, int line)
    ATTRIBUTE_IGNORE_READS_BEGIN { (void)file; (void)line; }
static inline void StaticAnnotateIgnoreReadsEnd(const char *file, int line)
    ATTRIBUTE_IGNORE_READS_END { (void)file; (void)line; }
static inline void StaticAnnotateIgnoreWritesBegin(
    const char *file, int line) { (void)file; (void)line; }
static inline void StaticAnnotateIgnoreWritesEnd(
    const char *file, int line) { (void)file; (void)line; }
#pragma GCC diagnostic pop
#endif

/* Return non-zero value if running under valgrind.

  If "valgrind.h" is included into dynamic_annotations.cc,
  the regular valgrind mechanism will be used.
  See http://valgrind.org/docs/manual/manual-core-adv.html about
  RUNNING_ON_VALGRIND and other valgrind "client requests".
  The file "valgrind.h" may be obtained by doing
     svn co svn://svn.valgrind.org/valgrind/trunk/include

  If for some reason you can't use "valgrind.h" or want to fake valgrind,
  there are two ways to make this function return non-zero:
    - Use environment variable: export RUNNING_ON_VALGRIND=1
    - Make your tool intercept the function RunningOnValgrind() and
      change its return value.
 */
int RunningOnValgrind(void);

/* ValgrindSlowdown returns:
    * 1.0, if (RunningOnValgrind() == 0)
    * 50.0, if (RunningOnValgrind() != 0 && getenv("VALGRIND_SLOWDOWN") == NULL)
    * atof(getenv("VALGRIND_SLOWDOWN")) otherwise
   This function can be used to scale timeout values:
   EXAMPLE:
   for (;;) {
     DoExpensiveBackgroundTask();
     SleepForSeconds(5 * ValgrindSlowdown());
   }
 */
double ValgrindSlowdown(void);

#ifdef __cplusplus
}
#endif

/* ANNOTATE_UNPROTECTED_READ is the preferred way to annotate racey reads.

     Instead of doing
        ANNOTATE_IGNORE_READS_BEGIN();
        ... = x;
        ANNOTATE_IGNORE_READS_END();
     one can use
        ... = ANNOTATE_UNPROTECTED_READ(x); */
#if defined(__cplusplus) && defined(ANNOTATIONS_ENABLED)
template <typename T>
inline T ANNOTATE_UNPROTECTED_READ(const volatile T &x) { /* NOLINT */
  ANNOTATE_IGNORE_READS_BEGIN();
  T res = x;
  ANNOTATE_IGNORE_READS_END();
  return res;
  }
#else
  #define ANNOTATE_UNPROTECTED_READ(x) (x)
#endif

#if DYNAMIC_ANNOTATIONS_ENABLED != 0 && defined(__cplusplus)
  /* Apply ANNOTATE_BENIGN_RACE_SIZED to a static variable. */
  #define ANNOTATE_BENIGN_RACE_STATIC(static_var, description)        \
    namespace {                                                       \
      class static_var ## _annotator {                                \
       public:                                                        \
        static_var ## _annotator() {                                  \
          ANNOTATE_BENIGN_RACE_SIZED(&static_var,                     \
                                      sizeof(static_var),             \
            # static_var ": " description);                           \
        }                                                             \
      };                                                              \
      static static_var ## _annotator the ## static_var ## _annotator;\
    }  // namespace
#else /* DYNAMIC_ANNOTATIONS_ENABLED == 0 */
  #define ANNOTATE_BENIGN_RACE_STATIC(static_var, description)  /* empty */
#endif /* DYNAMIC_ANNOTATIONS_ENABLED */

#ifdef ADDRESS_SANITIZER
/* Describe the current state of a contiguous container such as e.g.
 * std::vector or std::string. For more details see
 * sanitizer/common_interface_defs.h, which is provided by the compiler. */
#include <sanitizer/common_interface_defs.h>
#define ANNOTATE_CONTIGUOUS_CONTAINER(beg, end, old_mid, new_mid) \
  __sanitizer_annotate_contiguous_container(beg, end, old_mid, new_mid)
#define ADDRESS_SANITIZER_REDZONE(name)         \
  struct { char x[8] __attribute__ ((aligned (8))); } name
#else
#define ANNOTATE_CONTIGUOUS_CONTAINER(beg, end, old_mid, new_mid)
#define ADDRESS_SANITIZER_REDZONE(name)
#endif  // ADDRESS_SANITIZER

/* Undefine the macros intended only in this file. */
#undef ANNOTALYSIS_ENABLED
#undef ANNOTATIONS_ENABLED
#undef ATTRIBUTE_IGNORE_READS_BEGIN
#undef ATTRIBUTE_IGNORE_READS_END

#endif  /* ABSL_BASE_DYNAMIC_ANNOTATIONS_H_ */
