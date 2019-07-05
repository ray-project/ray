/* Copyright (c) 2014, Google Inc.
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION
 * OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE. */

#if !defined(_GNU_SOURCE)
#define _GNU_SOURCE  // needed for syscall() on Linux.
#endif

#include <openssl/rand.h>

#if !defined(OPENSSL_WINDOWS) && !defined(OPENSSL_FUCHSIA) && \
    !defined(BORINGSSL_UNSAFE_DETERMINISTIC_MODE) && !defined(OPENSSL_TRUSTY)

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#if defined(OPENSSL_LINUX)
#if defined(BORINGSSL_FIPS)
#include <linux/random.h>
#include <sys/ioctl.h>
#endif
#include <sys/syscall.h>
#endif

#include <openssl/thread.h>
#include <openssl/mem.h>

#include "internal.h"
#include "../delocate.h"
#include "../../internal.h"


#if defined(OPENSSL_LINUX)

#if defined(OPENSSL_X86_64)
#define EXPECTED_NR_getrandom 318
#elif defined(OPENSSL_X86)
#define EXPECTED_NR_getrandom 355
#elif defined(OPENSSL_AARCH64)
#define EXPECTED_NR_getrandom 278
#elif defined(OPENSSL_ARM)
#define EXPECTED_NR_getrandom 384
#elif defined(OPENSSL_PPC64LE)
#define EXPECTED_NR_getrandom 359
#endif

#if defined(EXPECTED_NR_getrandom)
#define USE_NR_getrandom

#if defined(__NR_getrandom)

#if __NR_getrandom != EXPECTED_NR_getrandom
#error "system call number for getrandom is not the expected value"
#endif

#else  // __NR_getrandom

#define __NR_getrandom EXPECTED_NR_getrandom

#endif  // __NR_getrandom

#endif  // EXPECTED_NR_getrandom

#if !defined(GRND_NONBLOCK)
#define GRND_NONBLOCK 1
#endif

#endif  // OPENSSL_LINUX

// rand_lock is used to protect the |*_requested| variables.
DEFINE_STATIC_MUTEX(rand_lock);

// The following constants are magic values of |urandom_fd|.
static const int kUnset = 0;
static const int kHaveGetrandom = -3;

// urandom_fd_requested is set by |RAND_set_urandom_fd|. It's protected by
// |rand_lock|.
DEFINE_BSS_GET(int, urandom_fd_requested);

// urandom_fd is a file descriptor to /dev/urandom. It's protected by |once|.
DEFINE_BSS_GET(int, urandom_fd);

DEFINE_STATIC_ONCE(rand_once);

// init_once initializes the state of this module to values previously
// requested. This is the only function that modifies |urandom_fd| and
// |urandom_buffering|, whose values may be read safely after calling the
// once.
static void init_once(void) {
  CRYPTO_STATIC_MUTEX_lock_read(rand_lock_bss_get());
  int fd = *urandom_fd_requested_bss_get();
  CRYPTO_STATIC_MUTEX_unlock_read(rand_lock_bss_get());

#if defined(USE_NR_getrandom)
  uint8_t dummy;
  long getrandom_ret =
      syscall(__NR_getrandom, &dummy, sizeof(dummy), GRND_NONBLOCK);

  if (getrandom_ret == 1) {
    *urandom_fd_bss_get() = kHaveGetrandom;
    return;
  } else if (getrandom_ret == -1 && errno == EAGAIN) {
    fprintf(
        stderr,
        "getrandom indicates that the entropy pool has not been initialized. "
        "Rather than continue with poor entropy, this process will block until "
        "entropy is available.\n");

    do {
      getrandom_ret =
          syscall(__NR_getrandom, &dummy, sizeof(dummy), 0 /* no flags */);
    } while (getrandom_ret == -1 && errno == EINTR);

    if (getrandom_ret == 1) {
      *urandom_fd_bss_get() = kHaveGetrandom;
      return;
    }
  }
#endif  // USE_NR_getrandom

  if (fd == kUnset) {
    do {
      fd = open("/dev/urandom", O_RDONLY);
    } while (fd == -1 && errno == EINTR);
  }

  if (fd < 0) {
    perror("failed to open /dev/urandom");
    abort();
  }

  assert(kUnset == 0);
  if (fd == kUnset) {
    // Because we want to keep |urandom_fd| in the BSS, we have to initialise
    // it to zero. But zero is a valid file descriptor too. Thus if open
    // returns zero for /dev/urandom, we dup it to get a non-zero number.
    fd = dup(fd);
    close(kUnset);

    if (fd <= 0) {
      perror("failed to dup /dev/urandom fd");
      abort();
    }
  }

#if defined(BORINGSSL_FIPS)
  // In FIPS mode we ensure that the kernel has sufficient entropy before
  // continuing. This is automatically handled by getrandom, which requires
  // that the entropy pool has been initialised, but for urandom we have to
  // poll.
  for (;;) {
    int entropy_bits;
    if (ioctl(fd, RNDGETENTCNT, &entropy_bits)) {
      fprintf(stderr,
              "RNDGETENTCNT on /dev/urandom failed. We cannot continue in this "
              "case when in FIPS mode.\n");
      abort();
    }

    static const int kBitsNeeded = 256;
    if (entropy_bits >= kBitsNeeded) {
      break;
    }

    usleep(250000);
  }
#endif

  int flags = fcntl(fd, F_GETFD);
  if (flags == -1) {
    // Native Client doesn't implement |fcntl|.
    if (errno != ENOSYS) {
      perror("failed to get flags from urandom fd");
      abort();
    }
  } else {
    flags |= FD_CLOEXEC;
    if (fcntl(fd, F_SETFD, flags) == -1) {
      perror("failed to set FD_CLOEXEC on urandom fd");
      abort();
    }
  }
  *urandom_fd_bss_get() = fd;
}

void RAND_set_urandom_fd(int fd) {
  fd = dup(fd);
  if (fd < 0) {
    perror("failed to dup supplied urandom fd");
    abort();
  }

  assert(kUnset == 0);
  if (fd == kUnset) {
    // Because we want to keep |urandom_fd| in the BSS, we have to initialise
    // it to zero. But zero is a valid file descriptor too. Thus if dup
    // returned zero we dup it again to get a non-zero number.
    fd = dup(fd);
    close(kUnset);

    if (fd <= 0) {
      perror("failed to dup supplied urandom fd");
      abort();
    }
  }

  CRYPTO_STATIC_MUTEX_lock_write(rand_lock_bss_get());
  *urandom_fd_requested_bss_get() = fd;
  CRYPTO_STATIC_MUTEX_unlock_write(rand_lock_bss_get());

  CRYPTO_once(rand_once_bss_get(), init_once);
  if (*urandom_fd_bss_get() == kHaveGetrandom) {
    close(fd);
  } else if (*urandom_fd_bss_get() != fd) {
    fprintf(stderr, "RAND_set_urandom_fd called after initialisation.\n");
    abort();
  }
}

#if defined(USE_NR_getrandom) && defined(OPENSSL_MSAN)
void __msan_unpoison(void *, size_t);
#endif

// fill_with_entropy writes |len| bytes of entropy into |out|. It returns one
// on success and zero on error.
static char fill_with_entropy(uint8_t *out, size_t len) {
  while (len > 0) {
    ssize_t r;

    if (*urandom_fd_bss_get() == kHaveGetrandom) {
#if defined(USE_NR_getrandom)
      do {
        r = syscall(__NR_getrandom, out, len, 0 /* no flags */);
      } while (r == -1 && errno == EINTR);

#if defined(OPENSSL_MSAN)
      if (r > 0) {
        // MSAN doesn't recognise |syscall| and thus doesn't notice that we
        // have initialised the output buffer.
        __msan_unpoison(out, r);
      }
#endif  // OPENSSL_MSAN

#else  // USE_NR_getrandom
      fprintf(stderr, "urandom fd corrupt.\n");
      abort();
#endif
    } else {
      do {
        r = read(*urandom_fd_bss_get(), out, len);
      } while (r == -1 && errno == EINTR);
    }

    if (r <= 0) {
      return 0;
    }
    out += r;
    len -= r;
  }

  return 1;
}

// CRYPTO_sysrand puts |requested| random bytes into |out|.
void CRYPTO_sysrand(uint8_t *out, size_t requested) {
  if (requested == 0) {
    return;
  }

  CRYPTO_once(rand_once_bss_get(), init_once);

  if (!fill_with_entropy(out, requested)) {
    perror("entropy fill failed");
    abort();
  }

#if defined(BORINGSSL_FIPS_BREAK_CRNG)
  // This breaks the "continuous random number generator test" defined in FIPS
  // 140-2, section 4.9.2, and implemented in rand_get_seed().
  OPENSSL_memset(out, 0, requested);
#endif
}

#endif /* !OPENSSL_WINDOWS && !defined(OPENSSL_FUCHSIA) && \
          !BORINGSSL_UNSAFE_DETERMINISTIC_MODE && !OPENSSL_TRUSTY */
