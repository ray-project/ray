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

#include <openssl/rand.h>

#include <assert.h>
#include <limits.h>
#include <string.h>

#if defined(BORINGSSL_FIPS)
#include <unistd.h>
#endif

#include <openssl/chacha.h>
#include <openssl/cpu.h>
#include <openssl/mem.h>

#include "internal.h"
#include "../../internal.h"
#include "../delocate.h"


// It's assumed that the operating system always has an unfailing source of
// entropy which is accessed via |CRYPTO_sysrand|. (If the operating system
// entropy source fails, it's up to |CRYPTO_sysrand| to abort the process—we
// don't try to handle it.)
//
// In addition, the hardware may provide a low-latency RNG. Intel's rdrand
// instruction is the canonical example of this. When a hardware RNG is
// available we don't need to worry about an RNG failure arising from fork()ing
// the process or moving a VM, so we can keep thread-local RNG state and use it
// as an additional-data input to CTR-DRBG.
//
// (We assume that the OS entropy is safe from fork()ing and VM duplication.
// This might be a bit of a leap of faith, esp on Windows, but there's nothing
// that we can do about it.)

// kReseedInterval is the number of generate calls made to CTR-DRBG before
// reseeding.
static const unsigned kReseedInterval = 4096;

// CRNGT_BLOCK_SIZE is the number of bytes in a “block” for the purposes of the
// continuous random number generator test in FIPS 140-2, section 4.9.2.
#define CRNGT_BLOCK_SIZE 16

#if defined(OPENSSL_X86_64) && !defined(OPENSSL_NO_ASM) && \
    !defined(BORINGSSL_UNSAFE_DETERMINISTIC_MODE)

// These functions are defined in asm/rdrand-x86_64.pl
extern int CRYPTO_rdrand(uint8_t out[8]);
extern int CRYPTO_rdrand_multiple8_buf(uint8_t *buf, size_t len);

static int have_rdrand(void) {
  return (OPENSSL_ia32cap_get()[1] & (1u << 30)) != 0;
}

static int hwrand(uint8_t *buf, const size_t len) {
  if (!have_rdrand()) {
    return 0;
  }

  const size_t len_multiple8 = len & ~7;
  if (!CRYPTO_rdrand_multiple8_buf(buf, len_multiple8)) {
    return 0;
  }
  const size_t remainder = len - len_multiple8;

  if (remainder != 0) {
    assert(remainder < 8);

    uint8_t rand_buf[8];
    if (!CRYPTO_rdrand(rand_buf)) {
      return 0;
    }
    OPENSSL_memcpy(buf + len_multiple8, rand_buf, remainder);
  }

#if defined(BORINGSSL_FIPS_BREAK_CRNG)
  // This breaks the "continuous random number generator test" defined in FIPS
  // 140-2, section 4.9.2, and implemented in rand_get_seed().
  OPENSSL_memset(buf, 0, len);
#endif

  return 1;
}

#else

static int hwrand(uint8_t *buf, size_t len) {
  return 0;
}

#endif

// rand_state contains an RNG state.
struct rand_state {
  CTR_DRBG_STATE drbg;
  // next forms a NULL-terminated linked-list of all free |rand_state| objects.
  struct rand_state *next;
  // calls is the number of generate calls made on |drbg| since it was last
  // (re)seeded. This is bound by |kReseedInterval|.
  unsigned calls;

#if defined(BORINGSSL_FIPS)
  // next_all forms another NULL-terminated linked-list, this time of all
  // |rand_state| objects that have been allocated including those that might
  // currently be in use.
  struct rand_state *next_all;
  // last_block contains the previous block from |CRYPTO_sysrand|.
  uint8_t last_block[CRNGT_BLOCK_SIZE];
  // last_block_valid is non-zero iff |last_block| contains data from
  // |CRYPTO_sysrand|.
  int last_block_valid;
#endif
};

#if defined(BORINGSSL_FIPS)

static void rand_get_seed(struct rand_state *state,
                          uint8_t seed[CTR_DRBG_ENTROPY_LEN]) {
  if (!state->last_block_valid) {
    if (!hwrand(state->last_block, sizeof(state->last_block))) {
      CRYPTO_sysrand(state->last_block, sizeof(state->last_block));
    }
    state->last_block_valid = 1;
  }

  // We overread from /dev/urandom or RDRAND by a factor of 10 and XOR to
  // whiten.
#define FIPS_OVERREAD 10
  uint8_t entropy[CTR_DRBG_ENTROPY_LEN * FIPS_OVERREAD];

  if (!hwrand(entropy, sizeof(entropy))) {
    CRYPTO_sysrand(entropy, sizeof(entropy));
  }

  // See FIPS 140-2, section 4.9.2. This is the “continuous random number
  // generator test” which causes the program to randomly abort. Hopefully the
  // rate of failure is small enough not to be a problem in practice.
  if (CRYPTO_memcmp(state->last_block, entropy, CRNGT_BLOCK_SIZE) == 0) {
    fprintf(stderr, "CRNGT failed.\n");
    BORINGSSL_FIPS_abort();
  }

  for (size_t i = CRNGT_BLOCK_SIZE; i < sizeof(entropy);
       i += CRNGT_BLOCK_SIZE) {
    if (CRYPTO_memcmp(entropy + i - CRNGT_BLOCK_SIZE, entropy + i,
                      CRNGT_BLOCK_SIZE) == 0) {
      fprintf(stderr, "CRNGT failed.\n");
      BORINGSSL_FIPS_abort();
    }
  }
  OPENSSL_memcpy(state->last_block,
                 entropy + sizeof(entropy) - CRNGT_BLOCK_SIZE,
                 CRNGT_BLOCK_SIZE);

  OPENSSL_memcpy(seed, entropy, CTR_DRBG_ENTROPY_LEN);

  for (size_t i = 1; i < FIPS_OVERREAD; i++) {
    for (size_t j = 0; j < CTR_DRBG_ENTROPY_LEN; j++) {
      seed[j] ^= entropy[CTR_DRBG_ENTROPY_LEN * i + j];
    }
  }
}

#else

static void rand_get_seed(struct rand_state *state,
                          uint8_t seed[CTR_DRBG_ENTROPY_LEN]) {
  // If not in FIPS mode, we don't overread from the system entropy source and
  // we don't depend only on the hardware RDRAND.
  CRYPTO_sysrand(seed, CTR_DRBG_ENTROPY_LEN);
}

#endif

// rand_state_free_list is a list of currently free, |rand_state| structures.
// When a thread needs a |rand_state| it picks the head element of this list and
// allocs a new one if the list is empty. Once it's finished, it pushes the
// state back onto the front of the list.
//
// Previously we used a thread-local state but for processes with large numbers
// of threads this can result in excessive memory usage. Since we don't free
// |rand_state| objects, the number of objects in memory will eventually equal
// the maximum concurrency of |RAND_bytes|.
DEFINE_BSS_GET(struct rand_state *, rand_state_free_list);

// rand_state_lock protects |rand_state_free_list| (and |rand_state_all_list|,
// in FIPS mode).
DEFINE_STATIC_MUTEX(rand_state_lock);

#if defined(BORINGSSL_FIPS)
// rand_state_all_list is the head of a linked-list of all |rand_state| objects
// in the process. This is needed because FIPS requires that they be zeroed on
// process exit.
DEFINE_BSS_GET(struct rand_state *, rand_state_all_list);

// rand_drbg_lock is taken in write mode by |rand_state_clear_all|, and
// in read mode by any operation on the |drbg| member of |rand_state|.
// This ensures that, in the event that a thread races destructor functions, we
// never return bogus random data. At worst, the thread will deadlock.
DEFINE_STATIC_MUTEX(rand_drbg_lock);

static void rand_state_clear_all(void) __attribute__((destructor));
static void rand_state_clear_all(void) {
  CRYPTO_STATIC_MUTEX_lock_write(rand_drbg_lock_bss_get());
  CRYPTO_STATIC_MUTEX_lock_write(rand_state_lock_bss_get());
  for (struct rand_state *cur = *rand_state_all_list_bss_get();
       cur != NULL; cur = cur->next_all) {
    CTR_DRBG_clear(&cur->drbg);
  }
  // Both locks are deliberately left locked so that any threads that are still
  // running will hang if they try to call |RAND_bytes|.
}
#endif

// rand_state_init seeds a |rand_state|.
static void rand_state_init(struct rand_state *state) {
  OPENSSL_memset(state, 0, sizeof(struct rand_state));
  uint8_t seed[CTR_DRBG_ENTROPY_LEN];
  rand_get_seed(state, seed);
  if (!CTR_DRBG_init(&state->drbg, seed, NULL, 0)) {
    abort();
  }
}

// rand_state_get pops a |rand_state| from the head of
// |rand_state_free_list| and returns it. If the list is empty, it
// creates a fresh |rand_state| and returns that instead.
static struct rand_state *rand_state_get(void) {
  struct rand_state *state = NULL;
  CRYPTO_STATIC_MUTEX_lock_write(rand_state_lock_bss_get());
  state = *rand_state_free_list_bss_get();
  if (state != NULL) {
    *rand_state_free_list_bss_get() = state->next;
  }
  CRYPTO_STATIC_MUTEX_unlock_write(rand_state_lock_bss_get());

  if (state != NULL) {
    return state;
  }

  state = OPENSSL_malloc(sizeof(struct rand_state));
  if (state == NULL) {
    return NULL;
  }

  rand_state_init(state);

#if defined(BORINGSSL_FIPS)
  CRYPTO_STATIC_MUTEX_lock_write(rand_state_lock_bss_get());
  state->next_all = *rand_state_all_list_bss_get();
  *rand_state_all_list_bss_get() = state;
  CRYPTO_STATIC_MUTEX_unlock_write(rand_state_lock_bss_get());
#endif

  return state;
}

// rand_state_put pushes |state| onto |rand_state_free_list|.
static void rand_state_put(struct rand_state *state) {
  CRYPTO_STATIC_MUTEX_lock_write(rand_state_lock_bss_get());
  state->next = *rand_state_free_list_bss_get();
  *rand_state_free_list_bss_get() = state;
  CRYPTO_STATIC_MUTEX_unlock_write(rand_state_lock_bss_get());
}

void RAND_bytes_with_additional_data(uint8_t *out, size_t out_len,
                                     const uint8_t user_additional_data[32]) {
  if (out_len == 0) {
    return;
  }

  // Additional data is mixed into every CTR-DRBG call to protect, as best we
  // can, against forks & VM clones. We do not over-read this information and
  // don't reseed with it so, from the point of view of FIPS, this doesn't
  // provide “prediction resistance”. But, in practice, it does.
  uint8_t additional_data[32];
  if (!hwrand(additional_data, sizeof(additional_data))) {
    // Without a hardware RNG to save us from address-space duplication, the OS
    // entropy is used. This can be expensive (one read per |RAND_bytes| call)
    // and so can be disabled by applications that we have ensured don't fork
    // and aren't at risk of VM cloning.
    if (!rand_fork_unsafe_buffering_enabled()) {
      CRYPTO_sysrand(additional_data, sizeof(additional_data));
    } else {
      OPENSSL_memset(additional_data, 0, sizeof(additional_data));
    }
  }

  for (size_t i = 0; i < sizeof(additional_data); i++) {
    additional_data[i] ^= user_additional_data[i];
  }

  struct rand_state stack_state;
  struct rand_state *state = rand_state_get();

  if (state == NULL) {
    // If the system is out of memory, use an ephemeral state on the
    // stack.
    state = &stack_state;
    rand_state_init(state);
  }

  if (state->calls >= kReseedInterval) {
    uint8_t seed[CTR_DRBG_ENTROPY_LEN];
    rand_get_seed(state, seed);
#if defined(BORINGSSL_FIPS)
    // Take a read lock around accesses to |state->drbg|. This is needed to
    // avoid returning bad entropy if we race with
    // |rand_state_clear_all|.
    //
    // This lock must be taken after any calls to |CRYPTO_sysrand| to avoid a
    // bug on ppc64le. glibc may implement pthread locks by wrapping user code
    // in a hardware transaction, but, on some older versions of glibc and the
    // kernel, syscalls made with |syscall| did not abort the transaction.
    CRYPTO_STATIC_MUTEX_lock_read(rand_drbg_lock_bss_get());
#endif
    if (!CTR_DRBG_reseed(&state->drbg, seed, NULL, 0)) {
      abort();
    }
    state->calls = 0;
  } else {
#if defined(BORINGSSL_FIPS)
    CRYPTO_STATIC_MUTEX_lock_read(rand_drbg_lock_bss_get());
#endif
  }

  int first_call = 1;
  while (out_len > 0) {
    size_t todo = out_len;
    if (todo > CTR_DRBG_MAX_GENERATE_LENGTH) {
      todo = CTR_DRBG_MAX_GENERATE_LENGTH;
    }

    if (!CTR_DRBG_generate(&state->drbg, out, todo, additional_data,
                           first_call ? sizeof(additional_data) : 0)) {
      abort();
    }

    out += todo;
    out_len -= todo;
    state->calls++;
    first_call = 0;
  }

  if (state == &stack_state) {
    CTR_DRBG_clear(&state->drbg);
  }

#if defined(BORINGSSL_FIPS)
  CRYPTO_STATIC_MUTEX_unlock_read(rand_drbg_lock_bss_get());
#endif

  if (state != &stack_state) {
    rand_state_put(state);
  }
}

int RAND_bytes(uint8_t *out, size_t out_len) {
  static const uint8_t kZeroAdditionalData[32] = {0};
  RAND_bytes_with_additional_data(out, out_len, kZeroAdditionalData);
  return 1;
}

int RAND_pseudo_bytes(uint8_t *buf, size_t len) {
  return RAND_bytes(buf, len);
}
