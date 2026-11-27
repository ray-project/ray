// erofs-pull's C API over rapidgzip: parallel decoding of one gzip stream, a
// layer that may still be downloading. See gzip.rs for the Rust side.
//
// The layer is a file that grows from its start as the download proceeds.
// GrowingFileReader reads it with pread, after a callback into Rust has
// waited for the bytes it wants to arrive. rapidgzip wraps it in a
// SharedFileReader, which serializes reads through it under a lock, so every
// byte rapidgzip decodes went through that wait.

#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <exception>
#include <filereader/FileReader.hpp>
#include <memory>
#include <optional>
#include <rapidgzip/ParallelGzipReader.hpp>
#include <stdexcept>
#include <string>
#include <system_error>

extern "C" {
// Waits until the first `end` bytes of the file are in; nonzero if they never
// will be (the download failed).
typedef int (*erofs_pull_wait_fn)(void *context, uint64_t end);
}

namespace {

class GrowingFileReader final : public rapidgzip::FileReader {
 public:
  GrowingFileReader(int fd, uint64_t size, erofs_pull_wait_fn wait, void *context)
      : fd_(fd), size_(size), wait_(wait), context_(context) {}

  void close() override { closed_ = true; }  // The fd belongs to the Rust side.
  [[nodiscard]] bool closed() const override { return closed_; }
  [[nodiscard]] bool eof() const override { return position_ >= size_; }
  [[nodiscard]] bool fail() const override { return failed_; }
  [[nodiscard]] int fileno() const override {
    // Nothing may read the fd around the wait.
    throw std::invalid_argument("A layer being downloaded has no fileno to share.");
  }
  [[nodiscard]] bool seekable() const override { return true; }
  [[nodiscard]] std::optional<size_t> size() const override { return size_; }
  [[nodiscard]] size_t tell() const override { return position_; }
  void clearerr() override { failed_ = false; }

  size_t read(char *buffer, size_t max_bytes) override {
    if (closed_) {
      throw std::invalid_argument("Read from a closed layer.");
    }
    const auto n = static_cast<size_t>(
        std::min<uint64_t>(max_bytes, size_ - std::min(position_, size_)));
    if (n == 0) {
      return 0;
    }
    if (wait_(context_, position_ + n) != 0) {
      failed_ = true;
      throw std::runtime_error("The layer's download failed.");
    }
    size_t done = 0;
    while (done < n) {
      const auto got =
          ::pread(fd_, buffer + done, n - done, static_cast<off_t>(position_ + done));
      if (got < 0 && errno == EINTR) {
        continue;
      }
      if (got <= 0) {
        failed_ = true;
        throw std::system_error(
            got < 0 ? errno : EIO, std::generic_category(), "Reading the layer failed");
      }
      done += static_cast<size_t>(got);
    }
    position_ += done;
    return done;
  }

  size_t seek(long long int offset, int origin) override {
    position_ = effectiveOffset(offset, origin);
    return position_;
  }

 protected:
  [[nodiscard]] rapidgzip::UniqueFileReader cloneRaw() const override {
    auto clone = std::make_unique<GrowingFileReader>(fd_, size_, wait_, context_);
    clone->position_ = position_;
    return clone;
  }

 private:
  int fd_;
  uint64_t size_;
  erofs_pull_wait_fn wait_;
  void *context_;
  uint64_t position_ = 0;
  bool closed_ = false;
  bool failed_ = false;
};

void set_error(char *error, size_t error_len, const char *message) {
  if (error != nullptr && error_len > 0) {
    std::strncpy(error, message, error_len - 1);
    error[error_len - 1] = '\0';
  }
}

using Reader = rapidgzip::ParallelGzipReader<>;

}  // namespace

extern "C" {

// Opens a decoder for the gzip stream in fd (`size` bytes once downloaded),
// decoding on `parallelism` threads (0: one per core) in chunks of
// `chunk_size` compressed bytes. Null on failure, with `error` set.
void *erofs_pull_gzip_open(int fd,
                           uint64_t size,
                           erofs_pull_wait_fn wait,
                           void *context,
                           uint32_t parallelism,
                           uint64_t chunk_size,
                           char *error,
                           size_t error_len) {
  try {
    auto reader = std::make_unique<Reader>(
        std::make_unique<GrowingFileReader>(fd, size, wait, context),
        parallelism,
        chunk_size);
    // gzip's own CRC-32 and size checks, on top of the layer's digest.
    reader->setCRC32Enabled(true);
    // Memory: the decoded chunks rapidgzip holds at a time are most of
    // erofs-pull's. Split a chunk that decodes to more than 8 times its size
    // (rapidgzip's default is 20), and drop each chunk's window once it's
    // read, as the layer is read once, front to back, and never seeked in.
    reader->setMaxDecompressedChunkSize(8 * chunk_size);
    reader->setKeepIndex(false);
    return reader.release();
  } catch (const std::exception &e) {
    set_error(error, error_len, e.what());
  } catch (...) {
    set_error(error, error_len, "unknown error opening the layer");
  }
  return nullptr;
}

// Reads up to `len` decoded bytes: the number read, 0 at the stream's end,
// -1 on failure with `error` set.
int64_t erofs_pull_gzip_read(
    void *reader, uint8_t *buffer, size_t len, char *error, size_t error_len) {
  try {
    return static_cast<int64_t>(
        static_cast<Reader *>(reader)->read(reinterpret_cast<char *>(buffer), len));
  } catch (const std::exception &e) {
    set_error(error, error_len, e.what());
  } catch (...) {
    set_error(error, error_len, "unknown error decoding the layer");
  }
  return -1;
}

// Stops the decoder's threads and frees it.
void erofs_pull_gzip_close(void *reader) {
  try {
    delete static_cast<Reader *>(reader);
  } catch (...) {
  }
}

}  // extern "C"
