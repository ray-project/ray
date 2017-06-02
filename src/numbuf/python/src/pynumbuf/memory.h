#ifndef PYNUMBUF_MEMORY_H
#define PYNUMBUF_MEMORY_H

#include <arrow/io/interfaces.h>

/* C++ includes */
#include <string>
#include <thread>
#include <vector>

#define THREADPOOL_SIZE 8
#define MEMCOPY_BLOCK_SIZE 64
#define BYTES_IN_MB (1 << 20)

using namespace std;

namespace numbuf {

class MockBufferStream : public arrow::io::OutputStream {
 public:
  virtual ~MockBufferStream() {}

  explicit MockBufferStream() : position_(0) {}

  arrow::Status Close() override { return arrow::Status::OK(); }

  arrow::Status Tell(int64_t* position) override {
    *position = position_;
    return arrow::Status::OK();
  }

  arrow::Status Write(const uint8_t* data, int64_t nbytes) override {
    position_ += nbytes;
    return arrow::Status::OK();
  }

 private:
  int64_t position_;
};

}  // namespace numbuf

#endif  // PYNUMBUF_MEMORY_H
