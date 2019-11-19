#pragma once

#include <cstring>
#include <memory>
#include <string>
#include <vector>

namespace ray {
template <typename T>
std::shared_ptr<T> make_shared_array(size_t size) {
  return std::shared_ptr<T>(new T[size], std::default_delete<T[]>());
}

class blob {
 public:
  blob() : _buffer(nullptr), _data(nullptr), _length(0) {}

  blob(const std::shared_ptr<char> &buffer, unsigned int length)
      : _holder(buffer), _buffer(_holder.get()), _data(_holder.get()), _length(length) {}

  blob(std::shared_ptr<char> &&buffer, unsigned int length)
      : _holder(std::move(buffer)),
        _buffer(_holder.get()),
        _data(_holder.get()),
        _length(length) {}

  blob(const std::shared_ptr<char> &buffer, int offset, unsigned int length)
      : _holder(buffer),
        _buffer(_holder.get()),
        _data(_holder.get() + offset),
        _length(length) {}

  blob(std::shared_ptr<char> &&buffer, int offset, unsigned int length)
      : _holder(std::move(buffer)),
        _buffer(_holder.get()),
        _data(_holder.get() + offset),
        _length(length) {}

  blob(const char *buffer, int offset, unsigned int length)
      : _buffer(buffer), _data(buffer + offset), _length(length) {}

  blob(const blob &source)
      : _holder(source._holder),
        _buffer(source._buffer),
        _data(source._data),
        _length(source._length) {}

  blob(blob &&source)
      : _holder(std::move(source._holder)),
        _buffer(source._buffer),
        _data(source._data),
        _length(source._length) {
    source._buffer = nullptr;
    source._data = nullptr;
    source._length = 0;
  }

  blob &operator=(const blob &that) {
    _holder = that._holder;
    _buffer = that._buffer;
    _data = that._data;
    _length = that._length;
    return *this;
  }

  blob &operator=(blob &&that) {
    _holder = std::move(that._holder);
    _buffer = that._buffer;
    _data = that._data;
    _length = that._length;
    that._buffer = nullptr;
    that._data = nullptr;
    that._length = 0;
    return *this;
  }

  void assign(const std::shared_ptr<char> &buffer, int offset, unsigned int length) {
    _holder = buffer;
    _buffer = _holder.get();
    _data = _holder.get() + offset;
    _length = length;
  }

  void assign(std::shared_ptr<char> &&buffer, int offset, unsigned int length) {
    _holder = std::move(buffer);
    _buffer = (_holder.get());
    _data = (_holder.get() + offset);
    _length = length;
  }

  void assign(const char *buffer, int offset, unsigned int length) {
    _holder = nullptr;
    _buffer = buffer;
    _data = buffer + offset;
    _length = length;
  }

  void extend(unsigned int len) { _length += len; }

  void remove_tail(unsigned int len) { _length -= len; }

  void remove_head(unsigned int len) {
    _data += len;
    _length -= len;
  }

  void set_length(unsigned int len) { _length = len; }

  const char *data() const { return _data; }

  unsigned int length() const { return _length; }

  std::shared_ptr<char> buffer() const { return _holder; }

  bool has_holder() const { return _holder.get() != nullptr; }

  const char *buffer_ptr() const { return _holder.get(); }

  // offset can be negative for buffer dereference
  blob range(int offset) const {
    assert(offset <= static_cast<int>(_length));

    blob temp = *this;
    temp._data += offset;
    temp._length -= offset;
    return temp;
  }

  blob range(int offset, unsigned int len) const {
    assert(offset <= static_cast<int>(_length));

    blob temp = *this;
    temp._data += offset;
    temp._length -= offset;
    assert(temp._length >= len);
    temp._length = len;
    return temp;
  }

  bool operator==(const blob &r) const {
    assert(false);
    return false;
  }

 private:
  friend class binary_writer;
  std::shared_ptr<char> _holder;
  const char *_buffer;
  const char *_data;
  unsigned int _length;  // data length
};

class binary_reader {
 public:
  // given bb on ctor
  binary_reader(const blob &blob);

  // or delayed init
  binary_reader() {}

  virtual ~binary_reader() {}

  void init(const blob &bb);

  template <typename T>
  int read_pod(/*out*/ T &val);
  template <typename T>
  int read(/*out*/ T &val) {
    assert(false);
    return 0;
  }
  int read(/*out*/ int8_t &val) { return read_pod(val); }
  int read(/*out*/ uint8_t &val) { return read_pod(val); }
  int read(/*out*/ int16_t &val) { return read_pod(val); }
  int read(/*out*/ uint16_t &val) { return read_pod(val); }
  int read(/*out*/ int32_t &val) { return read_pod(val); }
  int read(/*out*/ uint32_t &val) { return read_pod(val); }
  int read(/*out*/ int64_t &val) { return read_pod(val); }
  int read(/*out*/ uint64_t &val) { return read_pod(val); }
  int read(/*out*/ bool &val) { return read_pod(val); }

  int read(/*out*/ std::string &s);
  int read(char *buffer, int sz);
  int read(blob &blob);

  bool next(const void **data, int *size);
  bool skip(int count);
  bool backup(int count);

  blob get_buffer() const { return _blob; }
  blob get_remaining_buffer() const {
    return _blob.range(static_cast<int>(_ptr - _blob.data()));
  }
  bool is_eof() const { return _ptr >= _blob.data() + _size; }
  int total_size() const { return _size; }
  int get_remaining_size() const { return _remaining_size; }
  const char *ptr() const { return _ptr; }

 private:
  blob _blob;
  int _size;
  const char *_ptr;
  int _remaining_size;
};

class binary_writer {
 public:
  binary_writer(int reserved_buffer_size = 0);
  binary_writer(blob &buffer);
  virtual ~binary_writer();

  virtual void flush();

  template <typename T>
  void write_pod(const T &val);
  template <typename T>
  void write(const T &val) {
    assert(false);
  }
  void write(const int8_t &val) { write_pod(val); }
  void write(const uint8_t &val) { write_pod(val); }
  void write(const int16_t &val) { write_pod(val); }
  void write(const uint16_t &val) { write_pod(val); }
  void write(const int32_t &val) { write_pod(val); }
  void write(const uint32_t &val) { write_pod(val); }
  void write(const int64_t &val) { write_pod(val); }
  void write(const uint64_t &val) { write_pod(val); }
  void write(const bool &val) { write_pod(val); }

  void write(const std::string &val);
  void write(const char *buffer, int sz);
  void write(const blob &val);
  void write_empty(int sz);

  bool next(void **data, int *size);
  bool backup(int count);

  void get_buffers(/*out*/ std::vector<blob> &buffers);
  int get_buffer_count() const { return static_cast<int>(_buffers.size()); }
  blob get_buffer();
  blob get_current_buffer();  // without commit, write can be continued on the last buffer
  blob get_first_buffer() const;

  int total_size() const { return _total_size; }

 protected:
  // bb may have large space than size
  void create_buffer(size_t size);
  void commit();
  virtual void create_new_buffer(size_t size, /*out*/ blob &bb);

 private:
  std::vector<blob> _buffers;

  char *_current_buffer;
  int _current_offset;
  int _current_buffer_length;

  int _total_size;
  int _reserved_size_per_buffer;
};

//--------------- inline implementation -------------------
#define BINARY_WRITER_RESERVED_SIZE_PER_BUFFER 256

template <typename T>
inline int binary_reader::read_pod(/*out*/ T &val) {
  if (sizeof(T) <= get_remaining_size()) {
    memcpy((void *)&val, _ptr, sizeof(T));
    _ptr += sizeof(T);
    _remaining_size -= sizeof(T);
    return static_cast<int>(sizeof(T));
  } else {
    assert(false);
    return 0;
  }
}

template <typename T>
inline void binary_writer::write_pod(const T &val) {
  write((char *)&val, static_cast<int>(sizeof(T)));
}

inline void binary_writer::get_buffers(/*out*/ std::vector<blob> &buffers) {
  commit();
  buffers = _buffers;
}

inline blob binary_writer::get_first_buffer() const { return _buffers[0]; }

inline void binary_writer::write(const std::string &val) {
  int len = static_cast<int>(val.length());
  write((const char *)&len, sizeof(int));
  if (len > 0) write((const char *)&val[0], len);
}

inline void binary_writer::write(const blob &val) {
  // TODO: optimization by not memcpy
  int len = val.length();
  write((const char *)&len, sizeof(int));
  if (len > 0) write((const char *)val.data(), len);
}

inline binary_reader::binary_reader(const blob &blob) { init(blob); }

inline void binary_reader::init(const blob &bb) {
  _blob = bb;
  _size = bb.length();
  _ptr = bb.data();
  _remaining_size = _size;
}

inline int binary_reader::read(/*out*/ std::string &s) {
  int len;
  if (0 == read(len)) return 0;

  s.resize(len, 0);

  if (len > 0) {
    int x = read((char *)&s[0], len);
    return x == 0 ? x : (x + sizeof(len));
  } else {
    return static_cast<int>(sizeof(len));
  }
}

inline int binary_reader::read(blob &blob) {
  int len;
  if (0 == read(len)) return 0;

  if (len <= get_remaining_size()) {
    blob = _blob.range(static_cast<int>(_ptr - _blob.data()), len);

    // optimization: zero-copy
    if (!blob.buffer_ptr()) {
      std::shared_ptr<char> buffer(::ray::make_shared_array<char>(len));
      memcpy(buffer.get(), blob.data(), blob.length());
      blob = ::ray::blob(buffer, 0, blob.length());
    }

    _ptr += len;
    _remaining_size -= len;
    return len + sizeof(len);
  } else {
    assert(false);
    return 0;
  }
}

inline int binary_reader::read(char *buffer, int sz) {
  if (sz <= get_remaining_size()) {
    memcpy((void *)buffer, _ptr, sz);
    _ptr += sz;
    _remaining_size -= sz;
    return sz;
  } else {
    assert(false);
    return 0;
  }
}

inline bool binary_reader::next(const void **data, int *size) {
  if (get_remaining_size() > 0) {
    *data = (const void *)_ptr;
    *size = _remaining_size;

    _ptr += _remaining_size;
    _remaining_size = 0;
    return true;
  } else
    return false;
}

inline bool binary_reader::backup(int count) {
  if (count <= static_cast<int>(_ptr - _blob.data())) {
    _ptr -= count;
    _remaining_size += count;
    return true;
  } else
    return false;
}

inline bool binary_reader::skip(int count) {
  if (count <= get_remaining_size()) {
    _ptr += count;
    _remaining_size -= count;
    return true;
  } else {
    assert(false);
    return false;
  }
}

inline binary_writer::binary_writer(int reserveBufferSize) {
  _total_size = 0;
  _buffers.reserve(1);
  _reserved_size_per_buffer = (reserveBufferSize == 0)
                                  ? BINARY_WRITER_RESERVED_SIZE_PER_BUFFER
                                  : reserveBufferSize;
  _current_buffer = nullptr;
  _current_offset = 0;
  _current_buffer_length = 0;
}

inline binary_writer::binary_writer(blob &buffer) {
  _total_size = 0;
  _buffers.reserve(1);
  _reserved_size_per_buffer = BINARY_WRITER_RESERVED_SIZE_PER_BUFFER;

  _buffers.push_back(buffer);
  _current_buffer = (char *)buffer.data();
  _current_offset = 0;
  _current_buffer_length = buffer.length();
}

inline binary_writer::~binary_writer() {}

inline void binary_writer::flush() { commit(); }

inline void binary_writer::create_buffer(size_t size) {
  commit();

  blob bb;
  create_new_buffer(size, bb);
  _buffers.push_back(bb);

  _current_buffer = (char *)bb.data();
  _current_buffer_length = bb.length();
}

inline void binary_writer::create_new_buffer(size_t size, /*out*/ blob &bb) {
  bb.assign(::ray::make_shared_array<char>(size), 0, (int)size);
}

inline void binary_writer::commit() {
  if (_current_offset > 0) {
    *_buffers.rbegin() = _buffers.rbegin()->range(0, _current_offset);

    _current_offset = 0;
    _current_buffer_length = 0;
  }
}

inline blob binary_writer::get_buffer() {
  commit();

  if (_buffers.size() == 1) {
    return _buffers[0];
  } else if (_total_size == 0) {
    return blob();
  } else {
    std::shared_ptr<char> bptr(::ray::make_shared_array<char>(_total_size));
    blob bb(bptr, _total_size);
    const char *ptr = bb.data();

    for (int i = 0; i < static_cast<int>(_buffers.size()); i++) {
      memcpy((void *)ptr, (const void *)_buffers[i].data(), (size_t)_buffers[i].length());
      ptr += _buffers[i].length();
    }
    return bb;
  }
}

inline blob binary_writer::get_current_buffer() {
  if (_buffers.size() == 1) {
    return _current_offset > 0 ? _buffers[0].range(0, _current_offset) : _buffers[0];
  } else {
    std::shared_ptr<char> bptr(::ray::make_shared_array<char>(_total_size));
    blob bb(bptr, _total_size);
    const char *ptr = bb.data();

    for (int i = 0; i < static_cast<int>(_buffers.size()); i++) {
      size_t len = (size_t)_buffers[i].length();
      if (_current_offset > 0 && i + 1 == (int)_buffers.size()) {
        len = _current_offset;
      }

      memcpy((void *)ptr, (const void *)_buffers[i].data(), len);
      ptr += _buffers[i].length();
    }
    return bb;
  }
}

inline void binary_writer::write_empty(int sz) {
  int sz0 = sz;
  int rem_size = _current_buffer_length - _current_offset;
  if (rem_size >= sz) {
    _current_offset += sz;
  } else {
    _current_offset += rem_size;
    sz -= rem_size;

    int allocSize = _reserved_size_per_buffer;
    if (sz > allocSize) allocSize = sz;

    create_buffer(allocSize);
    _current_offset += sz;
  }

  _total_size += sz0;
}

inline void binary_writer::write(const char *buffer, int sz) {
  int rem_size = _current_buffer_length - _current_offset;
  if (rem_size >= sz) {
    memcpy((void *)(_current_buffer + _current_offset), buffer, (size_t)sz);
    _current_offset += sz;
    _total_size += sz;
  } else {
    if (rem_size > 0) {
      memcpy((void *)(_current_buffer + _current_offset), buffer, (size_t)rem_size);
      _current_offset += rem_size;
      _total_size += rem_size;
      sz -= rem_size;
    }

    int allocSize = _reserved_size_per_buffer;
    if (sz > allocSize) allocSize = sz;

    create_buffer(allocSize);
    memcpy((void *)(_current_buffer + _current_offset), buffer + rem_size, (size_t)sz);
    _current_offset += sz;
    _total_size += sz;
  }
}

inline bool binary_writer::next(void **data, int *size) {
  int rem_size = _current_buffer_length - _current_offset;
  if (rem_size == 0) {
    create_buffer(_reserved_size_per_buffer);
    rem_size = _current_buffer_length;
  }

  *size = rem_size;
  *data = (void *)(_current_buffer + _current_offset);
  _current_offset = _current_buffer_length;
  _total_size += rem_size;
  return true;
}

inline bool binary_writer::backup(int count) {
  assert(count <= _current_offset);
  _current_offset -= count;
  _total_size -= count;
  return true;
}
}  // namespace ray
