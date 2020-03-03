
#pragma once


#include <ray/api/blob.h>
#include <msgpack.hpp>

namespace ray {

class Arguments {
 public:
  template <typename T>
  static void wrap(msgpack::packer<msgpack::sbuffer> &packer, const T &val);

  static void wrap(msgpack::packer<msgpack::sbuffer> &packer);

  template <typename Arg1Type, typename... OtherArgTypes>
  static void wrap(msgpack::packer<msgpack::sbuffer> &packer, const Arg1Type &arg1,
                   const OtherArgTypes &... args);

  template <typename T>
  static void unwrap(msgpack::unpacker &unpacker, T &val);

  static void unwrap(msgpack::unpacker &unpacker);

  template <typename Arg1Type, typename... OtherArgTypes>
  static void unwrap(msgpack::unpacker &unpacker, Arg1Type &arg1,
                     OtherArgTypes &... args);

  template <typename T>
  static void wrap(::ray::binary_writer &writer, const T &val);

  static void wrap(::ray::binary_writer &writer);

  template <typename Arg1Type, typename... OtherArgTypes>
  static void wrap(::ray::binary_writer &writer, const Arg1Type &arg1,
                   const OtherArgTypes &... args);

  template <typename T>
  static void unwrap(::ray::binary_reader &reader, T &val);

  static void unwrap(::ray::binary_reader &reader);

  template <typename Arg1Type, typename... OtherArgTypes>
  static void unwrap(::ray::binary_reader &reader, Arg1Type &arg1,
                     OtherArgTypes &... args);
};
}  // namespace ray



#include <ray/api/serialization.h>

namespace ray {

using namespace ::ray;

template <typename T>
inline void Arguments::wrap(msgpack::packer<msgpack::sbuffer> &packer, const T &val) {
  packer.pack(val);
  return;
}

inline void Arguments::wrap(msgpack::packer<msgpack::sbuffer> &packer) { return; }

template <typename Arg1Type, typename... OtherArgTypes>
inline void Arguments::wrap(msgpack::packer<msgpack::sbuffer> &packer, const Arg1Type &arg1,
                            const OtherArgTypes &... args) {
  wrap(packer, arg1);
  wrap(packer, args...);
  return;
}

template <typename T>
inline void Arguments::unwrap(msgpack::unpacker &unpacker, T &val) {
  msgpack::object_handle oh;
  pac.next(oh);
  msgpack::object obj = oh.get();
  obj.convert(val);
  return;
}

inline void Arguments::unwrap(msgpack::unpacker &unpacker) { return; }

template <typename Arg1Type, typename... OtherArgTypes>
inline void Arguments::unwrap(msgpack::unpacker &unpacker, Arg1Type &arg1,
                              OtherArgTypes &... args) {
  unwrap(unpacker, arg1);
  unwrap(unpacker, args...);
  return;
}

template <typename T>
inline void Arguments::wrap(::ray::binary_writer &writer, const T &val) {
  marshall(writer, val);
  return;
}

inline void Arguments::wrap(::ray::binary_writer &writer) { return; }

template <typename Arg1Type, typename... OtherArgTypes>
inline void Arguments::wrap(::ray::binary_writer &writer, const Arg1Type &arg1,
                            const OtherArgTypes &... args) {
  wrap(writer, arg1);
  wrap(writer, args...);
  return;
}

template <typename T>
inline void Arguments::unwrap(::ray::binary_reader &reader, T &val) {
  unmarshall(reader, val);
  return;
}

inline void Arguments::unwrap(::ray::binary_reader &reader) { return; }

template <typename Arg1Type, typename... OtherArgTypes>
inline void Arguments::unwrap(::ray::binary_reader &reader, Arg1Type &arg1,
                              OtherArgTypes &... args) {
  unwrap(reader, arg1);
  unwrap(reader, args...);
  return;
}

}  // namespace ray