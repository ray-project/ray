
#pragma once

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
};
}  // namespace ray

namespace ray {

using namespace ::ray;

template <typename T>
inline void Arguments::wrap(msgpack::packer<msgpack::sbuffer> &packer, const T &val) {
  packer.pack(val);
  return;
}

inline void Arguments::wrap(msgpack::packer<msgpack::sbuffer> &packer) { return; }

template <typename Arg1Type, typename... OtherArgTypes>
inline void Arguments::wrap(msgpack::packer<msgpack::sbuffer> &packer,
                            const Arg1Type &arg1, const OtherArgTypes &... args) {
  wrap(packer, arg1);
  wrap(packer, args...);
  return;
}

template <typename T>
inline void Arguments::unwrap(msgpack::unpacker &unpacker, T &val) {
  msgpack::object_handle oh;
  bool result = unpacker.next(oh);
  if (result == false) {
    throw "unpack error";
  }
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

}  // namespace ray