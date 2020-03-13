
#pragma once

#include <msgpack.hpp>

namespace ray {
namespace api {

class Arguments {
 public:
  template <typename T>
  static void Wrap(msgpack::packer<msgpack::sbuffer> &packer, const T &val);

  static void Wrap(msgpack::packer<msgpack::sbuffer> &packer);

  template <typename Arg1Type, typename... OtherArgTypes>
  static void Wrap(msgpack::packer<msgpack::sbuffer> &packer, const Arg1Type &arg1,
                   const OtherArgTypes &... args);

  template <typename T>
  static void Unwrap(msgpack::unpacker &unpacker, T &val);

  static void Unwrap(msgpack::unpacker &unpacker);

  template <typename Arg1Type, typename... OtherArgTypes>
  static void Unwrap(msgpack::unpacker &unpacker, Arg1Type &arg1,
                     OtherArgTypes &... args);
};
}  // namespace api
}  // namespace ray

namespace ray {
namespace api {

using namespace ::ray;

template <typename T>
inline void Arguments::Wrap(msgpack::packer<msgpack::sbuffer> &packer, const T &val) {
  packer.pack(val);
  return;
}

inline void Arguments::Wrap(msgpack::packer<msgpack::sbuffer> &packer) { return; }

template <typename Arg1Type, typename... OtherArgTypes>
inline void Arguments::Wrap(msgpack::packer<msgpack::sbuffer> &packer,
                            const Arg1Type &arg1, const OtherArgTypes &... args) {
  Wrap(packer, arg1);
  Wrap(packer, args...);
  return;
}

template <typename T>
inline void Arguments::Unwrap(msgpack::unpacker &unpacker, T &val) {
  msgpack::object_handle oh;
  bool result = unpacker.next(oh);
  if (result == false) {
    throw "unpack error";
  }
  msgpack::object obj = oh.get();
  obj.convert(val);
  return;
}

inline void Arguments::Unwrap(msgpack::unpacker &unpacker) { return; }

template <typename Arg1Type, typename... OtherArgTypes>
inline void Arguments::Unwrap(msgpack::unpacker &unpacker, Arg1Type &arg1,
                              OtherArgTypes &... args) {
  Unwrap(unpacker, arg1);
  Unwrap(unpacker, args...);
  return;
}

}  // namespace api
}  // namespace ray