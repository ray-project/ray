
#include <gtest/gtest.h>
#include <ray/api.h>
#include <ray/api/arguments.h>
#include <iostream>

using namespace ray;

TEST(ray_marshall, type_bool) {
  bool in_arg1 = true, out_arg1;
  bool in_arg2 = false, out_arg2;

  // 0 args
  // marshall
  msgpack::sbuffer buffer0;
  msgpack::packer<msgpack::sbuffer> pk0(&buffer0);
  Arguments::wrap(pk0);
  // unmarshall
  msgpack::unpacker upk0;
  Arguments::unwrap(upk0);

  // 1 args
  // marshall
  msgpack::sbuffer buffer1;
  msgpack::packer<msgpack::sbuffer> pk1(&buffer1);
  Arguments::wrap(pk1, in_arg1);
  // unmarshall
  msgpack::unpacker upk1;
  upk1.reserve_buffer(buffer1.size());
  memcpy(upk1.buffer(), buffer1.data(), buffer1.size());
  upk1.buffer_consumed(buffer1.size());

  Arguments::unwrap(upk1, out_arg1);

  EXPECT_EQ(in_arg1, out_arg1);

  // 2 args
  // marshall
  msgpack::sbuffer buffer2;
  msgpack::packer<msgpack::sbuffer> pk2(&buffer2);
  Arguments::wrap(pk2, in_arg1, in_arg2);

  // unmarshall
  msgpack::unpacker upk2;
  upk2.reserve_buffer(buffer2.size());
  memcpy(upk2.buffer(), buffer2.data(), buffer2.size());
  upk2.buffer_consumed(buffer2.size());
  Arguments::unwrap(upk2, out_arg1, out_arg2);

  EXPECT_EQ(in_arg1, out_arg1);
  EXPECT_EQ(in_arg2, out_arg2);
}

TEST(ray_marshall, type_int8) {
  int8_t in_arg1 = 5, out_arg1;
  int8_t in_arg2 = 123, out_arg2;

  // 0 args
  // marshall
  msgpack::sbuffer buffer0;
  msgpack::packer<msgpack::sbuffer> pk0(&buffer0);
  Arguments::wrap(pk0);
  // unmarshall
  msgpack::unpacker upk0;
  Arguments::unwrap(upk0);

  // 1 args
  // marshall
  msgpack::sbuffer buffer1;
  msgpack::packer<msgpack::sbuffer> pk1(&buffer1);
  Arguments::wrap(pk1, in_arg1);
  // unmarshall
  msgpack::unpacker upk1;
  upk1.reserve_buffer(buffer1.size());
  memcpy(upk1.buffer(), buffer1.data(), buffer1.size());
  upk1.buffer_consumed(buffer1.size());

  Arguments::unwrap(upk1, out_arg1);

  EXPECT_EQ(in_arg1, out_arg1);

  // 2 args
  // marshall
  msgpack::sbuffer buffer2;
  msgpack::packer<msgpack::sbuffer> pk2(&buffer2);
  Arguments::wrap(pk2, in_arg1, in_arg2);

  // unmarshall
  msgpack::unpacker upk2;
  upk2.reserve_buffer(buffer2.size());
  memcpy(upk2.buffer(), buffer2.data(), buffer2.size());
  upk2.buffer_consumed(buffer2.size());
  Arguments::unwrap(upk2, out_arg1, out_arg2);

  EXPECT_EQ(in_arg1, out_arg1);
  EXPECT_EQ(in_arg2, out_arg2);
}

TEST(ray_marshall, type_uint8) {
  uint8_t in_arg1 = 5, out_arg1;
  uint8_t in_arg2 = 255, out_arg2;

  // 0 args
  // marshall
  msgpack::sbuffer buffer0;
  msgpack::packer<msgpack::sbuffer> pk0(&buffer0);
  Arguments::wrap(pk0);
  // unmarshall
  msgpack::unpacker upk0;
  Arguments::unwrap(upk0);

  // 1 args
  // marshall
  msgpack::sbuffer buffer1;
  msgpack::packer<msgpack::sbuffer> pk1(&buffer1);
  Arguments::wrap(pk1, in_arg1);
  // unmarshall
  msgpack::unpacker upk1;
  upk1.reserve_buffer(buffer1.size());
  memcpy(upk1.buffer(), buffer1.data(), buffer1.size());
  upk1.buffer_consumed(buffer1.size());

  Arguments::unwrap(upk1, out_arg1);

  EXPECT_EQ(in_arg1, out_arg1);

  // 2 args
  // marshall
  msgpack::sbuffer buffer2;
  msgpack::packer<msgpack::sbuffer> pk2(&buffer2);
  Arguments::wrap(pk2, in_arg1, in_arg2);

  // unmarshall
  msgpack::unpacker upk2;
  upk2.reserve_buffer(buffer2.size());
  memcpy(upk2.buffer(), buffer2.data(), buffer2.size());
  upk2.buffer_consumed(buffer2.size());
  Arguments::unwrap(upk2, out_arg1, out_arg2);

  EXPECT_EQ(in_arg1, out_arg1);
  EXPECT_EQ(in_arg2, out_arg2);

  EXPECT_EQ(in_arg1, out_arg1);
  EXPECT_EQ(in_arg2, out_arg2);
}

TEST(ray_marshall, type_int16) {
  int16_t in_arg1 = 5, out_arg1;
  int16_t in_arg2 = 12345, out_arg2;

  // 0 args
  // marshall
  msgpack::sbuffer buffer0;
  msgpack::packer<msgpack::sbuffer> pk0(&buffer0);
  Arguments::wrap(pk0);
  // unmarshall
  msgpack::unpacker upk0;
  Arguments::unwrap(upk0);

  // 1 args
  // marshall
  msgpack::sbuffer buffer1;
  msgpack::packer<msgpack::sbuffer> pk1(&buffer1);
  Arguments::wrap(pk1, in_arg1);
  // unmarshall
  msgpack::unpacker upk1;
  upk1.reserve_buffer(buffer1.size());
  memcpy(upk1.buffer(), buffer1.data(), buffer1.size());
  upk1.buffer_consumed(buffer1.size());

  Arguments::unwrap(upk1, out_arg1);

  EXPECT_EQ(in_arg1, out_arg1);

  // 2 args
  // marshall
  msgpack::sbuffer buffer2;
  msgpack::packer<msgpack::sbuffer> pk2(&buffer2);
  Arguments::wrap(pk2, in_arg1, in_arg2);

  // unmarshall
  msgpack::unpacker upk2;
  upk2.reserve_buffer(buffer2.size());
  memcpy(upk2.buffer(), buffer2.data(), buffer2.size());
  upk2.buffer_consumed(buffer2.size());
  Arguments::unwrap(upk2, out_arg1, out_arg2);

  EXPECT_EQ(in_arg1, out_arg1);
  EXPECT_EQ(in_arg2, out_arg2);
}

TEST(ray_marshall, type_uint16) {
  uint16_t in_arg1 = 5, out_arg1;
  uint16_t in_arg2 = 65535, out_arg2;

  // 0 args
  // marshall
  msgpack::sbuffer buffer0;
  msgpack::packer<msgpack::sbuffer> pk0(&buffer0);
  Arguments::wrap(pk0);
  // unmarshall
  msgpack::unpacker upk0;
  Arguments::unwrap(upk0);

  // 1 args
  // marshall
  msgpack::sbuffer buffer1;
  msgpack::packer<msgpack::sbuffer> pk1(&buffer1);
  Arguments::wrap(pk1, in_arg1);
  // unmarshall
  msgpack::unpacker upk1;
  upk1.reserve_buffer(buffer1.size());
  memcpy(upk1.buffer(), buffer1.data(), buffer1.size());
  upk1.buffer_consumed(buffer1.size());

  Arguments::unwrap(upk1, out_arg1);

  EXPECT_EQ(in_arg1, out_arg1);

  // 2 args
  // marshall
  msgpack::sbuffer buffer2;
  msgpack::packer<msgpack::sbuffer> pk2(&buffer2);
  Arguments::wrap(pk2, in_arg1, in_arg2);

  // unmarshall
  msgpack::unpacker upk2;
  upk2.reserve_buffer(buffer2.size());
  memcpy(upk2.buffer(), buffer2.data(), buffer2.size());
  upk2.buffer_consumed(buffer2.size());
  Arguments::unwrap(upk2, out_arg1, out_arg2);

  EXPECT_EQ(in_arg1, out_arg1);
  EXPECT_EQ(in_arg2, out_arg2);
}

TEST(ray_marshall, type_int32) {
  int32_t in_arg1 = 5, out_arg1;
  int32_t in_arg2 = 1234567890, out_arg2;

  // 0 args
  // marshall
  msgpack::sbuffer buffer0;
  msgpack::packer<msgpack::sbuffer> pk0(&buffer0);
  Arguments::wrap(pk0);
  // unmarshall
  msgpack::unpacker upk0;
  Arguments::unwrap(upk0);

  // 1 args
  // marshall
  msgpack::sbuffer buffer1;
  msgpack::packer<msgpack::sbuffer> pk1(&buffer1);
  Arguments::wrap(pk1, in_arg1);
  // unmarshall
  msgpack::unpacker upk1;
  upk1.reserve_buffer(buffer1.size());
  memcpy(upk1.buffer(), buffer1.data(), buffer1.size());
  upk1.buffer_consumed(buffer1.size());

  Arguments::unwrap(upk1, out_arg1);

  EXPECT_EQ(in_arg1, out_arg1);

  // 2 args
  // marshall
  msgpack::sbuffer buffer2;
  msgpack::packer<msgpack::sbuffer> pk2(&buffer2);
  Arguments::wrap(pk2, in_arg1, in_arg2);

  // unmarshall
  msgpack::unpacker upk2;
  upk2.reserve_buffer(buffer2.size());
  memcpy(upk2.buffer(), buffer2.data(), buffer2.size());
  upk2.buffer_consumed(buffer2.size());
  Arguments::unwrap(upk2, out_arg1, out_arg2);

  EXPECT_EQ(in_arg1, out_arg1);
  EXPECT_EQ(in_arg2, out_arg2);
}

TEST(ray_marshall, type_uint32) {
  uint32_t in_arg1 = 5, out_arg1;
  uint32_t in_arg2 = 4234567890, out_arg2;

  // 0 args
  // marshall
  msgpack::sbuffer buffer0;
  msgpack::packer<msgpack::sbuffer> pk0(&buffer0);
  Arguments::wrap(pk0);
  // unmarshall
  msgpack::unpacker upk0;
  Arguments::unwrap(upk0);

  // 1 args
  // marshall
  msgpack::sbuffer buffer1;
  msgpack::packer<msgpack::sbuffer> pk1(&buffer1);
  Arguments::wrap(pk1, in_arg1);
  // unmarshall
  msgpack::unpacker upk1;
  upk1.reserve_buffer(buffer1.size());
  memcpy(upk1.buffer(), buffer1.data(), buffer1.size());
  upk1.buffer_consumed(buffer1.size());

  Arguments::unwrap(upk1, out_arg1);

  EXPECT_EQ(in_arg1, out_arg1);

  // 2 args
  // marshall
  msgpack::sbuffer buffer2;
  msgpack::packer<msgpack::sbuffer> pk2(&buffer2);
  Arguments::wrap(pk2, in_arg1, in_arg2);

  // unmarshall
  msgpack::unpacker upk2;
  upk2.reserve_buffer(buffer2.size());
  memcpy(upk2.buffer(), buffer2.data(), buffer2.size());
  upk2.buffer_consumed(buffer2.size());
  Arguments::unwrap(upk2, out_arg1, out_arg2);

  EXPECT_EQ(in_arg1, out_arg1);
  EXPECT_EQ(in_arg2, out_arg2);
}

TEST(ray_marshall, type_int64) {
  int64_t in_arg1 = 5, out_arg1;
  int64_t in_arg2 = 1234567890123456, out_arg2;

  // 0 args
  // marshall
  msgpack::sbuffer buffer0;
  msgpack::packer<msgpack::sbuffer> pk0(&buffer0);
  Arguments::wrap(pk0);
  // unmarshall
  msgpack::unpacker upk0;
  Arguments::unwrap(upk0);

  // 1 args
  // marshall
  msgpack::sbuffer buffer1;
  msgpack::packer<msgpack::sbuffer> pk1(&buffer1);
  Arguments::wrap(pk1, in_arg1);
  // unmarshall
  msgpack::unpacker upk1;
  upk1.reserve_buffer(buffer1.size());
  memcpy(upk1.buffer(), buffer1.data(), buffer1.size());
  upk1.buffer_consumed(buffer1.size());

  Arguments::unwrap(upk1, out_arg1);

  EXPECT_EQ(in_arg1, out_arg1);

  // 2 args
  // marshall
  msgpack::sbuffer buffer2;
  msgpack::packer<msgpack::sbuffer> pk2(&buffer2);
  Arguments::wrap(pk2, in_arg1, in_arg2);

  // unmarshall
  msgpack::unpacker upk2;
  upk2.reserve_buffer(buffer2.size());
  memcpy(upk2.buffer(), buffer2.data(), buffer2.size());
  upk2.buffer_consumed(buffer2.size());
  Arguments::unwrap(upk2, out_arg1, out_arg2);

  EXPECT_EQ(in_arg1, out_arg1);
  EXPECT_EQ(in_arg2, out_arg2);
}

TEST(ray_marshall, type_uint64) {
  uint64_t in_arg1 = 5, out_arg1;
  uint64_t in_arg2 = 1234567890123456789, out_arg2;

  // 0 args
  // marshall
  msgpack::sbuffer buffer0;
  msgpack::packer<msgpack::sbuffer> pk0(&buffer0);
  Arguments::wrap(pk0);
  // unmarshall
  msgpack::unpacker upk0;
  Arguments::unwrap(upk0);

  // 1 args
  // marshall
  msgpack::sbuffer buffer1;
  msgpack::packer<msgpack::sbuffer> pk1(&buffer1);
  Arguments::wrap(pk1, in_arg1);
  // unmarshall
  msgpack::unpacker upk1;
  upk1.reserve_buffer(buffer1.size());
  memcpy(upk1.buffer(), buffer1.data(), buffer1.size());
  upk1.buffer_consumed(buffer1.size());

  Arguments::unwrap(upk1, out_arg1);

  EXPECT_EQ(in_arg1, out_arg1);

  // 2 args
  // marshall
  msgpack::sbuffer buffer2;
  msgpack::packer<msgpack::sbuffer> pk2(&buffer2);
  Arguments::wrap(pk2, in_arg1, in_arg2);

  // unmarshall
  msgpack::unpacker upk2;
  upk2.reserve_buffer(buffer2.size());
  memcpy(upk2.buffer(), buffer2.data(), buffer2.size());
  upk2.buffer_consumed(buffer2.size());
  Arguments::unwrap(upk2, out_arg1, out_arg2);

  EXPECT_EQ(in_arg1, out_arg1);
  EXPECT_EQ(in_arg2, out_arg2);
}

TEST(ray_marshall, type_string) {
  std::string in_arg1 = "abcDE", out_arg1;
  std::string in_arg2 = "123567ABC", out_arg2;

  // 0 args
  // marshall
  msgpack::sbuffer buffer0;
  msgpack::packer<msgpack::sbuffer> pk0(&buffer0);
  Arguments::wrap(pk0);
  // unmarshall
  msgpack::unpacker upk0;
  Arguments::unwrap(upk0);

  // 1 args
  // marshall
  msgpack::sbuffer buffer1;
  msgpack::packer<msgpack::sbuffer> pk1(&buffer1);
  Arguments::wrap(pk1, in_arg1);
  // unmarshall
  msgpack::unpacker upk1;
  upk1.reserve_buffer(buffer1.size());
  memcpy(upk1.buffer(), buffer1.data(), buffer1.size());
  upk1.buffer_consumed(buffer1.size());

  Arguments::unwrap(upk1, out_arg1);

  EXPECT_EQ(in_arg1, out_arg1);

  // 2 args
  // marshall
  msgpack::sbuffer buffer2;
  msgpack::packer<msgpack::sbuffer> pk2(&buffer2);
  Arguments::wrap(pk2, in_arg1, in_arg2);

  // unmarshall
  msgpack::unpacker upk2;
  upk2.reserve_buffer(buffer2.size());
  memcpy(upk2.buffer(), buffer2.data(), buffer2.size());
  upk2.buffer_consumed(buffer2.size());
  Arguments::unwrap(upk2, out_arg1, out_arg2);

  EXPECT_EQ(in_arg1, out_arg1);
  EXPECT_EQ(in_arg2, out_arg2);
}

TEST(ray_marshall, type_hybrid) {
  uint32_t in_arg1 = 123456789, out_arg1;
  std::string in_arg2 = "123567ABC", out_arg2;

  // 0 args
  // marshall
  msgpack::sbuffer buffer0;
  msgpack::packer<msgpack::sbuffer> pk0(&buffer0);
  Arguments::wrap(pk0);
  // unmarshall
  msgpack::unpacker upk0;
  Arguments::unwrap(upk0);

  // 1 args
  // marshall
  msgpack::sbuffer buffer1;
  msgpack::packer<msgpack::sbuffer> pk1(&buffer1);
  Arguments::wrap(pk1, in_arg1);
  // unmarshall
  msgpack::unpacker upk1;
  upk1.reserve_buffer(buffer1.size());
  memcpy(upk1.buffer(), buffer1.data(), buffer1.size());
  upk1.buffer_consumed(buffer1.size());

  Arguments::unwrap(upk1, out_arg1);

  EXPECT_EQ(in_arg1, out_arg1);

  // 2 args
  // marshall
  msgpack::sbuffer buffer2;
  msgpack::packer<msgpack::sbuffer> pk2(&buffer2);
  Arguments::wrap(pk2, in_arg1, in_arg2);

  // unmarshall
  msgpack::unpacker upk2;
  upk2.reserve_buffer(buffer2.size());
  memcpy(upk2.buffer(), buffer2.data(), buffer2.size());
  upk2.buffer_consumed(buffer2.size());
  Arguments::unwrap(upk2, out_arg1, out_arg2);

  EXPECT_EQ(in_arg1, out_arg1);
  EXPECT_EQ(in_arg2, out_arg2);
}

TEST(ray_marshall, type_ray_object) {
  UniqueId uid1;
  uid1.random();
  RayObject<uint32_t> in_arg1(uid1);
  RayObject<uint32_t> out_arg1;

  UniqueId uid2;
  uid2.random();
  RayObject<std::string> in_arg2(uid2);
  RayObject<std::string> out_arg2;

  // 0 args
  // marshall
  msgpack::sbuffer buffer0;
  msgpack::packer<msgpack::sbuffer> pk0(&buffer0);
  Arguments::wrap(pk0);
  // unmarshall
  msgpack::unpacker upk0;
  Arguments::unwrap(upk0);

  // 1 args
  // marshall
  msgpack::sbuffer buffer1;
  msgpack::packer<msgpack::sbuffer> pk1(&buffer1);
  Arguments::wrap(pk1, in_arg1);
  // unmarshall
  msgpack::unpacker upk1;
  upk1.reserve_buffer(buffer1.size());
  memcpy(upk1.buffer(), buffer1.data(), buffer1.size());
  upk1.buffer_consumed(buffer1.size());

  Arguments::unwrap(upk1, out_arg1);

  EXPECT_EQ(in_arg1, out_arg1);

  // 2 args
  // marshall
  msgpack::sbuffer buffer2;
  msgpack::packer<msgpack::sbuffer> pk2(&buffer2);
  Arguments::wrap(pk2, in_arg1, in_arg2);

  // unmarshall
  msgpack::unpacker upk2;
  upk2.reserve_buffer(buffer2.size());
  memcpy(upk2.buffer(), buffer2.data(), buffer2.size());
  upk2.buffer_consumed(buffer2.size());
  Arguments::unwrap(upk2, out_arg1, out_arg2);

  EXPECT_EQ(in_arg1, out_arg1);
  EXPECT_EQ(in_arg2, out_arg2);
}