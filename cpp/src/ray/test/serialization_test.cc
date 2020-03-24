
#include <gtest/gtest.h>
#include <ray/api.h>

using namespace ray::api;

TEST(SerializationTest, TypeHybridTest) {
  uint32_t in_arg1 = 123456789, out_arg1;
  std::string in_arg2 = "123567ABC", out_arg2;

  // 1 arg
  // marshall
  msgpack::sbuffer buffer1;
  msgpack::packer<msgpack::sbuffer> pk1(&buffer1);
  Serializer::Serialize(pk1, in_arg1);
  // unmarshall
  msgpack::unpacker upk1;
  upk1.reserve_buffer(buffer1.size());
  memcpy(upk1.buffer(), buffer1.data(), buffer1.size());
  upk1.buffer_consumed(buffer1.size());

  Serializer::Deserialize(upk1, &out_arg1);

  EXPECT_EQ(in_arg1, out_arg1);

  // 2 args
  // marshall
  msgpack::sbuffer buffer2;
  msgpack::packer<msgpack::sbuffer> pk2(&buffer2);
  Serializer::Serialize(pk2, in_arg1);
  Serializer::Serialize(pk2, in_arg2);

  // unmarshall
  msgpack::unpacker upk2;
  upk2.reserve_buffer(buffer2.size());
  memcpy(upk2.buffer(), buffer2.data(), buffer2.size());
  upk2.buffer_consumed(buffer2.size());
  Serializer::Deserialize(upk2, &out_arg1);
  Serializer::Deserialize(upk2, &out_arg2);

  EXPECT_EQ(in_arg1, out_arg1);
  EXPECT_EQ(in_arg2, out_arg2);
}