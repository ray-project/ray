
#include <gtest/gtest.h>
#include <ray/api.h>

TEST(SerializationTest, TypeHybridTest) {
  uint32_t in_arg1 = 123456789, out_arg1;
  std::string in_arg2 = "123567ABC", out_arg2;

  // 1 arg
  // marshall
  msgpack::sbuffer buffer1 = ray::internal::Serializer::Serialize(in_arg1);
  // unmarshall
  out_arg1 =
      ray::internal::Serializer::Deserialize<uint32_t>(buffer1.data(), buffer1.size());

  EXPECT_EQ(in_arg1, out_arg1);

  // 2 args
  // marshall
  msgpack::sbuffer buffer2 =
      ray::internal::Serializer::Serialize(std::make_tuple(in_arg1, in_arg2));

  // unmarshall
  std::tie(out_arg1, out_arg2) =
      ray::internal::Serializer::Deserialize<std::tuple<uint32_t, std::string>>(
          buffer2.data(), buffer2.size());

  EXPECT_EQ(in_arg1, out_arg1);
  EXPECT_EQ(in_arg2, out_arg2);
}