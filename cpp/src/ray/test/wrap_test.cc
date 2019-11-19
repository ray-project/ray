
#include <gtest/gtest.h>
#include <ray/api/blob.h>
#include <ray/api/impl/arguments.h>

using namespace ray;

TEST(ray_marshall, type_bool) {
  bool in_arg1 = true, out_arg1;
  bool in_arg2 = false, out_arg2;

  ::ray::blob blob;
  // 0 args
  // marshall
  ::ray::binary_writer writer0;
  Arguments::wrap(writer0);
  blob = writer0.get_buffer();
  // unmarshall
  ::ray::binary_reader reader0(blob);
  Arguments::unwrap(reader0);

  // 1 args
  // marshall
  ::ray::binary_writer writer1;
  Arguments::wrap(writer1, in_arg1);
  blob = writer1.get_buffer();
  // unmarshall
  ::ray::binary_reader reader1(blob);
  Arguments::unwrap(reader1, out_arg1);

  EXPECT_EQ(in_arg1, out_arg1);

  // 2 args
  // marshall
  ::ray::binary_writer writer2;
  Arguments::wrap(writer2, in_arg1, in_arg2);
  blob = writer2.get_buffer();
  // unmarshall
  ::ray::binary_reader reader2(blob);
  Arguments::unwrap(reader2, out_arg1, out_arg2);

  EXPECT_EQ(in_arg1, out_arg1);
  EXPECT_EQ(in_arg2, out_arg2);
}

TEST(ray_marshall, type_int8) {
  int8_t in_arg1 = 5, out_arg1;
  int8_t in_arg2 = 123, out_arg2;

  ::ray::blob blob;
  // 0 args
  // marshall
  ::ray::binary_writer writer0;
  Arguments::wrap(writer0);
  blob = writer0.get_buffer();
  // unmarshall
  ::ray::binary_reader reader0(blob);
  Arguments::unwrap(reader0);

  // 1 args
  // marshall
  ::ray::binary_writer writer1;
  Arguments::wrap(writer1, in_arg1);
  blob = writer1.get_buffer();
  // unmarshall
  ::ray::binary_reader reader1(blob);
  Arguments::unwrap(reader1, out_arg1);

  EXPECT_EQ(in_arg1, out_arg1);

  // 2 args
  // marshall
  ::ray::binary_writer writer2;
  Arguments::wrap(writer2, in_arg1, in_arg2);
  blob = writer2.get_buffer();
  // unmarshall
  ::ray::binary_reader reader2(blob);
  Arguments::unwrap(reader2, out_arg1, out_arg2);

  EXPECT_EQ(in_arg1, out_arg1);
  EXPECT_EQ(in_arg2, out_arg2);
}

TEST(ray_marshall, type_uint8) {
  uint8_t in_arg1 = 5, out_arg1;
  uint8_t in_arg2 = 255, out_arg2;

  ::ray::blob blob;
  // 0 args
  // marshall
  ::ray::binary_writer writer0;
  Arguments::wrap(writer0);
  blob = writer0.get_buffer();
  // unmarshall
  ::ray::binary_reader reader0(blob);
  Arguments::unwrap(reader0);

  // 1 args
  // marshall
  ::ray::binary_writer writer1;
  Arguments::wrap(writer1, in_arg1);
  blob = writer1.get_buffer();
  // unmarshall
  ::ray::binary_reader reader1(blob);
  Arguments::unwrap(reader1, out_arg1);

  EXPECT_EQ(in_arg1, out_arg1);

  // 2 args
  // marshall
  ::ray::binary_writer writer2;
  Arguments::wrap(writer2, in_arg1, in_arg2);
  blob = writer2.get_buffer();
  // unmarshall
  ::ray::binary_reader reader2(blob);
  Arguments::unwrap(reader2, out_arg1, out_arg2);

  EXPECT_EQ(in_arg1, out_arg1);
  EXPECT_EQ(in_arg2, out_arg2);
}

TEST(ray_marshall, type_int16) {
  int16_t in_arg1 = 5, out_arg1;
  int16_t in_arg2 = 12345, out_arg2;

  ::ray::blob blob;
  // 0 args
  // marshall
  ::ray::binary_writer writer0;
  Arguments::wrap(writer0);
  blob = writer0.get_buffer();
  // unmarshall
  ::ray::binary_reader reader0(blob);
  Arguments::unwrap(reader0);

  // 1 args
  // marshall
  ::ray::binary_writer writer1;
  Arguments::wrap(writer1, in_arg1);
  blob = writer1.get_buffer();
  // unmarshall
  ::ray::binary_reader reader1(blob);
  Arguments::unwrap(reader1, out_arg1);

  EXPECT_EQ(in_arg1, out_arg1);

  // 2 args
  // marshall
  ::ray::binary_writer writer2;
  Arguments::wrap(writer2, in_arg1, in_arg2);
  blob = writer2.get_buffer();
  // unmarshall
  ::ray::binary_reader reader2(blob);
  Arguments::unwrap(reader2, out_arg1, out_arg2);

  EXPECT_EQ(in_arg1, out_arg1);
  EXPECT_EQ(in_arg2, out_arg2);
}

TEST(ray_marshall, type_uint16) {
  uint16_t in_arg1 = 5, out_arg1;
  uint16_t in_arg2 = 65535, out_arg2;

  ::ray::blob blob;
  // 0 args
  // marshall
  ::ray::binary_writer writer0;
  Arguments::wrap(writer0);
  blob = writer0.get_buffer();
  // unmarshall
  ::ray::binary_reader reader0(blob);
  Arguments::unwrap(reader0);

  // 1 args
  // marshall
  ::ray::binary_writer writer1;
  Arguments::wrap(writer1, in_arg1);
  blob = writer1.get_buffer();
  // unmarshall
  ::ray::binary_reader reader1(blob);
  Arguments::unwrap(reader1, out_arg1);

  EXPECT_EQ(in_arg1, out_arg1);

  // 2 args
  // marshall
  ::ray::binary_writer writer2;
  Arguments::wrap(writer2, in_arg1, in_arg2);
  blob = writer2.get_buffer();
  // unmarshall
  ::ray::binary_reader reader2(blob);
  Arguments::unwrap(reader2, out_arg1, out_arg2);

  EXPECT_EQ(in_arg1, out_arg1);
  EXPECT_EQ(in_arg2, out_arg2);
}

TEST(ray_marshall, type_int32) {
  int32_t in_arg1 = 5, out_arg1;
  int32_t in_arg2 = 1234567890, out_arg2;

  ::ray::blob blob;
  // 0 args
  // marshall
  ::ray::binary_writer writer0;
  Arguments::wrap(writer0);
  blob = writer0.get_buffer();
  // unmarshall
  ::ray::binary_reader reader0(blob);
  Arguments::unwrap(reader0);

  // 1 args
  // marshall
  ::ray::binary_writer writer1;
  Arguments::wrap(writer1, in_arg1);
  blob = writer1.get_buffer();
  // unmarshall
  ::ray::binary_reader reader1(blob);
  Arguments::unwrap(reader1, out_arg1);

  EXPECT_EQ(in_arg1, out_arg1);

  // 2 args
  // marshall
  ::ray::binary_writer writer2;
  Arguments::wrap(writer2, in_arg1, in_arg2);
  blob = writer2.get_buffer();
  // unmarshall
  ::ray::binary_reader reader2(blob);
  Arguments::unwrap(reader2, out_arg1, out_arg2);

  EXPECT_EQ(in_arg1, out_arg1);
  EXPECT_EQ(in_arg2, out_arg2);
}

TEST(ray_marshall, type_uint32) {
  uint32_t in_arg1 = 5, out_arg1;
  uint32_t in_arg2 = 4234567890, out_arg2;

  ::ray::blob blob;
  // 0 args
  // marshall
  ::ray::binary_writer writer0;
  Arguments::wrap(writer0);
  blob = writer0.get_buffer();
  // unmarshall
  ::ray::binary_reader reader0(blob);
  Arguments::unwrap(reader0);

  // 1 args
  // marshall
  ::ray::binary_writer writer1;
  Arguments::wrap(writer1, in_arg1);
  blob = writer1.get_buffer();
  // unmarshall
  ::ray::binary_reader reader1(blob);
  Arguments::unwrap(reader1, out_arg1);

  EXPECT_EQ(in_arg1, out_arg1);

  // 2 args
  // marshall
  ::ray::binary_writer writer2;
  Arguments::wrap(writer2, in_arg1, in_arg2);
  blob = writer2.get_buffer();
  // unmarshall
  ::ray::binary_reader reader2(blob);
  Arguments::unwrap(reader2, out_arg1, out_arg2);

  EXPECT_EQ(in_arg1, out_arg1);
  EXPECT_EQ(in_arg2, out_arg2);
}

TEST(ray_marshall, type_int64) {
  int64_t in_arg1 = 5, out_arg1;
  int64_t in_arg2 = 1234567890123456, out_arg2;

  ::ray::blob blob;
  // 0 args
  // marshall
  ::ray::binary_writer writer0;
  Arguments::wrap(writer0);
  blob = writer0.get_buffer();
  // unmarshall
  ::ray::binary_reader reader0(blob);
  Arguments::unwrap(reader0);

  // 1 args
  // marshall
  ::ray::binary_writer writer1;
  Arguments::wrap(writer1, in_arg1);
  blob = writer1.get_buffer();
  // unmarshall
  ::ray::binary_reader reader1(blob);
  Arguments::unwrap(reader1, out_arg1);

  EXPECT_EQ(in_arg1, out_arg1);

  // 2 args
  // marshall
  ::ray::binary_writer writer2;
  Arguments::wrap(writer2, in_arg1, in_arg2);
  blob = writer2.get_buffer();
  // unmarshall
  ::ray::binary_reader reader2(blob);
  Arguments::unwrap(reader2, out_arg1, out_arg2);

  EXPECT_EQ(in_arg1, out_arg1);
  EXPECT_EQ(in_arg2, out_arg2);
}

TEST(ray_marshall, type_uint64) {
  uint64_t in_arg1 = 5, out_arg1;
  uint64_t in_arg2 = 1234567890123456789, out_arg2;

  ::ray::blob blob;
  // 0 args
  // marshall
  ::ray::binary_writer writer0;
  Arguments::wrap(writer0);
  blob = writer0.get_buffer();
  // unmarshall
  ::ray::binary_reader reader0(blob);
  Arguments::unwrap(reader0);

  // 1 args
  // marshall
  ::ray::binary_writer writer1;
  Arguments::wrap(writer1, in_arg1);
  blob = writer1.get_buffer();
  // unmarshall
  ::ray::binary_reader reader1(blob);
  Arguments::unwrap(reader1, out_arg1);

  EXPECT_EQ(in_arg1, out_arg1);

  // 2 args
  // marshall
  ::ray::binary_writer writer2;
  Arguments::wrap(writer2, in_arg1, in_arg2);
  blob = writer2.get_buffer();
  // unmarshall
  ::ray::binary_reader reader2(blob);
  Arguments::unwrap(reader2, out_arg1, out_arg2);

  EXPECT_EQ(in_arg1, out_arg1);
  EXPECT_EQ(in_arg2, out_arg2);
}
/*
TEST(ray_marshall, type_float)
{
    float in_arg1 = 1.1, out_arg1;
    float in_arg2 = 2.22, out_arg2;

    ::ray::blob blob;
    //0 args
    //marshall
    ::ray::binary_writer writer0;
    Arguments::wrap(writer0);
    blob = writer0.get_buffer();
    //unmarshall
    ::ray::binary_reader reader0(blob);
    Arguments::unwrap(reader0);

    //1 args
    //marshall
    ::ray::binary_writer writer1;
    Arguments::wrap(writer1, in_arg1);
    blob = writer1.get_buffer();
    //unmarshall
    ::ray::binary_reader reader1(blob);
    Arguments::unwrap(reader1, out_arg1);

    EXPECT_EQ(in_arg1, out_arg1);

    //2 args
    //marshall
    ::ray::binary_writer writer2;
    Arguments::wrap(writer2, in_arg1, in_arg2);
    blob = writer2.get_buffer();
    //unmarshall
    ::ray::binary_reader reader2(blob);
    Arguments::unwrap(reader2, out_arg1, out_arg2);

    EXPECT_EQ(in_arg1, out_arg1);
    EXPECT_EQ(in_arg2, out_arg2);
}*/

/*
TEST(ray_marshall, type_double)
{
    double in_arg1 = 1.1111, out_arg1;
    double in_arg2 = 2.22222, out_arg2;

    ::ray::blob blob;
    //0 args
    //marshall
    ::ray::binary_writer writer0;
    Arguments::wrap(writer0);
    blob = writer0.get_buffer();
    //unmarshall
    ::ray::binary_reader reader0(blob);
    Arguments::unwrap(reader0);

    //1 args
    //marshall
    ::ray::binary_writer writer1;
    Arguments::wrap(writer1, in_arg1);
    blob = writer1.get_buffer();
    //unmarshall
    ::ray::binary_reader reader1(blob);
    Arguments::unwrap(reader1, out_arg1);

    EXPECT_EQ(in_arg1, out_arg1);

    //2 args
    //marshall
    ::ray::binary_writer writer2;
    Arguments::wrap(writer2, in_arg1, in_arg2);
    blob = writer2.get_buffer();
    //unmarshall
    ::ray::binary_reader reader2(blob);
    Arguments::unwrap(reader2, out_arg1, out_arg2);

    EXPECT_EQ(in_arg1, out_arg1);
    EXPECT_EQ(in_arg2, out_arg2);
}*/

TEST(ray_marshall, type_string) {
  std::string in_arg1 = "abcDE", out_arg1;
  std::string in_arg2 = "123567ABC", out_arg2;

  ::ray::blob blob;
  // 0 args
  // marshall
  ::ray::binary_writer writer0;
  Arguments::wrap(writer0);
  blob = writer0.get_buffer();
  // unmarshall
  ::ray::binary_reader reader0(blob);
  Arguments::unwrap(reader0);

  // 1 args
  // marshall
  ::ray::binary_writer writer1;
  Arguments::wrap(writer1, in_arg1);
  blob = writer1.get_buffer();
  // unmarshall
  ::ray::binary_reader reader1(blob);
  Arguments::unwrap(reader1, out_arg1);

  EXPECT_EQ(in_arg1, out_arg1);

  // 2 args
  // marshall
  ::ray::binary_writer writer2;
  Arguments::wrap(writer2, in_arg1, in_arg2);
  blob = writer2.get_buffer();
  // unmarshall
  ::ray::binary_reader reader2(blob);
  Arguments::unwrap(reader2, out_arg1, out_arg2);

  EXPECT_EQ(in_arg1, out_arg1);
  EXPECT_EQ(in_arg2, out_arg2);
}

TEST(ray_marshall, type_hybrid) {
  uint32_t in_arg1 = 123456789, out_arg1;
  std::string in_arg2 = "123567ABC", out_arg2;

  ::ray::blob blob;
  // 0 args
  // marshall
  ::ray::binary_writer writer0;
  Arguments::wrap(writer0);
  blob = writer0.get_buffer();
  // unmarshall
  ::ray::binary_reader reader0(blob);
  Arguments::unwrap(reader0);

  // 1 args
  // marshall
  ::ray::binary_writer writer1;
  Arguments::wrap(writer1, in_arg1);
  blob = writer1.get_buffer();
  // unmarshall
  ::ray::binary_reader reader1(blob);
  Arguments::unwrap(reader1, out_arg1);

  EXPECT_EQ(in_arg1, out_arg1);

  // 2 args
  // marshall
  ::ray::binary_writer writer2;
  Arguments::wrap(writer2, in_arg1, in_arg2);
  blob = writer2.get_buffer();
  // unmarshall
  ::ray::binary_reader reader2(blob);
  Arguments::unwrap(reader2, out_arg1, out_arg2);

  EXPECT_EQ(in_arg1, out_arg1);
  EXPECT_EQ(in_arg2, out_arg2);
}

TEST(ray_marshall, type_ray_object) {
  uint32_t in_arg1 = 123456789, out_arg1;
  std::string in_arg2 = "123567ABC", out_arg2;
  UniqueId uid1;
  uid1.random();
  RayObject<uint32_t> in_arg3(uid1);
  RayObject<uint32_t> out_arg3;

  UniqueId uid2;
  uid2.random();
  RayObject<std::string> in_arg4(uid2);
  RayObject<std::string> out_arg4;

  ::ray::blob blob;
  // 0 args
  // marshall
  ::ray::binary_writer writer0;
  Arguments::wrap(writer0);
  blob = writer0.get_buffer();
  // unmarshall
  ::ray::binary_reader reader0(blob);
  Arguments::unwrap(reader0);

  // 1 args
  // marshall
  ::ray::binary_writer writer1;
  Arguments::wrap(writer1, in_arg3);
  blob = writer1.get_buffer();
  // unmarshall
  ::ray::binary_reader reader1(blob);
  Arguments::unwrap(reader1, out_arg3);

  EXPECT_EQ(in_arg3, out_arg3);

  // 2 args
  // marshall
  ::ray::binary_writer writer2;
  Arguments::wrap(writer2, in_arg1, in_arg3);
  blob = writer2.get_buffer();
  // unmarshall
  ::ray::binary_reader reader2(blob);
  Arguments::unwrap(reader2, out_arg1, out_arg3);

  EXPECT_EQ(in_arg1, out_arg1);
  EXPECT_EQ(in_arg3, out_arg3);

  // 2 args
  // marshall
  ::ray::binary_writer writer3;
  Arguments::wrap(writer3, in_arg4, in_arg2);
  blob = writer3.get_buffer();
  // unmarshall
  ::ray::binary_reader reader3(blob);
  Arguments::unwrap(reader3, out_arg4, out_arg2);

  EXPECT_EQ(in_arg4, out_arg4);
  EXPECT_EQ(in_arg2, out_arg2);

  // 2 args
  // marshall
  ::ray::binary_writer writer4;
  Arguments::wrap(writer4, in_arg3, in_arg4);
  blob = writer4.get_buffer();
  // unmarshall
  ::ray::binary_reader reader4(blob);
  Arguments::unwrap(reader4, out_arg3, out_arg4);

  EXPECT_EQ(in_arg3, out_arg3);
  EXPECT_EQ(in_arg4, out_arg4);
}
