#include "greatest.h"

#include "common.h"

SUITE(common_tests);

TEST sha1_test(void) {
  static char hex[ID_STRING_SIZE];
  UniqueID uid = globally_unique_id();
  object_id_to_string((ObjectID) uid, &hex[0], ID_STRING_SIZE);
  PASS();
}

SUITE(common_tests) {
  RUN_TEST(sha1_test);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(common_tests);
  GREATEST_MAIN_END();
}
