#include "greatest.h"

#include "common.h"

SUITE(common_tests);

TEST sha1_test(void) {
  static char hex[2 * UNIQUE_ID_SIZE + 1];
  unique_id uid = globally_unique_id();
  sha1_to_hex(&uid.id[0], &hex[0]);
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
