/**
 * @brief This is a tutorial to explain how IWYU perform analysis
 * 1. "ray/common/ray_object.h" and <unordered_map> are included but not used in this
 * file. IWYU will suggest removing it.
 * 2. `ray::UniqueID` is used but its header "ray/common/id.h" is missing. IWYU will
 * suggest adding this header.
 * 3. <iostream> and  are used correctly and will remain unchanged.
 * 4. The result will be output to the console and
 * bazel-bin/ci/iwyu/iwyu_test.iwyu_test.cc.iwyu.txt
 */

#include <iostream>
#include <unordered_map>  // IWYU will suggest removing this.

#include "ray/common/ray_object.h"  // IWYU will suggest removing this.

int main() {
  ray::UniqueID id = ray::UniqueID::FromRandom();  // Missing include for ray/common/id.h,
                                                   // IWYU will suggest adding this.
  std::cout << " ID: " << id << std::endl;
  return 0;
}
