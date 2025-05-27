#include "example.h"

int SumMapValues(const std::unordered_map<int, int> &map) {
  int sum = 0;
  for (const auto &[_, val] : map) {
    sum += val;
  }
  return sum;
}
