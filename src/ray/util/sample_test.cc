#include <vector>

#include "gtest/gtest.h"
#include "ray/util/sample.h"

namespace ray {

class RandomSampleTest : public ::testing::Test {
 protected:
  std::vector<int> *test_vector;
  virtual void SetUp() {
    test_vector = new std::vector<int>();
    for (int i = 0; i < 10; i++) {
      test_vector->push_back(i);
    }
  }

  virtual void TearDown() { delete test_vector; }
};

TEST_F(RandomSampleTest, TestEmpty) {
  std::vector<int> sample = random_sample(test_vector->begin(), test_vector->end(), 0);
  ASSERT_EQ(sample.size(), 0);
}

TEST_F(RandomSampleTest, TestSmallerThanSampleSize) {
  std::vector<int> sample =
      random_sample(test_vector->begin(), test_vector->end(), test_vector->size() + 1);
  ASSERT_EQ(sample.size(), test_vector->size());
}

TEST_F(RandomSampleTest, TestEqualToSampleSize) {
  std::vector<int> sample =
      random_sample(test_vector->begin(), test_vector->end(), test_vector->size());
  ASSERT_EQ(sample.size(), test_vector->size());
}

TEST_F(RandomSampleTest, TestLargerThanSampleSize) {
  std::vector<int> sample =
      random_sample(test_vector->begin(), test_vector->end(), test_vector->size() - 1);
  ASSERT_EQ(sample.size(), test_vector->size() - 1);
}

TEST_F(RandomSampleTest, TestEqualOccurrenceChance) {
  int trials = 100000;
  std::vector<int> occurrences(test_vector->size(), 0);
  for (int i = 0; i < trials; i++) {
    std::vector<int> sample =
        random_sample(test_vector->begin(), test_vector->end(), test_vector->size() / 2);
    for (int idx : sample) {
      occurrences[idx]++;
    }
  }
  for (int count : occurrences) {
    ASSERT_NEAR(trials / 2, count, 0.05 * trials / 2);
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
