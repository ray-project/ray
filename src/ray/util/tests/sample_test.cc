// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/util/sample.h"

#include <vector>

#include "gtest/gtest.h"

namespace ray {

class RandomSampleTest : public ::testing::Test {
 protected:
  std::vector<int> *sample;
  std::vector<int> *test_vector;
  virtual void SetUp() {
    sample = new std::vector<int>();
    test_vector = new std::vector<int>();
    for (int i = 0; i < 10; i++) {
      test_vector->push_back(i);
    }
  }

  virtual void TearDown() {
    delete sample;
    delete test_vector;
  }
};

TEST_F(RandomSampleTest, TestEmpty) {
  random_sample(test_vector->begin(), test_vector->end(), 0, sample);
  ASSERT_EQ(sample->size(), 0);
}

TEST_F(RandomSampleTest, TestSmallerThanSampleSize) {
  random_sample(
      test_vector->begin(), test_vector->end(), test_vector->size() + 1, sample);
  ASSERT_EQ(sample->size(), test_vector->size());
}

TEST_F(RandomSampleTest, TestEqualToSampleSize) {
  random_sample(test_vector->begin(), test_vector->end(), test_vector->size(), sample);
  ASSERT_EQ(sample->size(), test_vector->size());
}

TEST_F(RandomSampleTest, TestLargerThanSampleSize) {
  random_sample(
      test_vector->begin(), test_vector->end(), test_vector->size() - 1, sample);
  ASSERT_EQ(sample->size(), test_vector->size() - 1);
}

TEST_F(RandomSampleTest, TestEqualOccurrenceChance) {
  int trials = 1000000;
  std::vector<int> occurrences(test_vector->size(), 0);
  for (int i = 0; i < trials; i++) {
    random_sample(
        test_vector->begin(), test_vector->end(), test_vector->size() / 2, sample);
    for (int idx : *sample) {
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
