// Copyright 2021 The Ray Authors.
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

#include "plus.h"

int Return1() { return 1; };
int Plus1(int x) { return x + 1; };
int Plus(int x, int y) { return x + y; };

std::array<int, 100000> ReturnLargeArray(std::array<int, 100000> x) { return x; };
void ThrowTask() { throw std::logic_error("error"); }

RAY_REMOTE(Return1, Plus1, Plus, ThrowTask, ReturnLargeArray);
