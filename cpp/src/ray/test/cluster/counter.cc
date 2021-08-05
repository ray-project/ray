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

#include "counter.h"

Counter::Counter(int init) { count = init; }

Counter *Counter::FactoryCreate() { return new Counter(0); }

Counter *Counter::FactoryCreate(int init) { return new Counter(init); }

Counter *Counter::FactoryCreate(int init1, int init2) {
  return new Counter(init1 + init2);
}

int Counter::Plus1() {
  count += 1;
  return count;
}

int Counter::Add(int x) {
  count += x;
  return count;
}

RAY_REMOTE(RAY_FUNC(Counter::FactoryCreate), RAY_FUNC(Counter::FactoryCreate, int),
           RAY_FUNC(Counter::FactoryCreate, int, int), &Counter::Plus1, &Counter::Add);
