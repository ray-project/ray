
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
