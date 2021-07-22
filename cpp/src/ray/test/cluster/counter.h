
#pragma once

#include <ray/api.h>

/// a class of user code
class Counter {
 public:
  int count;
  Counter(int init);
  static Counter *FactoryCreate();
  static Counter *FactoryCreate(int init);
  static Counter *FactoryCreate(int init1, int init2);

  int Plus1();
  int Add(int x);
};