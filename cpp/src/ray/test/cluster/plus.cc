
#include "plus.h"

int Return1() { return 1; };
int Plus1(int x) { return x + 1; };
int Plus(int x, int y) { return x + y; };
void ThrowTask() { throw std::logic_error("error"); }

RAY_REMOTE(Return1, Plus1, Plus, ThrowTask);
