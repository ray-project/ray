
#include "plus.h"

int Return1() { return 1; };
int Plus1(int x) { return x + 1; };
int Plus(int x, int y) { return x + y; };

RAY_REMOTE(Return1, Plus1, Plus);
