// This is to avoid depending on the debug version of the Python library on Windows,
// which requires separate library files and linking.
#if defined(_WIN32) && defined(_DEBUG)
#ifdef Py_PYTHON_H
#error Python.h should not have been included at this point.
#endif
// Ensure some headers included before messing with macro
#include <io.h>
#include <stdio.h>
#ifdef Py_CONFIG_H
#error pyconfig.h should not have been included at this point.
#endif
#include "patchlevel.h"
#pragma push_macro("_DEBUG")
#undef _DEBUG
#include "pyconfig.h"
#pragma pop_macro("_DEBUG")
#endif
