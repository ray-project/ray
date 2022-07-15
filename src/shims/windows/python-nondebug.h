// Copyright 2020 The Ray Authors.
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
