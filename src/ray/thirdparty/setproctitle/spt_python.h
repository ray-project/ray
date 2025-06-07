/*-------------------------------------------------------------------------
 *
 * spt_python.h
 *    Include and customize Python definitions.
 *
 * Copyright (c) 2010-2021 Daniele Varrazzo <daniele.varrazzo@gmail.com>
 *
 *-------------------------------------------------------------------------
 */

#ifndef SPT_PYTHON_H
#define SPT_PYTHON_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>

/* Detect pypy */
#ifdef PYPY_VERSION
#define IS_PYPY
#endif

#ifndef __darwin__
/* defined in Modules/main.c but not publically declared */
void Py_GetArgcArgv(int *argc, wchar_t ***argv);
#endif

#endif   /* SPT_PYTHON_H */
