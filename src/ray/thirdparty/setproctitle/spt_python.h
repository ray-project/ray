/*-------------------------------------------------------------------------
 *
 * spt_python.h
 *    Include and customize Python definitions.
 *
 * Copyright (c) 2010-2020 Daniele Varrazzo <daniele.varrazzo@gmail.com>
 *
 *-------------------------------------------------------------------------
 */

#ifndef SPT_PYTHON_H
#define SPT_PYTHON_H

#define PY_SSIZE_T_CLEAN

#include <Python.h>

/* Things change a lot here... */
#if PY_MAJOR_VERSION >= 3
#define IS_PY3K
#endif

/* Detect pypy */
#ifdef PYPY_VERSION
#define IS_PYPY
#endif

/* The type returned by Py_GetArgcArgv */
#ifdef IS_PY3K
typedef wchar_t argv_t;
#else
typedef char argv_t;
#endif

/* defined in Modules/main.c but not publically declared */
void Py_GetArgcArgv(int *argc, argv_t ***argv);

/* Mangle the module name into the name of the module init function */
#ifdef IS_PY3K
#define INIT_MODULE(m) PyInit_ ## m
#else
#define INIT_MODULE(m) init ## m
#endif

/* Py2/3 compatibility layer */

#ifdef IS_PY3K

#define PyInt_AsLong           PyLong_AsLong

#define Bytes_Size PyBytes_Size
#define Bytes_AsString PyBytes_AsString

#else   /* Python 2 */

#define Bytes_Size PyString_Size
#define Bytes_AsString PyString_AsString

#if PY_VERSION_HEX < 0x02050000 && !defined(PY_SSIZE_T_MIN)
typedef int Py_ssize_t;
#endif

#endif  /* IS_PY3K > 2 */

#endif   /* SPT_PYTHON_H */
