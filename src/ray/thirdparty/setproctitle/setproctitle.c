/*-------------------------------------------------------------------------
 *
 * setproctitle.c
 *    Python extension module to update and read the process title.
 *
 * Copyright (c) 2009-2021 Daniele Varrazzo <daniele.varrazzo@gmail.com>
 *
 * The module allows Python code to access the functions get_ps_display()
 * and set_ps_display().
 *
 *-------------------------------------------------------------------------
 */

#include "spt.h"
#include "spt_setup.h"
#include "spt_status.h"

#ifndef SPT_VERSION
#define SPT_VERSION unknown
#endif

/* macro trick to stringify a macro expansion */
#define xstr(s) str(s)
#define str(s) #s

/* ----------------------------------------------------- */

static char spt_setproctitle__doc__[] =
"setproctitle(title) -- Change the process title."
;

static PyObject *
spt_setproctitle(PyObject *self, PyObject *args, PyObject *kwargs)
{
    const char *title = NULL;
    static char *kwlist[] = {"title", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s", kwlist, &title)) {
        spt_debug("failed to parse tuple and keywords");
        return NULL;
    }

    if (spt_setup() < 0) {
        spt_debug("failed to initialize setproctitle");
    }

    /* Initialize the process title */
    set_ps_display(title, true);

    Py_RETURN_NONE;
}


static char spt_getproctitle__doc__[] =
"getproctitle() -- Get the current process title."
;

static PyObject *
spt_getproctitle(PyObject *self, PyObject *args)
{
    size_t tlen;
    const char *title;

    if (spt_setup() < 0) {
        spt_debug("failed to initialize setproctitle");
    }

    title = get_ps_display(&tlen);

    return Py_BuildValue("s#", title, (int)tlen);
}


static char spt_setthreadtitle__doc__[] =
"setthreadtitle(title) -- Change the thread title."
;

static PyObject *
spt_setthreadtitle(PyObject *self, PyObject *args, PyObject *kwargs)
{
    const char *title = NULL;
    static char *kwlist[] = {"title", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s", kwlist, &title)) {
        spt_debug("failed to parse tuple and keywords");
        return NULL;
    }

    set_thread_title(title);

    Py_RETURN_NONE;
}


static char spt_getthreadtitle__doc__[] =
"getthreadtitle() -- Return the thread title."
;

static PyObject *
spt_getthreadtitle(PyObject *self, PyObject *args)
{
    char title[16] = {'\0'};

    get_thread_title(title);

    return Py_BuildValue("s", title);
}

/* Module initialization function */

static int
spt_exec(PyObject *m)
{
    spt_debug("module init");
    return 0;
}

/* List of slots defined in the module */

static PyModuleDef_Slot spt_slots[] = {
    {Py_mod_exec, spt_exec},
#if PY_VERSION_HEX >= 0x030c0000
    {Py_mod_multiple_interpreters, Py_MOD_PER_INTERPRETER_GIL_SUPPORTED},
#endif
#if PY_VERSION_HEX >= 0x030d0000
    {Py_mod_gil, Py_MOD_GIL_NOT_USED},
#endif
    {0, NULL}
};

/* List of methods defined in the module */

static struct PyMethodDef spt_methods[] = {
    {"setproctitle",
        (PyCFunction)spt_setproctitle,
        METH_VARARGS|METH_KEYWORDS,
        spt_setproctitle__doc__},

    {"getproctitle",
        (PyCFunction)spt_getproctitle,
        METH_NOARGS,
        spt_getproctitle__doc__},

    {"setthreadtitle",
        (PyCFunction)spt_setthreadtitle,
        METH_VARARGS|METH_KEYWORDS,
        spt_setthreadtitle__doc__},

    {"getthreadtitle",
        (PyCFunction)spt_getthreadtitle,
        METH_NOARGS,
        spt_getthreadtitle__doc__},

    {NULL, (PyCFunction)NULL, 0, NULL}        /* sentinel */
};


/* Initialization function for the module (*must* be called initsetproctitle) */

static char setproctitle_module_documentation[] =
"Allow customization of the process title."
;

static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "_setproctitle",
    setproctitle_module_documentation,
    0,
    spt_methods,
    spt_slots,
    NULL,
    NULL,
    NULL
};

PyMODINIT_FUNC
PyInit__setproctitle(void)
{
    return PyModuleDef_Init(&moduledef);
}
