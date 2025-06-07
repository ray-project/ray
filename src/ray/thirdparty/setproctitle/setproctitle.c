/*-------------------------------------------------------------------------
 *
 * setproctitle.c
 *    Python extension module to update and read the process title.
 *
 * Copyright (c) 2009-2020 Daniele Varrazzo <daniele.varrazzo@gmail.com>
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

static PyObject *spt_version;


static char spt_setproctitle__doc__[] =
"setproctitle(title) -- Change the process title."
;

static PyObject *
spt_setproctitle(PyObject *self, PyObject *args, PyObject *kwargs)
{
    const char *title = NULL;
    static char *kwlist[] = {"title", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s", kwlist, &title))
        return NULL;

    /* Initialize the process title */
    if (0 <= spt_setup()) {
        set_ps_display(title, true);
    }
    else {
        spt_debug("failed to initialize setproctitle");
    }

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

    spt_setup();
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

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s", kwlist, &title))
        return NULL;

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

#ifdef IS_PY3K

static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "setproctitle",
    setproctitle_module_documentation,
    -1,
    spt_methods,
    NULL,
    NULL,
    NULL,
    NULL
};

#endif

PyMODINIT_FUNC
INIT_MODULE(setproctitle)(void)
{
    PyObject *m, *d;

    spt_debug("module init");

    /* Create the module and add the functions */
#ifdef IS_PY3K
    m = PyModule_Create(&moduledef);
#else
    m = Py_InitModule3("setproctitle", spt_methods,
        setproctitle_module_documentation);
#endif
    if (m == NULL) { goto exit; }

    /* Add version string to the module*/
    d = PyModule_GetDict(m);
    spt_version = Py_BuildValue("s", xstr(SPT_VERSION));
    PyDict_SetItemString(d, "__version__", spt_version);

exit:

#ifdef IS_PY3K
    return m;
#else
    return;
#endif

}

