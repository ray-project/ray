/*-------------------------------------------------------------------------
 *
 * spt_setup.c
 *    Initalization code for the spt_status.c module functions.
 *
 * Copyright (c) 2009-2021 Daniele Varrazzo <daniele.varrazzo@gmail.com>
 *
 *-------------------------------------------------------------------------
 */

#include "spt_setup.h"

#include "spt.h"
#include "spt_status.h"

#include <string.h>

/* Darwin doesn't export environ */
#if defined(__darwin__)
#include <crt_externs.h>
#define environ (*_NSGetEnviron())
#elif !defined(_WIN32)
extern char **environ;
#endif

#ifndef WIN32

/* Return a concatenated version of a strings vector.
 *
 * Return newly allocated heap space: clean it up with free().
 *
 * Return NULL and raise an exception on error.
 */
static char *
join_argv(int argc, char **argv)
{
    int i;
    size_t len = 0;
    char *buf;
    char *src;
    char *dest;

    /* Calculate the final string length */
    for (i = 0; i < argc; i++) {
        len += strlen(argv[i]) + 1;
    }

    if (!(dest = buf = (char *)malloc(len))) {
        PyErr_NoMemory();
        return NULL;
    }

    /* Copy the strings in the buffer joining with spaces */
    for (i = 0; i < argc; i++) {
        src = argv[i];
        while (*src) {
            *dest++ = *src++;
        }
        *dest++ = ' ';
    }
    *--dest = '\x00';

    return buf;
}

#ifndef __darwin__

/* I don't expect it to be defined: should include limits.h. But then it's
 * another of those ./configure can of worms to find where it is... */
#ifndef ARG_MAX
#define ARG_MAX (96 * 1024)
#endif


/* Return a copy of argv[0] encoded in the default encoding.
 *
 * Return a newly allocated buffer to be released with free().
 *
 * Return NULL in case of error. If the error shouldn't be ignored, also set
 * a Python exception.
 */
static char *
get_encoded_arg0(wchar_t *argv0)
{
    PyObject *ua = NULL, *ba = NULL;
    char *rv = NULL;

    if (!(ua = PyUnicode_FromWideChar(argv0, -1))) {
        spt_debug("failed to convert argv[0] to unicode");
        PyErr_Clear();
        goto exit;
    }

    if (!(ba = PyUnicode_AsEncodedString(
            ua, PyUnicode_GetDefaultEncoding(), "strict"))) {
        spt_debug("failed to encode argv[0]");
        PyErr_Clear();
        goto exit;
    }

    if (!(rv = strdup(PyBytes_AsString(ba)))) {
        PyErr_NoMemory();
    }

exit:
    Py_XDECREF(ua);
    Py_XDECREF(ba);

    return rv;
}


/* Find the original arg buffer starting from the env position.
 *
 * Return a malloc'd argv vector, pointing to the original arguments.
 *
 * Return NULL in case of error. If the error shouldn't be ignored, also set
 * a Python exception.
 *
 * Required on Python 3 as Py_GetArgcArgv doesn't return pointers to the
 * original area. It can be used on Python 2 too in case we can't get argv,
 * such as in embedded environment.
 */
static char **
find_argv_from_env(int argc, char *arg0)
{
    int i;
    char **buf = NULL;
    char **rv = NULL;
    char *ptr;
    char *limit;

    spt_debug("walking from environ to look for the arguments");

    if (!(buf = (char **)malloc((argc + 1) * sizeof(char *)))) {
        spt_debug("can't malloc %d args!", argc);
        PyErr_NoMemory();
        goto exit;
    }
    buf[argc] = NULL;

    /* Walk back from environ until you find argc-1 null-terminated strings.
     * Don't look for argv[0] as it's probably not preceded by 0. */
    ptr = environ[0];
    if (!ptr) {
        /* It happens on os.environ.clear() */
        spt_debug("environ pointer is NULL");
        goto exit;
    }
    spt_debug("found environ at %p", ptr);
    limit = ptr - ARG_MAX;
    --ptr;
    for (i = argc - 1; i >= 1; --i) {
        if (*ptr) {
            spt_debug("zero %d not found", i);
            goto exit;
        }
        --ptr;
        while (*ptr && ptr > limit) { --ptr; }
        if (ptr <= limit) {
            spt_debug("failed to found arg %d start", i);
            goto exit;
        }
        buf[i] = (ptr + 1);
        spt_debug("found argv[%d] at %p: %s", i, buf[i], buf[i]);
    }

    /* The first arg has not a zero in front. But what we have is reliable
     * enough (modulo its encoding). Check if it is exactly what found.
     *
     * The check is known to fail on OS X with locale C if there are
     * non-ascii characters in the executable path. See Python issue #9167
     */
    ptr -= strlen(arg0);
    spt_debug("argv[0] should be at %p", ptr);

    if (ptr <= limit) {
        spt_debug("failed to find argv[0] start");
        goto exit;
    }
    if (strcmp(ptr, arg0)) {
        spt_debug("argv[0] '%s' doesn't match '%s'", ptr, arg0);
        goto exit;
    }

    /* We have all the pieces of the jigsaw. */
    buf[0] = ptr;
    spt_debug("found argv[0]: %s", buf[0]);
    rv = buf;
    buf = NULL;

exit:
    if (buf) { free(buf); }

    return rv;
}


/* Come on, why is this missing?! this is just cruel!
 * I guess you club seal pups for hobby. */
PyObject *
PyFile_FromString(const char *filename, const char *mode)
{
    PyObject *io = NULL;
    PyObject *rv = NULL;

    if (!(io = PyImport_ImportModule("io"))) {
        spt_debug("failed to import io");
        goto exit;
    }

    rv = PyObject_CallMethod(io, "open", "ss", filename, mode);

exit:
    Py_XDECREF(io);
    return rv;
}

/* Read the number of arguments and the first argument from /proc/pid/cmdline
 *
 * Return 0 if found, else -1. Return arg0 in a malloc'd array.
 *
 * If the function fails in a way that shouldn't be ignored, also set
 * a Python exception.
 */
static int
get_args_from_proc(int *argc_o, char **arg0_o)
{
    /* allow /proc/PID/cmdline, with oversize max_pid, and them some. */
#define FNLEN 30
    char fn[FNLEN];

    PyObject *os = NULL;
    PyObject *pid_py = NULL;
    long pid;
    PyObject *f = NULL;
    PyObject *cl = NULL;

    PyObject *tmp = NULL;
    int rv = -1;

    spt_debug("looking for args into proc fs");

    /* get the pid from os.getpid() */
    if (!(os = PyImport_ImportModule("os"))) {
        spt_debug("failed to import os");
        goto exit;
    }
    if (!(pid_py = PyObject_CallMethod(os, "getpid", NULL))) {
        spt_debug("calling os.getpid() failed");
        /* os.getpid() may be not available, so ignore this error. */
        PyErr_Clear();
        goto exit;
    }
    if (-1 == (pid = PyLong_AsLong(pid_py))) {
        spt_debug("os.getpid() returned crap?");
        /* Don't bother to check PyErr_Occurred as pid can't just be -1. */
        goto exit;
    }

    /* get the content of /proc/PID/cmdline */
    snprintf(fn, FNLEN, "/proc/%ld/cmdline", pid);
    if (!(f = PyFile_FromString(fn, "rb"))) {
        spt_debug("opening '%s' failed", fn);
        /* That's ok: procfs is easily not available on menomated unices */
        PyErr_Clear();
        goto exit;
    }
    /* the file has been open in binary mode, so we get bytes */
    cl = PyObject_CallMethod(f, "read", NULL);
    if (!(tmp = PyObject_CallMethod(f, "close", NULL))) {
        spt_debug("closing failed");
    }
    else {
        Py_DECREF(tmp);
    }

    if (!cl) {
        spt_debug("reading failed");
        /* could there be some protected environment where a process cannot
         * read its own pid? Who knows, better not to risk. */
        PyErr_Clear();
        goto exit;
    }

    /* the cmdline is a buffer of null-terminated strings. We can strdup it to
     * get a copy of arg0, and count the zeros to get argc */
    {
        char *ccl;
        Py_ssize_t i;

        if (!(ccl = PyBytes_AsString(cl))) {
            spt_debug("failed to get cmdline string");
            goto exit;
        }
        if (!(*arg0_o = strdup(ccl))) {
            spt_debug("arg0 strdup failed");
            PyErr_NoMemory();
            goto exit;
        }
        spt_debug("got argv[0] = '%s' from /proc", *arg0_o);

        *argc_o = 0;
        for (i = PyBytes_Size(cl) - 1; i >= 0; --i) {
            if (ccl[i] == '\0') { (*argc_o)++; }
        }
        spt_debug("got argc = %d from /proc", *argc_o);
    }

    /* success */
    rv = 0;

exit:
    Py_XDECREF(cl);
    Py_XDECREF(f);
    Py_XDECREF(pid_py);
    Py_XDECREF(os);

    return rv;
}

/* Find the original arg buffer, return 0 if found, else -1.
 *
 * If found, set argc to the number of arguments, argv to an array
 * of pointers to the single arguments. The array is allocated via malloc.
 *
 * If the function fails in a way that shouldn't be ignored, also set
 * a Python exception.
 *
 * The function overcomes three Py_GetArgcArgv shortcomings:
 * - some python parameters mess up with the original argv, e.g. -m
 *   (see issue #8)
 * - with Python 3, argv is a decoded copy and doesn't point to
 *   the original area.
 * - If python is embedded, the function doesn't return anything.
 */
static int
get_argc_argv(int *argc_o, char ***argv_o)
{
    int argc = 0;
    wchar_t **argv_py = NULL;
    char **argv = NULL;
    char *arg0 = NULL;
    int rv = -1;

#ifndef IS_PYPY
    spt_debug("reading argc/argv from Python main");
    Py_GetArgcArgv(&argc, &argv_py);
#endif

    if (argc > 0) {
        spt_debug("found %d arguments", argc);

        if (!(arg0 = get_encoded_arg0(argv_py[0]))) {
            spt_debug("couldn't get a copy of argv[0]");
            goto exit;
        }
        /* we got argv: on py2 it used to pointsto the right place in memory; on
         * py3 we only got a copy of argv[0]: we will use it to look from env
         */
    }
    else {
        spt_debug("no good news from Py_GetArgcArgv");

        /* get a copy of argv[0] from /proc, so we get back in the same
         * situation of Py3 */
        if (0 > get_args_from_proc(&argc, &arg0)) {
            spt_debug("failed to get args from proc fs");
            goto exit;
        }
    }

    /* If we don't know argv but we know the content of argv[0], we can walk
     * backwards from environ and see if we get it. */
    if (arg0 && !argv) {
        if (!(argv = find_argv_from_env(argc, arg0))) {
            spt_debug("couldn't find argv from environ");
            goto exit;
        }
    }

    /* success */
    *argc_o = argc;
    *argv_o = argv;
    argv = NULL;
    rv = 0;

exit:
    if (arg0) { free(arg0); }
    if (argv) { free(argv); }

    return rv;
}

#else /* __darwin__ */

static int
get_argc_argv(int *argc_o, char ***argv_o)
{
    int * pargc = _NSGetArgc();
    if (!pargc) {
        spt_debug("_NSGetArgc returned NULL");
        return -1;
    }
    int argc = *pargc;
    char *** pargv = _NSGetArgv();
    if (!pargv) {
        spt_debug("_NSGetArgv returned NULL");
        return -1;
    }
    char ** buf = malloc((argc + 1) * sizeof(char *));
    if (!buf) {
        spt_debug("can't malloc %d args!", argc);
        PyErr_NoMemory();
        return -1;
    }
    memcpy(buf, *pargv, argc * sizeof(char *));
    buf[argc] = NULL;
    *argc_o = argc;
    *argv_o = buf;

    return 0;
}

#endif /* __darwin__ */

#endif  /* !WIN32 */


/* Initialize the module internal functions.
 *
 * The function reproduces the initialization performed by PostgreSQL
 * to be able to call the functions in pg_status.c
 *
 * Return 0 in case of success, else -1. In case of failure with an error that
 * shouldn't be ignored, also set a Python exception.
 *
 * The function should be called only once in the process lifetime.
 * so is called at module initialization. After the function is called,
 * set_ps_display() can be used.
 */
int
spt_setup(void)
{
    const int not_happened = 3;
    static int rv = 3;

    /* Make sure setup happens just once, either successful or failed */
    if (rv != not_happened) {
        spt_debug("setup was called more than once!");
        return rv;
    }

    rv = -1;

#ifndef WIN32
    int argc = 0;
    char **argv = NULL;
    char *init_title;

    if (0 > get_argc_argv(&argc, &argv)) {
        spt_debug("get_argc_argv failed");
        goto exit;
    }

    save_ps_display_args(argc, argv);

    /* Set up the first title to fully initialize the code */
    if (!(init_title = join_argv(argc, argv))) { goto exit; }
    init_ps_display(init_title);
    free(init_title);

#else
    /* On Windows save_ps_display_args is a no-op
     * This is a good news, because Py_GetArgcArgv seems not usable.
     */
    LPTSTR init_title = GetCommandLine();
    init_ps_display(init_title);
#endif

    rv = 0;

exit:
    return rv;
}

