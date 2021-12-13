#include <Python.h>
#include <frameobject.h>

#ifdef __GNUC__
#define UNUSED(name) name __attribute__((unused))
#else
#define UNUSED(name) name
#endif

#define EXC_TYPE_REF(frame) (((PyGenObject*)((frame)->f_gen))->gi_exc_state.exc_type)
#define EXC_VALUE_REF(frame) (((PyGenObject*)((frame)->f_gen))->gi_exc_state.exc_value)
#define EXC_TRACEBACK_REF(frame) \
  (((PyGenObject*)((frame)->f_gen))->gi_exc_state.exc_traceback)


static PyObject* unset_value_repr(PyObject* UNUSED(self)) {
    return PyUnicode_FromString("unset_value");
}

static void unset_value_dealloc(PyObject* UNUSED(self)) {
    /* this should never get called, but we also don't want to SEGV if
       we accidentally decref unset_value out of existence */
    Py_FatalError("deallocating unset_value");
}

PyMethodDef unset_value_methods[] = {
    {"__reduce__", (PyCFunction) unset_value_repr, METH_NOARGS, NULL},
    {NULL},
};

PyObject* get_module(PyObject* UNUSED(self), void* UNUSED(arg)) {
  return PyUnicode_FromString("cloudpickle_generators._core");
}

PyGetSetDef unset_value_getsets[] = {
    {"__module__", get_module, NULL, NULL, NULL},
    {NULL},
};

static PyTypeObject unset_value_type = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    "cloudpickle_generators._core.unset_value_type",  /* tp_name */
    sizeof(PyObject),                                 /* tp_basicsize */
    0,                                                /* tp_itemsize */
    unset_value_dealloc,                              /* tp_dealloc */
    0,                                                /* tp_print */
    0,                                                /* tp_getattr */
    0,                                                /* tp_setattr */
    0,                                                /* tp_reserved */
    unset_value_repr,                                 /* tp_repr */
    0,                                                /* tp_as_number */
    0,                                                /* tp_as_sequence */
    0,                                                /* tp_as_mapping */
    0,                                                /* tp_hash */
    0,                                                /* tp_call */
    0,                                                /* tp_str */
    0,                                                /* tp_getattro */
    0,                                                /* tp_setattro */
    0,                                                /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                               /* tp_flags */
    0,                                                /* tp_doc */
    0,                                                /* tp_traverse */
    0,                                                /* tp_clear */
    0,                                                /* tp_richcompare */
    0,                                                /* tp_weaklistoffset */
    0,                                                /* tp_coroutine */
    0,                                                /* tp_coroutinenext */
    unset_value_methods,                              /* tp_methods */
    0,                                                /* tp_members */
    unset_value_getsets,                              /* tp_getset */
};

PyObject unset_value = {
    _PyObject_EXTRA_INIT
    1,
    &unset_value_type,
};

typedef struct {
    PyObject tb_ob;
    PyTryBlock tb_block;
} try_block_object;

PyTypeObject try_block_type;

static PyObject* make_try_block(int type, int handler, int level) {
    try_block_object* self;

    if (!(self = PyObject_New(try_block_object, &try_block_type))) {
        return NULL;
    }

    self->tb_block.b_type = type;
    self->tb_block.b_handler = handler;
    self->tb_block.b_level = level;

    return (PyObject*) self;
}

static PyObject* wrap_try_block(PyTryBlock* block) {
    return make_try_block(block->b_type,
                          block->b_handler,
                          block->b_level);
}

static PyObject*
try_block_new(PyTypeObject* UNUSED(cls), PyObject* args, PyObject* kwargs) {
    static char* keywords[] = {"type", "handler", "level", NULL};

    int type;
    int handler;
    int level;

    if (!PyArg_ParseTupleAndKeywords(args,
                                     kwargs,
                                     "iii:try_block",
                                     keywords,
                                     &type,
                                     &handler,
                                     &level)) {
        return NULL;
    }

    return make_try_block(type, handler, level);
}

static PyObject* try_block_getnewargs(try_block_object* self) {
    return Py_BuildValue("iii",
                         self->tb_block.b_type,
                         self->tb_block.b_handler,
                         self->tb_block.b_level);
}

PyMethodDef try_block_methods[] = {
    {"__getnewargs__", (PyCFunction) try_block_getnewargs, METH_NOARGS, NULL},
    {NULL},
};

PyTypeObject try_block_type = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    "cloudpickle_generators._core.try_block",   /* tp_name */
    sizeof(try_block_object),                   /* tp_basicsize */
    0,                                          /* tp_itemsize */
    0,                                          /* tp_dealloc */
    0,                                          /* tp_print */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_reserved */
    0,                                          /* tp_repr */
    0,                                          /* tp_as_number */
    0,                                          /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    0,                                          /* tp_hash */
    0,                                          /* tp_call */
    0,                                          /* tp_str */
    0,                                          /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                         /* tp_flags */
    0,                                          /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    try_block_methods,                          /* tp_methods */
    0,                                          /* tp_members */
    0,                                          /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    0,                                          /* tp_init */
    0,                                          /* tp_alloc */
    try_block_new,                              /* tp_new */
};

static PyObject*
private_frame_data(PyObject* UNUSED(self), PyObject* frame_ob) {
    PyFrameObject* frame;
    size_t ix;
    size_t size;
    PyObject* stack = NULL;
    PyObject* block_stack = NULL;
    PyObject* exc_info = NULL;
    PyObject* ret;

    if (!PyFrame_Check(frame_ob)) {
        PyErr_SetString(PyExc_TypeError, "frame is not a frame object");
        return NULL;
    }

    frame = (PyFrameObject*) frame_ob;
    size = frame->f_stacktop - frame->f_valuestack;

    if (!(stack = PyTuple_New(size))) {
        return NULL;
    }

    for (ix = 0; ix < size; ++ix) {
        PyObject* ob = frame->f_valuestack[ix];
        if (ob == NULL) {
            ob = &unset_value;
        }
        Py_INCREF(ob);
        PyTuple_SET_ITEM(stack, ix, ob);
    }

    if (!(block_stack = PyTuple_New(frame->f_iblock))) {
        goto error;
    }

    for (ix = 0; (int) ix < frame->f_iblock; ++ix) {
        PyObject* block = wrap_try_block(&frame->f_blockstack[ix]);
        if (!block) {
            goto error;
        }
        PyTuple_SET_ITEM(block_stack, ix, block);
    }

    if (!EXC_TYPE_REF(frame) || (EXC_TYPE_REF(frame) == Py_None)) {
        if (EXC_VALUE_REF(frame) || EXC_TRACEBACK_REF(frame)) {
            PyErr_SetString(PyExc_AssertionError,
                            "the exception type was null but found non-null"
                            " value or traceback");
            goto error;
        }

        exc_info = Py_None;
        Py_INCREF(Py_None);
    }
    else if (!(exc_info = PyTuple_Pack(3,
                                       EXC_TYPE_REF(frame),
                                       EXC_VALUE_REF(frame),
                                       EXC_TRACEBACK_REF(frame)))) {
        goto error;
    }

    if (!(ret = PyTuple_New(3))) {
        goto error;
    }

    PyTuple_SET_ITEM(ret, 0, stack);
    PyTuple_SET_ITEM(ret, 1, block_stack);
    PyTuple_SET_ITEM(ret, 2, exc_info);

    return ret;

error:
    Py_XDECREF(stack);
    Py_XDECREF(block_stack);
    Py_XDECREF(exc_info);

    return NULL;
}

static PyObject*
restore_frame(PyObject* UNUSED(self), PyObject* args, PyObject* kwargs) {
    static char* keywords[] = {"frame",
                               "lasti",
                               "locals",
                               "stack",
                               "block_stack",
                               "exc_info",
                               NULL};

    PyFrameObject* frame;
    int lasti;
    PyObject* locals;
    PyObject* stack;
    PyObject* block_stack;
    PyObject* exc_info;

    Py_ssize_t ix;

    if (!PyArg_ParseTupleAndKeywords(args,
                                     kwargs,
                                     "O!iO!O!O!O:restore_frame",
                                     keywords,
                                     &PyFrame_Type,
                                     &frame,
                                     &lasti,
                                     &PyList_Type,
                                     &locals,
                                     &PyTuple_Type,
                                     &stack,
                                     &PyTuple_Type,
                                     &block_stack,
                                     &exc_info)) {
        return NULL;
    }

    /* set the lasti to move the generator's instruction pointer */
    frame->f_lasti = lasti;

    /* restore the local variable state */
    for (ix = 0; ix < PyList_Size(locals); ++ix) {
        PyObject* ob = PyList_GET_ITEM(locals, ix);
        if (ob != &unset_value) {
            Py_INCREF(ob);
            frame->f_localsplus[ix] = ob;
        }
    }

    /* restore the data stack state */
    for (ix = 0; ix < PyTuple_Size(stack); ++ix) {
        PyObject* ob = PyTuple_GET_ITEM(stack, ix);
        if (ob == &unset_value) {
            ob = NULL;
        }
        Py_XINCREF(ob);
        *frame->f_stacktop++ = ob;
    }

    /* restore the block stack (exceptions and loops) state */
    for (ix = 0; ix < PyTuple_Size(block_stack); ++ix) {
        try_block_object* block =
            (try_block_object*) PyTuple_GET_ITEM(block_stack, ix);
        PyFrame_BlockSetup(frame,
                           block->tb_block.b_type,
                           block->tb_block.b_handler,
                           block->tb_block.b_level);
    }

    /* exc_info is either a tuple of (type, value, traceback) or None which
       indicates that no exception is being held */
    if (PyTuple_CheckExact(exc_info)) {
        if (PyTuple_Size(exc_info) != 3) {
            PyErr_Format(PyExc_ValueError,
                         "exc_info must either be None or a 3-tuple,"
                         " got tuple of length: %ld",
                         PyTuple_Size(exc_info));
            return NULL;
        }

        EXC_TYPE_REF(frame) = PyTuple_GET_ITEM(exc_info, 0);
        Py_INCREF(EXC_TYPE_REF(frame));

        EXC_VALUE_REF(frame) = PyTuple_GET_ITEM(exc_info, 1);
        Py_INCREF(EXC_VALUE_REF(frame));

        EXC_TRACEBACK_REF(frame) = PyTuple_GET_ITEM(exc_info, 2);
        Py_INCREF(EXC_TRACEBACK_REF(frame));
    }
    else if (exc_info != Py_None) {
        PyErr_Format(PyExc_ValueError,
                     "exc_info must either be None or a 3-tuple,  got %R",
                     exc_info);
        return NULL;
    }

    Py_RETURN_NONE;
}

PyMethodDef _core_methods[] = {
    {"private_frame_data",
     (PyCFunction) private_frame_data,
     METH_O,
     NULL},
    {"restore_frame",
     (PyCFunction) restore_frame,
     METH_VARARGS | METH_KEYWORDS,
     NULL},
    {NULL},
};

PyModuleDef _core_module = {
    PyModuleDef_HEAD_INIT,
    "_core",
    NULL,
    -1,
    _core_methods,
    NULL,
    NULL,
    NULL,
    NULL
};


PyMODINIT_FUNC
PyInit__core(void) {
    PyObject* m;

    if (PyType_Ready(&try_block_type) < 0) {
        return NULL;
    }

    if (PyType_Ready(&unset_value_type) < 0) {
        return NULL;
    }

    if (!(m = PyModule_Create(&_core_module))) {
        return NULL;
    }

    if (PyObject_SetAttrString(m, "try_block", (PyObject*) &try_block_type)) {
        Py_DECREF(m);
        return NULL;
    }

    if (PyObject_SetAttrString(m,
                               "unset_value",
                               (PyObject*) &unset_value)) {
        Py_DECREF(m);
        return NULL;
    }

    return m;
}
