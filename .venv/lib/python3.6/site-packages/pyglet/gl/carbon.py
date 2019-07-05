#!/usr/bin/env python

'''
'''
from __future__ import absolute_import

__docformat__ = 'restructuredtext'
__version__ = '$Id: $'

from .base import Config, CanvasConfig, Context

from pyglet.libs.darwin import *
from pyglet.libs.darwin import _oscheck
from pyglet.gl import ContextException
from pyglet.gl import gl
from pyglet.gl import agl

from pyglet.canvas.carbon import CarbonCanvas, CarbonFullScreenCanvas

def _aglcheck():
    err = agl.aglGetError()
    if err != agl.AGL_NO_ERROR:
        raise RuntimeError(cast(agl.aglErrorString(err), c_char_p).value)

class CarbonConfig(Config):
    def match(self, canvas):
        # Construct array of attributes for aglChoosePixelFormat
        attrs = []
        for name, value in self.get_gl_attributes():
            attr = CarbonCanvasConfig._attribute_ids.get(name, None)
            if not attr or not value:
                continue
            attrs.append(attr)
            if attr not in CarbonCanvasConfig._boolean_attributes:
                attrs.append(int(value))

        # Support for RAGE-II, which is not compliant
        attrs.append(agl.AGL_ALL_RENDERERS)

        # Force selection policy and RGBA
        attrs.append(agl.AGL_MAXIMUM_POLICY)
        attrs.append(agl.AGL_RGBA)

        # In 10.3 and later, AGL_FULLSCREEN is specified so the window can
        # be toggled to/from fullscreen without losing context.  pyglet
        # no longer supports earlier versions of OS X, so we always supply it.
        attrs.append(agl.AGL_FULLSCREEN)

        # Terminate the list.
        attrs.append(agl.AGL_NONE)
        attrib_list = (c_int * len(attrs))(*attrs)

        gdevice = cast(canvas.screen.get_gdevice(), agl.GDHandle)
        pformat = agl.aglChoosePixelFormat(gdevice, 1, attrib_list)
        _aglcheck()

        if not pformat:
            return []
        else:
            return [CarbonCanvasConfig(canvas, self, pformat, self)]

class CarbonCanvasConfig(CanvasConfig):
    # Valid names for GL attributes, and their corresponding AGL constant. 
    _attribute_ids = {
        'double_buffer': agl.AGL_DOUBLEBUFFER,
        'stereo': agl.AGL_STEREO,
        'buffer_size': agl.AGL_BUFFER_SIZE, 
        'sample_buffers': agl.AGL_SAMPLE_BUFFERS_ARB,
        'samples': agl.AGL_SAMPLES_ARB,
        'aux_buffers': agl.AGL_AUX_BUFFERS,
        'red_size': agl.AGL_RED_SIZE,
        'green_size': agl.AGL_GREEN_SIZE,
        'blue_size': agl.AGL_BLUE_SIZE,
        'alpha_size': agl.AGL_ALPHA_SIZE,
        'depth_size': agl.AGL_DEPTH_SIZE,
        'stencil_size': agl.AGL_STENCIL_SIZE,
        'accum_red_size': agl.AGL_ACCUM_RED_SIZE,
        'accum_green_size': agl.AGL_ACCUM_GREEN_SIZE,
        'accum_blue_size': agl.AGL_ACCUM_BLUE_SIZE,
        'accum_alpha_size': agl.AGL_ACCUM_ALPHA_SIZE,

        # Not exposed by pyglet API (set internally)
        'all_renderers': agl.AGL_ALL_RENDERERS,
        'rgba': agl.AGL_RGBA,
        'fullscreen': agl.AGL_FULLSCREEN,
        'minimum_policy': agl.AGL_MINIMUM_POLICY,
        'maximum_policy': agl.AGL_MAXIMUM_POLICY,

        # Not supported in current pyglet API
        'level': agl.AGL_LEVEL, 
        'pixel_size': agl.AGL_PIXEL_SIZE,   # == buffer_size
        'aux_depth_stencil': agl.AGL_AUX_DEPTH_STENCIL,
        'color_float': agl.AGL_COLOR_FLOAT,
        'offscreen': agl.AGL_OFFSCREEN,
        'sample_alpha': agl.AGL_SAMPLE_ALPHA,
        'multisample': agl.AGL_MULTISAMPLE,
        'supersample': agl.AGL_SUPERSAMPLE,
    }

    # AGL constants which do not require a value.
    _boolean_attributes = \
        (agl.AGL_ALL_RENDERERS, 
         agl.AGL_RGBA,
         agl.AGL_DOUBLEBUFFER,
         agl.AGL_STEREO,
         agl.AGL_MINIMUM_POLICY,
         agl.AGL_MAXIMUM_POLICY,
         agl.AGL_OFFSCREEN,
         agl.AGL_FULLSCREEN,
         agl.AGL_AUX_DEPTH_STENCIL,
         agl.AGL_COLOR_FLOAT,
         agl.AGL_MULTISAMPLE,
         agl.AGL_SUPERSAMPLE,
         agl.AGL_SAMPLE_ALPHA)

    def __init__(self, canvas, screen, pformat, config):
        super(CarbonCanvasConfig, self).__init__(canvas, config)
        self.screen = screen
        self._pformat = pformat
        self._attributes = {}

        for name, attr in self._attribute_ids.items():
            value = c_int()
            result = agl.aglDescribePixelFormat(pformat, attr, byref(value))
            if result:
                setattr(self, name, value.value)
 
    def create_context(self, share):
        if share:
            context = agl.aglCreateContext(self._pformat, share._context)
        else:
            context = agl.aglCreateContext(self._pformat, None)
        _aglcheck()
        return CarbonContext(self, context, share, self._pformat)

    def compatible(self, canvas):
        return isinstance(canvas, CarbonCanvas) or \
               isinstance(canvas, CarbonFullScreenCanvas)

class CarbonContext(Context):
    def __init__(self, config, context, share, pixelformat):
        super(CarbonContext, self).__init__(share)
        self.config = config
        self._context = context
        self._pixelformat = pixelformat

    def attach(self, canvas):
        if self.config._requires_gl_3():
            raise ContextException('AGL does not support OpenGL 3')

        super(CarbonContext, self).attach(canvas)
        if isinstance(canvas, CarbonFullScreenCanvas):
            # XXX not used any more (cannot use AGL_BUFFER_RECT)   
            agl.aglEnable(self._context, agl.AGL_FS_CAPTURE_SINGLE)
            agl.aglSetFullScreen(self._context, canvas.width, canvas.height,
                                 canvas.screen._refresh_rate, 0)
        else:
            agl.aglSetDrawable(self._context, 
                               cast(canvas.drawable, agl.AGLDrawable))
        agl.aglSetCurrentContext(self._context)
        if canvas.bounds is not None:
            bounds = (gl.GLint * 4)(*canvas.bounds)
            agl.aglSetInteger(self._context, agl.AGL_BUFFER_RECT, bounds)
            agl.aglEnable(self._context, agl.AGL_BUFFER_RECT)
        else:
            agl.aglDisable(self._context, agl.AGL_BUFFER_RECT)
        _aglcheck()

        self.set_current()

    def detach(self):
        super(CarbonContext, self).detach()
        agl.aglSetDrawable(self._context, None)
        _aglcheck()
    
    def set_current(self):
        super(CarbonContext, self).set_current()
        agl.aglSetCurrentContext(self._context)
        _aglcheck()

    def update_geometry(self):
        agl.aglUpdateContext(self._context)
        _aglcheck()

    def destroy(self):
        super(CarbonContext, self).destroy()
        agl.aglDestroyContext(self._context)

    def set_vsync(self, vsync=True):
        swap = c_long(int(vsync))
        agl.aglSetInteger(self._context, agl.AGL_SWAP_INTERVAL, byref(swap))
        _aglcheck()

    def get_vsync(self):
        swap = c_long()
        agl.aglGetInteger(self._context, agl.AGL_SWAP_INTERVAL, byref(swap))
        _aglcheck()
        return bool(swap.value)

    def flip(self):
        agl.aglSwapBuffers(self._context)
        _aglcheck()

