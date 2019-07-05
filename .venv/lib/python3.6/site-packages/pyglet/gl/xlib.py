#!/usr/bin/env python

'''
'''
from __future__ import absolute_import

__docformat__ = 'restructuredtext'
__version__ = '$Id: $'

from ctypes import *

from pyglet.canvas.xlib import XlibCanvas
from .base import Config, CanvasConfig, Context

from pyglet import gl
from pyglet.gl import glx
from pyglet.gl import glxext_arb
from pyglet.gl import glx_info
from pyglet.gl import glxext_mesa


class XlibConfig(Config):
    def match(self, canvas):
        if not isinstance(canvas, XlibCanvas):
            raise RuntimeError('Canvas must be an instance of XlibCanvas')

        x_display = canvas.display._display
        x_screen = canvas.display.x_screen

        info = glx_info.GLXInfo(x_display)
        have_13 = info.have_version(1, 3)
        if have_13:
            config_class = XlibCanvasConfig13
        else:
            if 'ATI' in info.get_client_vendor():
                config_class = XlibCanvasConfig10ATI
            else:
                config_class = XlibCanvasConfig10

        # Construct array of attributes
        attrs = []
        for name, value in self.get_gl_attributes():
            attr = config_class.attribute_ids.get(name, None)
            if attr and value is not None:
                attrs.extend([attr, int(value)])

        if have_13:
            attrs.extend([glx.GLX_X_RENDERABLE, True])
        else:
            attrs.extend([glx.GLX_RGBA, True])

        if len(attrs):
            attrs.extend([0, 0])
            attrib_list = (c_int * len(attrs))(*attrs)
        else:
            attrib_list = None

        if have_13:
            elements = c_int()
            configs = glx.glXChooseFBConfig(x_display, x_screen,
                                            attrib_list, byref(elements))
            if not configs:
                return []

            configs = cast(configs,
                           POINTER(glx.GLXFBConfig * elements.value)).contents

            result = [config_class(canvas, info, c, self) for c in configs]

            # Can't free array until all XlibGLConfig13's are GC'd.  Too much
            # hassle, live with leak. XXX
            # xlib.XFree(configs)

            return result
        else:
            try:
                return [config_class(canvas, info, attrib_list, self)]
            except gl.ContextException:
                return []


class BaseXlibCanvasConfig(CanvasConfig):
    # Common code shared between GLX 1.0 and GLX 1.3 configs.

    attribute_ids = {
        'buffer_size': glx.GLX_BUFFER_SIZE,
        'level': glx.GLX_LEVEL,  # Not supported
        'double_buffer': glx.GLX_DOUBLEBUFFER,
        'stereo': glx.GLX_STEREO,
        'aux_buffers': glx.GLX_AUX_BUFFERS,
        'red_size': glx.GLX_RED_SIZE,
        'green_size': glx.GLX_GREEN_SIZE,
        'blue_size': glx.GLX_BLUE_SIZE,
        'alpha_size': glx.GLX_ALPHA_SIZE,
        'depth_size': glx.GLX_DEPTH_SIZE,
        'stencil_size': glx.GLX_STENCIL_SIZE,
        'accum_red_size': glx.GLX_ACCUM_RED_SIZE,
        'accum_green_size': glx.GLX_ACCUM_GREEN_SIZE,
        'accum_blue_size': glx.GLX_ACCUM_BLUE_SIZE,
        'accum_alpha_size': glx.GLX_ACCUM_ALPHA_SIZE,
    }

    def __init__(self, canvas, glx_info, config):
        super(BaseXlibCanvasConfig, self).__init__(canvas, config)
        self.glx_info = glx_info

    def compatible(self, canvas):
        # TODO check more
        return isinstance(canvas, XlibCanvas)

    def _create_glx_context(self, share):
        raise NotImplementedError('abstract')

    def is_complete(self):
        return True

    def get_visual_info(self):
        raise NotImplementedError('abstract')


class XlibCanvasConfig10(BaseXlibCanvasConfig):
    def __init__(self, canvas, glx_info, attrib_list, config):
        super(XlibCanvasConfig10, self).__init__(canvas, glx_info, config)
        x_display = canvas.display._display
        x_screen = canvas.display.x_screen

        self._visual_info = glx.glXChooseVisual(
            x_display, x_screen, attrib_list)
        if not self._visual_info:
            raise gl.ContextException('No conforming visual exists')

        for name, attr in self.attribute_ids.items():
            value = c_int()
            result = glx.glXGetConfig(
                x_display, self._visual_info, attr, byref(value))
            if result >= 0:
                setattr(self, name, value.value)
        self.sample_buffers = 0
        self.samples = 0

    def get_visual_info(self):
        return self._visual_info.contents

    def create_context(self, share):
        return XlibContext10(self, share)


class XlibCanvasConfig10ATI(XlibCanvasConfig10):
    attribute_ids = BaseXlibCanvasConfig.attribute_ids.copy()
    del attribute_ids['stereo']
    stereo = False


class XlibCanvasConfig13(BaseXlibCanvasConfig):
    attribute_ids = BaseXlibCanvasConfig.attribute_ids.copy()
    attribute_ids.update({
        'sample_buffers': glx.GLX_SAMPLE_BUFFERS,
        'samples': glx.GLX_SAMPLES,

        # Not supported in current pyglet API:
        'render_type': glx.GLX_RENDER_TYPE,
        'config_caveat': glx.GLX_CONFIG_CAVEAT,
        'transparent_type': glx.GLX_TRANSPARENT_TYPE,
        'transparent_index_value': glx.GLX_TRANSPARENT_INDEX_VALUE,
        'transparent_red_value': glx.GLX_TRANSPARENT_RED_VALUE,
        'transparent_green_value': glx.GLX_TRANSPARENT_GREEN_VALUE,
        'transparent_blue_value': glx.GLX_TRANSPARENT_BLUE_VALUE,
        'transparent_alpha_value': glx.GLX_TRANSPARENT_ALPHA_VALUE,

        # Used internally
        'x_renderable': glx.GLX_X_RENDERABLE,
    })

    def __init__(self, canvas, glx_info, fbconfig, config):
        super(XlibCanvasConfig13, self).__init__(canvas, glx_info, config)
        x_display = canvas.display._display

        self._fbconfig = fbconfig
        for name, attr in self.attribute_ids.items():
            value = c_int()
            result = glx.glXGetFBConfigAttrib(
                x_display, self._fbconfig, attr, byref(value))
            if result >= 0:
                setattr(self, name, value.value)

    def get_visual_info(self):
        return glx.glXGetVisualFromFBConfig(self.canvas.display._display, self._fbconfig).contents

    def create_context(self, share):
        if self.glx_info.have_extension('GLX_ARB_create_context'):
            return XlibContextARB(self, share)
        else:
            return XlibContext13(self, share)


class BaseXlibContext(Context):
    def __init__(self, config, share):
        super(BaseXlibContext, self).__init__(config, share)

        self.x_display = config.canvas.display._display

        self.glx_context = self._create_glx_context(share)
        if not self.glx_context:
            # TODO: Check Xlib error generated
            raise gl.ContextException('Could not create GL context')

        self._have_SGI_video_sync = config.glx_info.have_extension('GLX_SGI_video_sync')
        self._have_SGI_swap_control = config.glx_info.have_extension('GLX_SGI_swap_control')
        self._have_MESA_swap_control = config.glx_info.have_extension('GLX_MESA_swap_control')

        # In order of preference:
        # 1. GLX_MESA_swap_control (more likely to work where video_sync will
        #    not)
        # 2. GLX_SGI_video_sync (does not work on Intel 945GM, but that has
        #    MESA)
        # 3. GLX_SGI_swap_control (cannot be disabled once enabled).
        self._use_video_sync = (self._have_SGI_video_sync and not self._have_MESA_swap_control)

        # XXX mandate that vsync defaults on across all platforms.
        self._vsync = True

    def is_direct(self):
        return glx.glXIsDirect(self.x_display, self.glx_context)

    def set_vsync(self, vsync=True):
        self._vsync = vsync
        interval = vsync and 1 or 0
        if not self._use_video_sync and self._have_MESA_swap_control:
            glxext_mesa.glXSwapIntervalMESA(interval)
        elif self._have_SGI_swap_control:
            glxext_arb.glXSwapIntervalSGI(interval)

    def get_vsync(self):
        return self._vsync

    def _wait_vsync(self):
        if self._vsync and self._have_SGI_video_sync and self._use_video_sync:
            count = c_uint()
            glxext_arb.glXGetVideoSyncSGI(byref(count))
            glxext_arb.glXWaitVideoSyncSGI(2, (count.value + 1) % 2, byref(count))


class XlibContext10(BaseXlibContext):
    def __init__(self, config, share):
        super(XlibContext10, self).__init__(config, share)

    def _create_glx_context(self, share):
        if self.config._requires_gl_3():
            raise gl.ContextException(
                'Require GLX_ARB_create_context extension to create ' +
                'OpenGL 3 contexts.')

        if share:
            share_context = share.glx_context
        else:
            share_context = None

        return glx.glXCreateContext(self.config.canvas.display._display,
                                    self.config._visual_info, share_context, True)

    def attach(self, canvas):
        super(XlibContext10, self).attach(canvas)

        self.set_current()

    def set_current(self):
        glx.glXMakeCurrent(self.x_display, self.canvas.x_window,
                           self.glx_context)
        super(XlibContext10, self).set_current()

    def detach(self):
        if not self.canvas:
            return

        self.set_current()
        gl.glFlush()
        glx.glXMakeCurrent(self.x_display, 0, None)
        super(XlibContext10, self).detach()

    def destroy(self):
        super(XlibContext10, self).destroy()

        glx.glXDestroyContext(self.x_display, self.glx_context)
        self.glx_context = None

    def flip(self):
        if not self.canvas:
            return

        if self._vsync:
            self._wait_vsync()
        glx.glXSwapBuffers(self.x_display, self.canvas.x_window)


class XlibContext13(BaseXlibContext):
    def __init__(self, config, share):
        super(XlibContext13, self).__init__(config, share)
        self.glx_window = None

    def _create_glx_context(self, share):
        if self.config._requires_gl_3():
            raise gl.ContextException(
                'Require GLX_ARB_create_context extension to create ' +
                'OpenGL 3 contexts.')

        if share:
            share_context = share.glx_context
        else:
            share_context = None

        return glx.glXCreateNewContext(self.config.canvas.display._display,
                                       self.config._fbconfig, glx.GLX_RGBA_TYPE, share_context,
                                       True)

    def attach(self, canvas):
        if canvas is self.canvas:  # XXX do this for carbon too?
            return

        super(XlibContext13, self).attach(canvas)

        self.glx_window = glx.glXCreateWindow(
            self.x_display, self.config._fbconfig, canvas.x_window, None)
        self.set_current()

    def set_current(self):
        glx.glXMakeContextCurrent(
            self.x_display, self.glx_window, self.glx_window, self.glx_context)
        super(XlibContext13, self).set_current()

    def detach(self):
        if not self.canvas:
            return

        self.set_current()
        gl.glFlush()  # needs to be in try/except?

        super(XlibContext13, self).detach()

        glx.glXMakeContextCurrent(self.x_display, 0, 0, None)
        if self.glx_window:
            glx.glXDestroyWindow(self.x_display, self.glx_window)
            self.glx_window = None

    def destroy(self):
        super(XlibContext13, self).destroy()
        if self.glx_window:
            glx.glXDestroyWindow(self.config.display._display, self.glx_window)
            self.glx_window = None
        if self.glx_context:
            glx.glXDestroyContext(self.x_display, self.glx_context)
            self.glx_context = None

    def flip(self):
        if not self.glx_window:
            return

        if self._vsync:
            self._wait_vsync()
        glx.glXSwapBuffers(self.x_display, self.glx_window)


class XlibContextARB(XlibContext13):
    def _create_glx_context(self, share):
        if share:
            share_context = share.glx_context
        else:
            share_context = None

        attribs = []
        if self.config.major_version is not None:
            attribs.extend([glxext_arb.GLX_CONTEXT_MAJOR_VERSION_ARB,
                            self.config.major_version])
        if self.config.minor_version is not None:
            attribs.extend([glxext_arb.GLX_CONTEXT_MINOR_VERSION_ARB,
                            self.config.minor_version])
        flags = 0
        if self.config.forward_compatible:
            flags |= glxext_arb.GLX_CONTEXT_FORWARD_COMPATIBLE_BIT_ARB
        if self.config.debug:
            flags |= glxext_arb.GLX_CONTEXT_DEBUG_BIT_ARB
        if flags:
            attribs.extend([glxext_arb.GLX_CONTEXT_FLAGS_ARB, flags])
        attribs.append(0)
        attribs = (c_int * len(attribs))(*attribs)

        return glxext_arb.glXCreateContextAttribsARB(
            self.config.canvas.display._display,
            self.config._fbconfig, share_context, True, attribs)
