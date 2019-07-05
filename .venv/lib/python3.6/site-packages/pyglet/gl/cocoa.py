#!/usr/bin/env python

'''
'''

__docformat__ = 'restructuredtext'
__version__ = '$Id: $'

import platform

from pyglet.gl.base import Config, CanvasConfig, Context

from pyglet.gl import ContextException
from pyglet.gl import gl
from pyglet.gl import agl

from pyglet.canvas.cocoa import CocoaCanvas

from pyglet.libs.darwin.cocoapy import *

NSOpenGLPixelFormat = ObjCClass('NSOpenGLPixelFormat')
NSOpenGLContext = ObjCClass('NSOpenGLContext')

# Version info, needed as OpenGL different Lion and onward
"""Version is based on Darwin kernel, not OS-X version.
OS-X / Darwin version history
http://en.wikipedia.org/wiki/Darwin_(operating_system)#Release_history
pre-release:    0.1, 0.2, 1.0, 1.1,
kodiak:         1.2.1,
cheetah:        1.3.1,
puma:           1.4.1, 5.1 -> 5.5
jaguar:         6.0.1 -> 6.8
panther:        7.0 -> 7.9
tiger:          8.0 -> 8.11
leopard:        9.0 -> 9.8
snow_leopard:   10.0 -> 10.8
lion:           11.0 -> 11.4
mountain_lion:  12.0 -> 12.5
mavericks:      13.0 -> 13.4
yosemite:       14.0 -> 14.5
el_capitan:     15.0 -> 15.6
sierra:         16.0 -> 16.6
"""
os_x_release = {
    'pre-release':      (0,1),
    'kodiak':           (1,2,1),
    'cheetah':          (1,3,1),
    'puma':             (1,4.1),
    'jaguar':           (6,0,1),
    'panther':          (7,),
    'tiger':            (8,),
    'leopard':          (9,),
    'snow_leopard':     (10,),
    'lion':             (11,),
    'mountain_lion':    (12,),
    'mavericks':        (13,),
    'yosemite':         (14,),
    'el_capitan':       (15,),
    'sierra':           (16,),
    }

def os_x_version():
    version = tuple([int(v) for v in platform.release().split('.')])

    # ensure we return a tuple
    if len(version) > 0:
        return version
    return (version,)

_os_x_version = os_x_version()

# Valid names for GL attributes and their corresponding NSOpenGL constant.
_gl_attributes = {
    'double_buffer': NSOpenGLPFADoubleBuffer,
    'stereo': NSOpenGLPFAStereo,
    'buffer_size': NSOpenGLPFAColorSize,
    'sample_buffers': NSOpenGLPFASampleBuffers,
    'samples': NSOpenGLPFASamples,
    'aux_buffers': NSOpenGLPFAAuxBuffers,
    'alpha_size': NSOpenGLPFAAlphaSize,
    'depth_size': NSOpenGLPFADepthSize,
    'stencil_size': NSOpenGLPFAStencilSize,

    # Not exposed by pyglet API (set internally)
    'all_renderers': NSOpenGLPFAAllRenderers,
    'fullscreen': NSOpenGLPFAFullScreen,
    'minimum_policy': NSOpenGLPFAMinimumPolicy,
    'maximum_policy': NSOpenGLPFAMaximumPolicy,
    'screen_mask' : NSOpenGLPFAScreenMask,

    # Not supported in current pyglet API
    'color_float': NSOpenGLPFAColorFloat,
    'offscreen': NSOpenGLPFAOffScreen,
    'sample_alpha': NSOpenGLPFASampleAlpha,
    'multisample': NSOpenGLPFAMultisample,
    'supersample': NSOpenGLPFASupersample,
}

# NSOpenGL constants which do not require a value.
_boolean_gl_attributes = frozenset([
    NSOpenGLPFAAllRenderers,
    NSOpenGLPFADoubleBuffer,
    NSOpenGLPFAStereo,
    NSOpenGLPFAMinimumPolicy,
    NSOpenGLPFAMaximumPolicy,
    NSOpenGLPFAOffScreen,
    NSOpenGLPFAFullScreen,
    NSOpenGLPFAColorFloat,
    NSOpenGLPFAMultisample,
    NSOpenGLPFASupersample,
    NSOpenGLPFASampleAlpha,
])

# Attributes for which no NSOpenGLPixelFormatAttribute name exists.
# We could probably compute actual values for these using
# NSOpenGLPFAColorSize / 4 and NSOpenGLFAAccumSize / 4, but I'm not that
# confident I know what I'm doing.
_fake_gl_attributes = {
    'red_size': 0,
    'green_size': 0,
    'blue_size': 0,
    'accum_red_size': 0,
    'accum_green_size': 0,
    'accum_blue_size': 0,
    'accum_alpha_size': 0
}

class CocoaConfig(Config):

    def match(self, canvas):
        # Construct array of attributes for NSOpenGLPixelFormat
        attrs = []
        for name, value in self.get_gl_attributes():
            attr = _gl_attributes.get(name)
            if not attr or not value:
                continue
            attrs.append(attr)
            if attr not in _boolean_gl_attributes:
                attrs.append(int(value))

        # Support for RAGE-II, which is not compliant.
        attrs.append(NSOpenGLPFAAllRenderers)

        # Force selection policy.
        attrs.append(NSOpenGLPFAMaximumPolicy)

        # NSOpenGLPFAFullScreen is always supplied so we can switch to and
        # from fullscreen without losing the context.  Also must supply the
        # NSOpenGLPFAScreenMask attribute with appropriate display ID.
        # Note that these attributes aren't necessary to render in fullscreen
        # on Mac OS X 10.6, because there we are simply rendering into a
        # screen sized window.  See:
        # http://developer.apple.com/library/mac/#documentation/GraphicsImaging/Conceptual/OpenGL-MacProgGuide/opengl_fullscreen/opengl_cgl.html%23//apple_ref/doc/uid/TP40001987-CH210-SW6
        # Otherwise, make sure we refer to the correct Profile for OpenGL (Core or
        # Legacy) on Lion and afterwards
        if _os_x_version < os_x_release['snow_leopard']:
            attrs.append(NSOpenGLPFAFullScreen)
            attrs.append(NSOpenGLPFAScreenMask)
            attrs.append(quartz.CGDisplayIDToOpenGLDisplayMask(quartz.CGMainDisplayID()))
        elif _os_x_version >= os_x_release['lion']:
            # check for opengl profile
            # This requires OS-X Lion (Darwin 11) or higher
            version = (
                getattr(self, 'major_version', None),
                getattr(self, 'minor_version', None)
                )
            # tell os-x we want to request a profile
            attrs.append(NSOpenGLPFAOpenGLProfile)

            # check if we're wanting core or legacy
            # Mavericks (Darwin 13) and up are capable of the Core 4.1 profile,
            # while Lion and up are only capable of Core 3.2
            if version == (4, 1) and _os_x_version >= os_x_release['mavericks']:
                attrs.append(int(NSOpenGLProfileVersion4_1Core))
            elif version == (3, 2):
                attrs.append(int(NSOpenGLProfileVersion3_2Core))
            else:
                attrs.append(int(NSOpenGLProfileVersionLegacy))
        # Terminate the list.
        attrs.append(0)

        # Create the pixel format.
        attrsArrayType = c_uint32 * len(attrs)
        attrsArray = attrsArrayType(*attrs)
        pixel_format = NSOpenGLPixelFormat.alloc().initWithAttributes_(attrsArray)

        # Return the match list.
        if pixel_format is None:
            return []
        else:
            return [CocoaCanvasConfig(canvas, self, pixel_format)]


class CocoaCanvasConfig(CanvasConfig):

    def __init__(self, canvas, config, pixel_format):
        super(CocoaCanvasConfig, self).__init__(canvas, config)
        self._pixel_format = pixel_format

        # Query values for the attributes of the pixel format, and then set the
        # corresponding attributes of the canvas config.
        for name, attr in _gl_attributes.items():
            vals = c_int()
            self._pixel_format.getValues_forAttribute_forVirtualScreen_(byref(vals), attr, 0)
            setattr(self, name, vals.value)

        # Set these attributes so that we can run pyglet.info.
        for name, value in _fake_gl_attributes.items():
            setattr(self, name, value)

        # Update the minor/major version from profile if (Mountain)Lion
        if _os_x_version >= os_x_release['lion']:
            vals = c_int()
            profile = self._pixel_format.getValues_forAttribute_forVirtualScreen_(
                byref(vals),
                NSOpenGLPFAOpenGLProfile,
                0
                )

            if profile == NSOpenGLProfileVersion4_1Core:
                setattr(self, "major_version", 4)
                setattr(self, "minor_version", 1)
            elif profile == NSOpenGLProfileVersion3_2Core:
                setattr(self, "major_version", 3)
                setattr(self, "minor_version", 2)
            else:
                setattr(self, "major_version", 2)
                setattr(self, "minor_version", 1)

    def create_context(self, share):
        # Determine the shared NSOpenGLContext.
        if share:
            share_context = share._nscontext
        else:
            share_context = None

        # Create a new NSOpenGLContext.
        nscontext = NSOpenGLContext.alloc().initWithFormat_shareContext_(
            self._pixel_format,
            share_context)

        return CocoaContext(self, nscontext, share)

    def compatible(self, canvas):
        return isinstance(canvas, CocoaCanvas)


class CocoaContext(Context):

    def __init__(self, config, nscontext, share):
        super(CocoaContext, self).__init__(config, share)
        self.config = config
        self._nscontext = nscontext

    def attach(self, canvas):
        # See if we want OpenGL 3 in a non-Lion OS
        if _os_x_version < os_x_release['lion'] and self.config._requires_gl_3():
            raise ContextException('OpenGL 3 not supported')

        super(CocoaContext, self).attach(canvas)
        # The NSView instance should be attached to a nondeferred window before calling
        # setView, otherwise you get an "invalid drawable" message.
        self._nscontext.setView_(canvas.nsview)
        self._nscontext.view().setWantsBestResolutionOpenGLSurface_(1)
        self.set_current()

    def detach(self):
        super(CocoaContext, self).detach()
        self._nscontext.clearDrawable()

    def set_current(self):
        self._nscontext.makeCurrentContext()
        super(CocoaContext, self).set_current()

    def update_geometry(self):
        # Need to call this method whenever the context drawable (an NSView)
        # changes size or location.
        self._nscontext.update()

    def set_full_screen(self):
        self._nscontext.makeCurrentContext()
        self._nscontext.setFullScreen()

    def destroy(self):
        super(CocoaContext, self).destroy()
        self._nscontext.release()
        self._nscontext = None

    def set_vsync(self, vsync=True):
        vals = c_int(vsync)
        self._nscontext.setValues_forParameter_(byref(vals), NSOpenGLCPSwapInterval)

    def get_vsync(self):
        vals = c_int()
        self._nscontext.getValues_forParameter_(byref(vals), NSOpenGLCPSwapInterval)
        return vals.value

    def flip(self):
        self._nscontext.flushBuffer()
