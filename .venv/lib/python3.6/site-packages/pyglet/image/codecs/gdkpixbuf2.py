# ----------------------------------------------------------------------------
# pyglet
# Copyright (c) 2006-2008 Alex Holkner
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions 
# are met:
#
#  * Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
#  * Redistributions in binary form must reproduce the above copyright 
#    notice, this list of conditions and the following disclaimer in
#    the documentation and/or other materials provided with the
#    distribution.
#  * Neither the name of pyglet nor the names of its
#    contributors may be used to endorse or promote products
#    derived from this software without specific prior written
#    permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
# ----------------------------------------------------------------------------

from builtins import object
from ctypes import *

from pyglet.gl import *
from pyglet.image import *
from pyglet.image.codecs import *
from pyglet.image.codecs import gif

import pyglet.lib
import pyglet.window

gdk = pyglet.lib.load_library('gdk-x11-2.0')
gdkpixbuf = pyglet.lib.load_library('gdk_pixbuf-2.0')

GdkPixbufLoader = c_void_p
GdkPixbuf = c_void_p
guchar = c_char
gdkpixbuf.gdk_pixbuf_loader_new.restype = POINTER(GdkPixbufLoader)
gdkpixbuf.gdk_pixbuf_loader_get_pixbuf.restype = POINTER(GdkPixbuf)
gdkpixbuf.gdk_pixbuf_get_pixels.restype = POINTER(guchar)
gdkpixbuf.gdk_pixbuf_loader_get_animation.restype = POINTER(c_void_p)
gdkpixbuf.gdk_pixbuf_animation_get_iter.restype = POINTER(c_void_p)
gdkpixbuf.gdk_pixbuf_animation_iter_get_pixbuf.restype = POINTER(GdkPixbuf)


class GTimeVal(Structure):
    _fields_ = [
        ('tv_sec', c_long),
        ('tv_usec', c_long)
    ]


GQuark = c_uint32
gint = c_int
gchar = c_char


class GError(Structure):
    _fields_ = [
            ('domain', GQuark),
            ('code', gint),
            ('message', POINTER(gchar))
    ]

gerror_ptr = POINTER(GError)

def _gerror_to_string(error):
    """
    Convert a GError to a string.
    `error` should be a valid pointer to a GError struct.
    """
    return 'GdkPixBuf Error: domain[{}], code[{}]: {}'.format(error.contents.domain,
                                                              error.contents.code,
                                                              error.contents.message)


class GdkPixBufLoader(object):
    """
    Wrapper around GdkPixBufLoader object.
    """
    def __init__(self, file_, filename):
        self.closed = False
        self._file = file_
        self._filename = filename
        self._loader = gdkpixbuf.gdk_pixbuf_loader_new()
        if self._loader is None:
            raise ImageDecodeException('Unable to instantiate gdk pixbuf loader')
        self._load_file()

    def __del__(self):
        if self._loader is not None:
            if not self.closed:
                self._cancel_load()
            gdk.g_object_unref(self._loader)

    def _load_file(self):
        assert self._file is not None
        self._file.seek(0)
        data = self._file.read()
        self.write(data)

    def _finish_load(self):
        assert not self.closed
        error = gerror_ptr()
        all_data_passed = gdkpixbuf.gdk_pixbuf_loader_close(self._loader, byref(error))
        self.closed = True
        if not all_data_passed:
            raise ImageDecodeException(_gerror_to_string(error))

    def _cancel_load(self):
        assert not self.closed
        gdkpixbuf.gdk_pixbuf_loader_close(self._loader, None)
        self.closed = True

    def write(self, data):
        assert not self.closed, 'Cannot write after closing loader'
        error = gerror_ptr()
        if not gdkpixbuf.gdk_pixbuf_loader_write(self._loader, data, len(data), byref(error)):
            raise ImageDecodeException(_gerror_to_string(error))

    def get_pixbuf(self):
        self._finish_load()
        pixbuf = gdkpixbuf.gdk_pixbuf_loader_get_pixbuf(self._loader)
        if pixbuf is None:
            raise ImageDecodeException('Failed to get pixbuf from loader')
        return GdkPixBuf(self, pixbuf)

    def get_animation(self):
        self._finish_load()
        anim = gdkpixbuf.gdk_pixbuf_loader_get_animation(self._loader)
        if anim is None:
            raise ImageDecodeException('Failed to get animation from loader')
        gif_delays = self._get_gif_delays()
        return GdkPixBufAnimation(self, anim, gif_delays)

    def _get_gif_delays(self):
        # GDK pixbuf animations will loop indefinitely if looping is enabled for the
        # gif, so get number of frames and delays from gif metadata
        assert self._file is not None
        self._file.seek(0)
        gif_stream = gif.read(self._file)
        return [image.delay for image in gif_stream.images]


class GdkPixBuf(object):
    """
    Wrapper around GdkPixBuf object.
    """
    def __init__(self, loader, pixbuf):
        # Keep reference to loader alive
        self._loader = loader
        self._pixbuf = pixbuf
        gdk.g_object_ref(pixbuf)

    def __del__(self):
        if self._pixbuf is not None:
            gdk.g_object_unref(self._pixbuf)

    def load_next(self):
        return self._pixbuf is not None

    @property
    def width(self):
        assert self._pixbuf is not None
        return gdkpixbuf.gdk_pixbuf_get_width(self._pixbuf)

    @property
    def height(self):
        assert self._pixbuf is not None
        return gdkpixbuf.gdk_pixbuf_get_height(self._pixbuf)

    @property
    def channels(self):
        assert self._pixbuf is not None
        return gdkpixbuf.gdk_pixbuf_get_n_channels(self._pixbuf)

    @property
    def rowstride(self):
        assert self._pixbuf is not None
        return gdkpixbuf.gdk_pixbuf_get_rowstride(self._pixbuf)

    @property
    def has_alpha(self):
        assert self._pixbuf is not None
        return gdkpixbuf.gdk_pixbuf_get_has_alpha(self._pixbuf) == 1

    def get_pixels(self):
        pixels = gdkpixbuf.gdk_pixbuf_get_pixels(self._pixbuf)
        assert pixels is not None
        buf = (c_ubyte * (self.rowstride * self.height))()
        memmove(buf, pixels, self.rowstride * (self.height - 1) + self.width * self.channels)
        return buf

    def to_image(self):
        if self.width < 1 or self.height < 1 or self.channels < 1 or self.rowstride < 1:
            return None

        pixels = self.get_pixels()

        # Determine appropriate GL type
        if self.channels == 3:
            format = 'RGB'
        else:
            format = 'RGBA'

        return ImageData(self.width, self.height, format, pixels, -self.rowstride)


class GdkPixBufAnimation(object):
    """
    Wrapper for a GdkPixBufIter for an animation.
    """
    def __init__(self, loader, anim, gif_delays):
        self._loader = loader
        self._anim = anim
        self._gif_delays = gif_delays
        gdk.g_object_ref(anim)

    def __del__(self):
        if self._anim is not None:
            gdk.g_object_unref(self._anim)

    def __iter__(self):
        time = GTimeVal(0, 0)
        anim_iter = gdkpixbuf.gdk_pixbuf_animation_get_iter(self._anim, byref(time))
        return GdkPixBufAnimationIterator(self._loader, anim_iter, time, self._gif_delays)

    def to_animation(self):
        return Animation(list(self))


class GdkPixBufAnimationIterator(object):
    def __init__(self, loader, anim_iter, start_time, gif_delays):
        self._iter = anim_iter
        self._first = True
        self._time = start_time
        self._loader = loader
        self._gif_delays = gif_delays
        self.delay_time = None

    def __del__(self):
        if self._iter is not None:
            gdk.g_object_unref(self._iter)
        # The pixbuf returned by the iter is owned by the iter, so no need to destroy that one

    def __iter__(self):
        return self

    def __next__(self):
        self._advance()
        frame = self.get_frame()
        if frame is None:
            raise StopIteration
        return frame

    def _advance(self):
        if not self._gif_delays:
            raise StopIteration
        self.delay_time = self._gif_delays.pop(0)

        if self._first:
            self._first = False
        else:
            if self.gdk_delay_time == -1:
                raise StopIteration
            else:
                gdk_delay = self.gdk_delay_time * 1000 # milliseconds to microseconds
                us = self._time.tv_usec + gdk_delay
                self._time.tv_sec += us // 1000000
                self._time.tv_usec = us % 1000000
                gdkpixbuf.gdk_pixbuf_animation_iter_advance(self._iter, byref(self._time))

    def get_frame(self):
        pixbuf = gdkpixbuf.gdk_pixbuf_animation_iter_get_pixbuf(self._iter)
        if pixbuf is None:
            return None
        image = GdkPixBuf(self._loader, pixbuf).to_image()
        return AnimationFrame(image, self.delay_time)

    @property
    def gdk_delay_time(self):
        assert self._iter is not None
        return gdkpixbuf.gdk_pixbuf_animation_iter_get_delay_time(self._iter)


class GdkPixbuf2ImageDecoder(ImageDecoder):
    def get_file_extensions(self):
        return ['.png', '.xpm', '.jpg', '.jpeg', '.tif', '.tiff', '.pnm',
                '.ras', '.bmp', '.gif']

    def get_animation_file_extensions(self):
        return ['.gif', '.ani']

    def decode(self, file, filename):
        loader = GdkPixBufLoader(file, filename)
        return loader.get_pixbuf().to_image()

    def decode_animation(self, file, filename):
        loader = GdkPixBufLoader(file, filename)
        return loader.get_animation().to_animation()


def get_decoders():
    return [GdkPixbuf2ImageDecoder()]


def get_encoders():
    return []


def init():
    gdk.g_type_init()

init()

