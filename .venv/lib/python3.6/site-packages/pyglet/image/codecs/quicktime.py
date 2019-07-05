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

'''
'''
from __future__ import division
from builtins import chr

__docformat__ = 'restructuredtext'
__version__ = '$Id: pil.py 163 2006-11-13 04:15:46Z Alex.Holkner $'

import sys

from ctypes import *

from pyglet.gl import *
from pyglet.image import *
from pyglet.image.codecs import *

from pyglet.window.carbon import carbon, quicktime, _oscheck
from pyglet.libs.darwin.constants import _name
from pyglet.libs.darwin.types import *

Handle = POINTER(POINTER(c_byte))

GWorldPtr = c_void_p
carbon.NewHandle.restype = Handle
HandleDataHandlerSubType = _name('hndl')
PointerDataHandlerSubType = _name('ptr ')
kDataHCanRead = 1
kDataRefExtensionFileName = _name('fnam')
kDataRefExtensionMIMEType = _name('mime')
ComponentInstance = c_void_p
   
k1MonochromePixelFormat       = 0x00000001
k2IndexedPixelFormat          = 0x00000002
k4IndexedPixelFormat          = 0x00000004
k8IndexedPixelFormat          = 0x00000008
k16BE555PixelFormat           = 0x00000010
k24RGBPixelFormat             = 0x00000018
k32ARGBPixelFormat            = 0x00000020
k32BGRAPixelFormat            = _name('BGRA')
k1IndexedGrayPixelFormat      = 0x00000021
k2IndexedGrayPixelFormat      = 0x00000022
k4IndexedGrayPixelFormat      = 0x00000024
k8IndexedGrayPixelFormat      = 0x00000028
kNativeEndianPixMap           = 1 << 8

kGraphicsImporterDontDoGammaCorrection = 1 << 0
kGraphicsImporterDontUseColorMatching = 1 << 3

newMovieActive                = 1
noErr                         = 0
movieTrackMediaType = 1 << 0
movieTrackCharacteristic = 1 << 1
movieTrackEnabledOnly = 1 << 2
VisualMediaCharacteristic = _name('eyes')
nextTimeMediaSample           = 1

class PointerDataRefRecord(Structure):
    _fields_ = [
        ('data', c_void_p),
        ('dataLength', c_long)
    ]

def Str255(value):
    return create_string_buffer(chr(len(value)) + value)

class QuickTimeImageDecoder(ImageDecoder):
    def get_file_extensions(self):
        # Only most common ones shown here
        return ['.bmp', '.cur', '.gif', '.ico', '.jpg', '.jpeg', '.pcx', '.png',
                '.tga', '.tif', '.tiff', '.xbm', '.xpm']

    def get_animation_file_extensions(self):
        return ['.gif']

    def _get_data_ref(self, file, filename):
        self._data_hold = data = create_string_buffer(file.read())

        dataref = carbon.NewHandle(sizeof(PointerDataRefRecord))
        datarec = cast(dataref,
            POINTER(POINTER(PointerDataRefRecord))).contents.contents
        datarec.data = addressof(data)
        datarec.dataLength = len(data)

        self._data_handler_holder = data_handler = ComponentInstance() 
        r = quicktime.OpenADataHandler(dataref, PointerDataHandlerSubType,
            None, 0, None, kDataHCanRead, byref(data_handler))
        _oscheck(r)

        extension_handle = Handle()

        self._filename_hold = filename = Str255(filename)
        r = carbon.PtrToHand(filename, byref(extension_handle), len(filename))
        r = quicktime.DataHSetDataRefExtension(data_handler, extension_handle,
                                               kDataRefExtensionFileName)
        _oscheck(r)
        quicktime.DisposeHandle(extension_handle)

        quicktime.DisposeHandle(dataref)

        dataref = c_void_p()
        r = quicktime.DataHGetDataRef(data_handler, byref(dataref))
        _oscheck(r)
        
        quicktime.CloseComponent(data_handler)
        
        return dataref

    def _get_formats(self):
        # TODO choose 24 bit where appropriate.
        if sys.byteorder == 'big':
            format = 'ARGB'
            qtformat = k32ARGBPixelFormat
        else:
            format = 'BGRA'
            qtformat = k32BGRAPixelFormat
        return format, qtformat

    def decode(self, file, filename):
        dataref = self._get_data_ref(file, filename)
        importer = ComponentInstance()
        quicktime.GetGraphicsImporterForDataRef(dataref, 
            PointerDataHandlerSubType, byref(importer))

        if not importer:
            raise ImageDecodeException(filename or file)

        rect = Rect()
        quicktime.GraphicsImportGetNaturalBounds(importer, byref(rect))
        width = rect.right
        height = rect.bottom

        format, qtformat = self._get_formats()

        buffer = (c_byte * (width * height * len(format)))()
        world = GWorldPtr()
        quicktime.QTNewGWorldFromPtr(byref(world), qtformat,
            byref(rect), c_void_p(), c_void_p(), 0, buffer,
            len(format) * width)

        flags = (kGraphicsImporterDontUseColorMatching |
                 kGraphicsImporterDontDoGammaCorrection)
        quicktime.GraphicsImportSetFlags(importer, flags)
        quicktime.GraphicsImportSetGWorld(importer, world, c_void_p())
        
        result = quicktime.GraphicsImportDraw(importer)
        quicktime.DisposeGWorld(world)
        quicktime.CloseComponent(importer)

        if result != 0:
            raise ImageDecodeException(filename or file)

        pitch = len(format) * width

        return ImageData(width, height, format, buffer, -pitch)

    def decode_animation(self, file, filename):
        # TODO: Stop playing chicken with the GC
        # TODO: Cleanup in errors

        quicktime.EnterMovies()

        data_ref = self._get_data_ref(file, filename)
        if not data_ref:
            raise ImageDecodeException(filename or file)

        movie = c_void_p()
        id = c_short()
        result = quicktime.NewMovieFromDataRef(byref(movie), 
                                      newMovieActive,
                                      0,
                                      data_ref,
                                      PointerDataHandlerSubType)

        if not movie:
            #_oscheck(result)
            raise ImageDecodeException(filename or file)
        quicktime.GoToBeginningOfMovie(movie)

        time_scale = float(quicktime.GetMovieTimeScale(movie))

        format, qtformat = self._get_formats()
        
        # Get movie width and height
        rect = Rect()
        quicktime.GetMovieBox(movie, byref(rect))
        width = rect.right
        height = rect.bottom
        pitch = len(format) * width

        # Set gworld
        buffer = (c_byte * (width * height * len(format)))()
        world = GWorldPtr()
        quicktime.QTNewGWorldFromPtr(byref(world), qtformat,
            byref(rect), c_void_p(), c_void_p(), 0, buffer,
            len(format) * width) 
        quicktime.SetGWorld(world, 0)
        quicktime.SetMovieGWorld(movie, world, 0)

        visual = quicktime.GetMovieIndTrackType(movie, 1, 
                                                VisualMediaCharacteristic, 
                                                movieTrackCharacteristic)
        if not visual:
            raise ImageDecodeException('No video track')

        time = 0

        interesting_time = c_int()
        quicktime.GetTrackNextInterestingTime(
            visual,
            nextTimeMediaSample,
            time,
            1,
            byref(interesting_time),
            None)
        duration = interesting_time.value / time_scale

        frames = []

        while time >= 0:
            result = quicktime.GetMoviesError()
            if result == noErr:
                # force redraw
                result = quicktime.UpdateMovie(movie)
            if result == noErr:
                # process movie
                quicktime.MoviesTask(movie, 0)
                result = quicktime.GetMoviesError()
            _oscheck(result)

            buffer_copy = (c_byte * len(buffer))()
            memmove(buffer_copy, buffer, len(buffer))
            image = ImageData(width, height, format, buffer_copy, -pitch)
            frames.append(AnimationFrame(image, duration))

            interesting_time = c_int()
            duration = c_int()
            quicktime.GetTrackNextInterestingTime(
                visual,
                nextTimeMediaSample,
                time,
                1,
                byref(interesting_time),
                byref(duration))

            quicktime.SetMovieTimeValue(movie, interesting_time)
            time = interesting_time.value
            duration = duration.value / time_scale
            if duration <= 0.01:
                duration = 0.1

        quicktime.DisposeMovie(movie)
        carbon.DisposeHandle(data_ref)

        quicktime.ExitMovies()

        return Animation(frames)

def get_decoders():
    return [QuickTimeImageDecoder()]

def get_encoders():
    return []

