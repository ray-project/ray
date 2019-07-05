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

'''Collection of image encoders and decoders.

Modules must subclass ImageDecoder and ImageEncoder for each method of
decoding/encoding they support.

Modules must also implement the two functions::
    
    def get_decoders():
        # Return a list of ImageDecoder instances or []
        return []

    def get_encoders():
        # Return a list of ImageEncoder instances or []
        return []
    
'''
from builtins import object

__docformat__ = 'restructuredtext'
__version__ = '$Id: $'

import os.path
from pyglet import compat_platform

_decoders = []              # List of registered ImageDecoders
_decoder_extensions = {}    # Map str -> list of matching ImageDecoders
_decoder_animation_extensions = {}    
                            # Map str -> list of matching ImageDecoders
_encoders = []              # List of registered ImageEncoders
_encoder_extensions = {}    # Map str -> list of matching ImageEncoders

class ImageDecodeException(Exception):
    exception_priority = 10

class ImageEncodeException(Exception):
    pass

class ImageDecoder(object):
    def get_file_extensions(self):
        '''Return a list of accepted file extensions, e.g. ['.png', '.bmp']
        Lower-case only.
        '''
        return []

    def get_animation_file_extensions(self):
        '''Return a list of accepted file extensions, e.g. ['.gif', '.flc']
        Lower-case only.
        '''
        return []

    def decode(self, file, filename):
        '''Decode the given file object and return an instance of `Image`.
        Throws ImageDecodeException if there is an error.  filename
        can be a file type hint.
        '''
        raise NotImplementedError()

    def decode_animation(self, file, filename):
        '''Decode the given file object and return an instance of :py:class:`~pyglet.image.Animation`.
        Throws ImageDecodeException if there is an error.  filename
        can be a file type hint.
        '''
        raise ImageDecodeException('This decoder cannot decode animations.')

class ImageEncoder(object):
    def get_file_extensions(self):
        '''Return a list of accepted file extensions, e.g. ['.png', '.bmp']
        Lower-case only.
        '''
        return []

    def encode(self, image, file, filename, options={}):
        '''Encode the given image to the given file.  filename
        provides a hint to the file format desired.  options are
        encoder-specific, and unknown options should be ignored or
        issue warnings.
        '''
        raise NotImplementedError()

def get_encoders(filename=None):
    '''Get an ordered list of encoders to attempt.  filename can be used
    as a hint for the filetype.
    '''
    encoders = []
    if filename:
        extension = os.path.splitext(filename)[1].lower()
        encoders += _encoder_extensions.get(extension, [])
    encoders += [e for e in _encoders if e not in encoders]
    return encoders

def get_decoders(filename=None):
    '''Get an ordered list of decoders to attempt.  filename can be used
     as a hint for the filetype.
    '''
    decoders = []
    if filename:
        extension = os.path.splitext(filename)[1].lower()
        decoders += _decoder_extensions.get(extension, [])
    decoders += [e for e in _decoders if e not in decoders]
    return decoders

def get_animation_decoders(filename=None):
    '''Get an ordered list of decoders to attempt.  filename can be used
     as a hint for the filetype.
    '''
    decoders = []
    if filename:
        extension = os.path.splitext(filename)[1].lower()
        decoders += _decoder_animation_extensions.get(extension, [])
    decoders += [e for e in _decoders if e not in decoders]
    return decoders

def add_decoders(module):
    '''Add a decoder module.  The module must define `get_decoders`.  Once
    added, the appropriate decoders defined in the codec will be returned by
    pyglet.image.codecs.get_decoders.
    '''
    for decoder in module.get_decoders():
        _decoders.append(decoder)
        for extension in decoder.get_file_extensions():
            if extension not in _decoder_extensions:
                _decoder_extensions[extension] = []
            _decoder_extensions[extension].append(decoder)
        for extension in decoder.get_animation_file_extensions():
            if extension not in _decoder_animation_extensions:
                _decoder_animation_extensions[extension] = []
            _decoder_animation_extensions[extension].append(decoder)

def add_encoders(module):
    '''Add an encoder module.  The module must define `get_encoders`.  Once
    added, the appropriate encoders defined in the codec will be returned by
    pyglet.image.codecs.get_encoders.
    '''
    for encoder in module.get_encoders():
        _encoders.append(encoder)
        for extension in encoder.get_file_extensions():
            if extension not in _encoder_extensions:
                _encoder_extensions[extension] = []
            _encoder_extensions[extension].append(encoder)
 
def add_default_image_codecs():
    # Add the codecs we know about.  These should be listed in order of
    # preference.  This is called automatically by pyglet.image.

    # Compressed texture in DDS format
    try:
        from pyglet.image.codecs import dds
        add_encoders(dds)
        add_decoders(dds)
    except ImportError:
        pass

    # Mac OS X default: Quicktime for Carbon, Quartz for Cocoa.
    # TODO: Make ctypes Quartz the default for both Carbon & Cocoa.
    if compat_platform == 'darwin':
        try:
            from pyglet import options as pyglet_options
            if pyglet_options['darwin_cocoa']:
                import pyglet.image.codecs.quartz
                add_encoders(quartz)
                add_decoders(quartz)
            else:
                import pyglet.image.codecs.quicktime
                add_encoders(quicktime)
                add_decoders(quicktime)
        except ImportError:
            pass

    # Windows XP default: GDI+
    if compat_platform in ('win32', 'cygwin'):
        try:
            import pyglet.image.codecs.gdiplus
            add_encoders(gdiplus)
            add_decoders(gdiplus)
        except ImportError:
            pass

    # Linux default: GdkPixbuf 2.0
    if compat_platform.startswith('linux'):
        try:
            import pyglet.image.codecs.gdkpixbuf2
            add_encoders(gdkpixbuf2)
            add_decoders(gdkpixbuf2)
        except ImportError:
            pass

    # Fallback: PIL
    try:
        import pyglet.image.codecs.pil
        add_encoders(pil)
        add_decoders(pil)
    except ImportError:
        pass

    # Fallback: PNG loader (slow)
    try:
        import pyglet.image.codecs.png
        add_encoders(png)
        add_decoders(png)
    except ImportError:
        pass

    # Fallback: BMP loader (slow)
    try:
        import pyglet.image.codecs.bmp
        add_encoders(bmp)
        add_decoders(bmp)
    except ImportError:
        pass
