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

'''Encoder and decoder for PNG files, using PyPNG (png.py).
'''

__docformat__ = 'restructuredtext'
__version__ = '$Id: $'

import array
import itertools

from pyglet.gl import *
from pyglet.image import *
from pyglet.image.codecs import *

import pyglet.extlibs.png as pypng


class PNGImageDecoder(ImageDecoder):
    def get_file_extensions(self):
        return ['.png']

    def decode(self, file, filename):
        try:
            reader = pypng.Reader(file=file)
            width, height, pixels, metadata = reader.asDirect()
        except Exception as e:
            raise ImageDecodeException(
                'PyPNG cannot read %r: %s' % (filename or file, e))

        if metadata['greyscale']:
            if metadata['alpha']:
                format = 'LA'
            else:
                format = 'L'
        else:
            if metadata['alpha']:
                format = 'RGBA'
            else:
                format = 'RGB'
        pitch = len(format) * width

        pixels = array.array('BH'[metadata['bitdepth']>8], itertools.chain(*pixels))
        return ImageData(width, height, format, pixels.tostring(), -pitch)

class PNGImageEncoder(ImageEncoder):
    def get_file_extensions(self):
        return ['.png']

    def encode(self, image, file, filename):
        image = image.get_image_data()

        has_alpha = 'A' in image.format
        greyscale = len(image.format) < 3
        if has_alpha:
            if greyscale:
                image.format = 'LA'
            else:
                image.format = 'RGBA'
        else:
            if greyscale:
                image.format = 'L'
            else:
                image.format = 'RGB'

        image.pitch = -(image.width * len(image.format))

        writer = pypng.Writer(
            image.width, image.height,
            bytes_per_sample=1,
            greyscale=greyscale,
            alpha=has_alpha)

        data = array.array('B')
        data.fromstring(image.data)
        writer.write_array(file, data)

def get_decoders():
    return [PNGImageDecoder()]

def get_encoders():
    return [PNGImageEncoder()]
