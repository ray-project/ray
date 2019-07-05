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

'''Read GIF control data.

http://www.w3.org/Graphics/GIF/spec-gif89a.txt
'''
from __future__ import print_function
from __future__ import division
from builtins import object

__docformat__ = 'restructuredtext'
__version__ = '$Id$'

import struct

from pyglet.image.codecs import ImageDecodeException

class GIFStream(object):
    def __init__(self):
        self.images = []

class GIFImage(object):
    delay = None

class GraphicsScope(object):
    delay = None

# Appendix A.
LABEL_EXTENSION_INTRODUCER = 0x21
LABEL_GRAPHIC_CONTROL_EXTENSION = 0xf9
LABEL_IMAGE_DESCRIPTOR = 0x2c
LABEL_TRAILER = 0x3b

def unpack(format, file):
    size = struct.calcsize(format)
    data = file.read(size)
    if len(data) < size:
        raise ImageDecodeException('Unexpected EOF')
    return struct.unpack(format, data)

def read_byte(file):
    data = file.read(1)
    if not len(data):
        raise ImageDecodeException('Unexpected EOF')
    return ord(data)

def read(file):
    '''Read a GIF file stream.

    :rtype: GIFStream
    '''
    # 17. Header
    signature = file.read(3)
    version = file.read(3)
    if signature != b'GIF':
        raise ImageDecodeException('Not a GIF stream')

    stream = GIFStream()

    # 18. Logical screen descriptor
    (logical_screen_width,
     logical_screen_height,
     fields, 
     background_color_index,
     pixel_aspect_ratio) = unpack('HHBBB', file)
    global_color_table_flag = fields & 0x80
    global_color_table_size = fields & 0x7

    # 19. Global color table
    if global_color_table_flag:
        global_color_table = file.read(6 << global_color_table_size)

    # <Data>*
    graphics_scope = GraphicsScope()
    block_type = read_byte(file)

    while block_type != LABEL_TRAILER:
        if block_type == LABEL_IMAGE_DESCRIPTOR:
            read_table_based_image(file, stream, graphics_scope)
            graphics_scope = GraphicsScope()
        elif block_type == LABEL_EXTENSION_INTRODUCER:
            extension_block_type = read_byte(file)
            if extension_block_type == LABEL_GRAPHIC_CONTROL_EXTENSION:
                read_graphic_control_extension(file, stream, graphics_scope)
            else:
                skip_data_sub_blocks(file)
        else:
            # Skip bytes until a valid start character is found
            print(block_type)
            pass
        block_type = read_byte(file)

    return stream
        
def skip_data_sub_blocks(file):
    # 15. Data sub-blocks
    block_size = read_byte(file)
    while block_size != 0:
        data = file.read(block_size)
        block_size = read_byte(file)

def read_table_based_image(file, stream, graphics_scope):
    gif_image = GIFImage()
    stream.images.append(gif_image)
    gif_image.delay = graphics_scope.delay
        
    # 20. Image descriptor
    (image_left_position,
     image_top_position,
     image_width,
     image_height,
     fields) = unpack('HHHHB', file)

    local_color_table_flag = fields & 0x80
    local_color_table_size = fields & 0x7

    # 21. Local color table
    if local_color_table_flag:
        local_color_table = file.read(6 << local_color_table_size)

    # 22. Table based image data
    lzw_code_size = file.read(1)
    skip_data_sub_blocks(file)

def read_graphic_control_extension(file, stream, graphics_scope):
    # 23. Graphic control extension
    (block_size,
     fields,
     delay_time,
     transparent_color_index,
     terminator) = unpack('BBHBB', file)
    if block_size != 4:
        raise ImageDecodeException('Incorrect block size')
    
    if delay_time:
        # Follow Firefox/Mac behaviour: use 100ms delay for any delay
        # less than 10ms.
        if delay_time <= 1:
            delay_time = 10
        graphics_scope.delay = float(delay_time) / 100
