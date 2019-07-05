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

"""Image load, capture and high-level texture functions.

Only basic functionality is described here; for full reference see the
accompanying documentation.

To load an image::

    from pyglet import image
    pic = image.load('picture.png')

The supported image file types include PNG, BMP, GIF, JPG, and many more,
somewhat depending on the operating system.  To load an image from a file-like
object instead of a filename::

    pic = image.load('hint.jpg', file=fileobj)

The hint helps the module locate an appropriate decoder to use based on the
file extension.  It is optional.

Once loaded, images can be used directly by most other modules of pyglet.  All
images have a width and height you can access::

    width, height = pic.width, pic.height

You can extract a region of an image (this keeps the original image intact;
the memory is shared efficiently)::

    subimage = pic.get_region(x, y, width, height)

Remember that y-coordinates are always increasing upwards.

Drawing images
--------------

To draw an image at some point on the screen::

    pic.blit(x, y, z)

This assumes an appropriate view transform and projection have been applied.

Some images have an intrinsic "anchor point": this is the point which will be
aligned to the ``x`` and ``y`` coordinates when the image is drawn.  By
default the anchor point is the lower-left corner of the image.  You can use
the anchor point to center an image at a given point, for example::

    pic.anchor_x = pic.width // 2
    pic.anchor_y = pic.height // 2
    pic.blit(x, y, z)

Texture access
--------------

If you are using OpenGL directly, you can access the image as a texture::

    texture = pic.get_texture()

(This is the most efficient way to obtain a texture; some images are
immediately loaded as textures, whereas others go through an intermediate
form).  To use a texture with pyglet.gl::

    from pyglet.gl import *
    glEnable(texture.target)        # typically target is GL_TEXTURE_2D
    glBindTexture(texture.target, texture.id)
    # ... draw with the texture

Pixel access
------------

To access raw pixel data of an image::

    rawimage = pic.get_image_data()

(If the image has just been loaded this will be a very quick operation;
however if the image is a texture a relatively expensive readback operation
will occur).  The pixels can be accessed as a string::

    format = 'RGBA'
    pitch = rawimage.width * len(format)
    pixels = rawimage.get_data(format, pitch)

"format" strings consist of characters that give the byte order of each color
component.  For example, if rawimage.format is 'RGBA', there are four color
components: red, green, blue and alpha, in that order.  Other common format
strings are 'RGB', 'LA' (luminance, alpha) and 'I' (intensity).

The "pitch" of an image is the number of bytes in a row (this may validly be
more than the number required to make up the width of the image, it is common
to see this for word alignment).  If "pitch" is negative the rows of the image
are ordered from top to bottom, otherwise they are ordered from bottom to top.

Retrieving data with the format and pitch given in `ImageData.format` and
`ImageData.pitch` avoids the need for data conversion (assuming you can make
use of the data in this arbitrary format).

"""

from __future__ import division
from builtins import bytes
from builtins import zip

__docformat__ = 'restructuredtext'
__version__ = '$Id$'

from io import open
import re
import warnings
import weakref

from ctypes import *

from pyglet.gl import *
from pyglet.gl import gl_info
from pyglet.window import *

from pyglet.image import atlas
from pyglet.image import codecs as _codecs
from pyglet.compat import asbytes, bytes_type, BytesIO


class ImageException(Exception):
    pass


def load(filename, file=None, decoder=None):
    """Load an image from a file.

    :note: You can make no assumptions about the return type; usually it will
        be ImageData or CompressedImageData, but decoders are free to return
        any subclass of AbstractImage.

    :Parameters:
        `filename` : str
            Used to guess the image format, and to load the file if `file` is
            unspecified.
        `file` : file-like object or None
            Source of image data in any supported format.        
        `decoder` : ImageDecoder or None
            If unspecified, all decoders that are registered for the filename
            extension are tried.  If none succeed, the exception from the
            first decoder is raised.

    :rtype: AbstractImage
    """

    if not file:
        file = open(filename, 'rb')
        opened_file = file
    else:
        opened_file = None

    if not hasattr(file, 'seek'):
        file = BytesIO(file.read())

    try:
        if decoder:
            return decoder.decode(file, filename)
        else:
            first_exception = None
            for decoder in _codecs.get_decoders(filename):
                try:
                    image = decoder.decode(file, filename)
                    return image
                except _codecs.ImageDecodeException as e:
                    if (not first_exception or
                                first_exception.exception_priority < e.exception_priority):
                        first_exception = e
                    file.seek(0)

            if not first_exception:
                raise _codecs.ImageDecodeException('No image decoders are available')
            raise first_exception
    finally:
        if opened_file:
            opened_file.close()


def create(width, height, pattern=None):
    """Create an image optionally filled with the given pattern.

    :note: You can make no assumptions about the return type; usually it will
        be ImageData or CompressedImageData, but patterns are free to return
        any subclass of AbstractImage.

    :Parameters:
        `width` : int
            Width of image to create
        `height` : int
            Height of image to create
        `pattern` : ImagePattern or None
            Pattern to fill image with.  If unspecified, the image will
            initially be transparent.

    :rtype: AbstractImage
    """
    if not pattern:
        pattern = SolidColorImagePattern()
    return pattern.create_image(width, height)


def color_as_bytes(color):
    if sys.version.startswith('2'):
        return '%c%c%c%c' % color
    else:
        if len(color) != 4:
            raise TypeError("color is expected to have 4 components")
        return bytes(color)


class ImagePattern(object):
    """Abstract image creation class."""

    def create_image(self, width, height):
        """Create an image of the given size.

        :Parameters:
            `width` : int
                Width of image to create
            `height` : int
                Height of image to create
        
        :rtype: AbstractImage
        """
        raise NotImplementedError('abstract')


class SolidColorImagePattern(ImagePattern):
    """Creates an image filled with a solid color."""

    def __init__(self, color=(0, 0, 0, 0)):
        """Create a solid image pattern with the given color.

        :Parameters:
            `color` : (int, int, int, int)
                4-tuple of ints in range [0,255] giving RGBA components of
                color to fill with.

        """
        self.color = color_as_bytes(color)

    def create_image(self, width, height):
        data = self.color * width * height
        return ImageData(width, height, 'RGBA', data)


class CheckerImagePattern(ImagePattern):
    """Create an image with a tileable checker image.
    """

    def __init__(self, color1=(150, 150, 150, 255), color2=(200, 200, 200, 255)):
        """Initialise with the given colors.

        :Parameters:
            `color1` : (int, int, int, int)
                4-tuple of ints in range [0,255] giving RGBA components of
                color to fill with.  This color appears in the top-left and
                bottom-right corners of the image.
            `color2` : (int, int, int, int)
                4-tuple of ints in range [0,255] giving RGBA components of
                color to fill with.  This color appears in the top-right and
                bottom-left corners of the image.

        """
        self.color1 = color_as_bytes(color1)
        self.color2 = color_as_bytes(color2)

    def create_image(self, width, height):
        hw = width // 2
        hh = height // 2
        row1 = self.color1 * hw + self.color2 * hw
        row2 = self.color2 * hw + self.color1 * hw
        data = row1 * hh + row2 * hh
        return ImageData(width, height, 'RGBA', data)


class AbstractImage(object):
    """Abstract class representing an image.

    :Ivariables:
        `width` : int
            Width of image
        `height` : int
            Height of image
        `anchor_x` : int
            X coordinate of anchor, relative to left edge of image data
        `anchor_y` : int
            Y coordinate of anchor, relative to bottom edge of image data
    """
    anchor_x = 0
    anchor_y = 0

    _is_rectangle = False

    def __init__(self, width, height):
        self.width = width
        self.height = height

    def __repr__(self):
        return '<%s %dx%d>' % (self.__class__.__name__, self.width, self.height)

    def get_image_data(self):
        """Get an ImageData view of this image.  
        
        Changes to the returned instance may or may not be reflected in this
        image.

        :rtype: :py:class:`~pyglet.image.ImageData`

        .. versionadded:: 1.1
        """
        raise ImageException('Cannot retrieve image data for %r' % self)

    @property
    def image_data(self):
        """An :py:class:`~pyglet.image.ImageData` view of this image.  
        
        Changes to the returned instance may or may not be reflected in this
        image.  Read-only.

        :deprecated: Use :py:meth:`~pyglet.image.ImageData.get_image_data`.

        :type: :py:class:`~pyglet.image.ImageData`
        """
        return self.get_image_data()

    def get_texture(self, rectangle=False, force_rectangle=False):
        """A :py:class:`~pyglet.image.Texture` view of this image.  

        By default, textures are created with dimensions that are powers of
        two.  Smaller images will return a :py:class:`~pyglet.image.TextureRegion` that covers just
        the image portion of the larger texture.  This restriction is required
        on older video cards, and for compressed textures, or where texture
        repeat modes will be used, or where mipmapping is desired.

        If the `rectangle` parameter is ``True``, this restriction is ignored
        and a texture the size of the image may be created if the driver
        supports the ``GL_ARB_texture_rectangle`` or
        ``GL_NV_texture_rectangle`` extensions.  If the extensions are not
        present, the image already is a texture, or the image has power 2
        dimensions, the `rectangle` parameter is ignored.

        Examine `Texture.target` to determine if the returned texture is a
        rectangle (``GL_TEXTURE_RECTANGLE_ARB`` or
        ``GL_TEXTURE_RECTANGLE_NV``) or not (``GL_TEXTURE_2D``).

        If the `force_rectangle` parameter is ``True``, one of these
        extensions must be present, and the returned texture always
        has target ``GL_TEXTURE_RECTANGLE_ARB`` or ``GL_TEXTURE_RECTANGLE_NV``.
        
        Changes to the returned instance may or may not be reflected in this
        image.

        :Parameters:
            `rectangle` : bool
                True if the texture can be created as a rectangle.
            `force_rectangle` : bool
                True if the texture must be created as a rectangle.

                .. versionadded:: 1.1.4.
        :rtype: :py:class:`~pyglet.image.Texture`

        .. versionadded:: 1.1
        """
        raise ImageException('Cannot retrieve texture for %r' % self)

    @property
    def texture(self):
        """Get a :py:class:`~pyglet.image.Texture` view of this image.  
        
        Changes to the returned instance may or may not be reflected in this
        image.

        :deprecated: Use :py:meth:`~pyglet.image.AbstractImage.get_texture`.

        :type: :py:class:`~pyglet.image.Texture`
        """
        return self.get_texture()

    def get_mipmapped_texture(self):
        """Retrieve a :py:class:`~pyglet.image.Texture` instance with all mipmap levels filled in.

        Requires that image dimensions be powers of 2. 

        :rtype: :py:class:`~pyglet.image.Texture`

        .. versionadded:: 1.1
        """
        raise ImageException('Cannot retrieve mipmapped texture for %r' % self)

    @property
    def mipmapped_texture(self):
        """A Texture view of this image.  
        
        The returned Texture will have mipmaps filled in for all levels.
        Requires that image dimensions be powers of 2.  Read-only.

        :deprecated: Use `get_mipmapped_texture`.

        :type: :py:class:`~pyglet.image.Texture`
        """
        return self.get_mipmapped_texture()

    def get_region(self, x, y, width, height):
        """Retrieve a rectangular region of this image.

        :Parameters:
            `x` : int
                Left edge of region.
            `y` : int
                Bottom edge of region.
            `width` : int
                Width of region.
            `height` : int
                Height of region.

        :rtype: AbstractImage
        """
        raise ImageException('Cannot get region for %r' % self)

    def save(self, filename=None, file=None, encoder=None):
        """Save this image to a file.

        :Parameters:
            `filename` : str
                Used to set the image file format, and to open the output file
                if `file` is unspecified.
            `file` : file-like object or None
                File to write image data to.
            `encoder` : ImageEncoder or None
                If unspecified, all encoders matching the filename extension
                are tried.  If all fail, the exception from the first one
                attempted is raised.

        """
        if not file:
            file = open(filename, 'wb')

        if encoder:
            encoder.encode(self, file, filename)
        else:
            first_exception = None
            for encoder in _codecs.get_encoders(filename):
                try:
                    encoder.encode(self, file, filename)
                    return
                except _codecs.ImageEncodeException as e:
                    first_exception = first_exception or e
                    file.seek(0)

            if not first_exception:
                raise _codecs.ImageEncodeException(
                    'No image encoders are available')
            raise first_exception

    def blit(self, x, y, z=0):
        """Draw this image to the active framebuffers.
        
        The image will be drawn with the lower-left corner at 
        (``x -`` `anchor_x`, ``y -`` `anchor_y`, ``z``).
        """
        raise ImageException('Cannot blit %r.' % self)

    def blit_into(self, source, x, y, z):
        """Draw `source` on this image.

        `source` will be copied into this image such that its anchor point
        is aligned with the `x` and `y` parameters.  If this image is a 3D
        texture, the `z` coordinate gives the image slice to copy into.
        
        Note that if `source` is larger than this image (or the positioning
        would cause the copy to go out of bounds) then you must pass a
        region of `source` to this method, typically using get_region().
        """
        raise ImageException('Cannot blit images onto %r.' % self)

    def blit_to_texture(self, target, level, x, y, z=0):
        """Draw this image on the currently bound texture at `target`.
        
        This image is copied into the texture such that this image's anchor
        point is aligned with the given `x` and `y` coordinates of the
        destination texture.  If the currently bound texture is a 3D texture,
        the `z` coordinate gives the image slice to blit into.
        """
        raise ImageException('Cannot blit %r to a texture.' % self)


class AbstractImageSequence(object):
    """Abstract sequence of images.

    The sequence is useful for storing image animations or slices of a volume.
    For efficient access, use the `texture_sequence` member.  The class
    also implements the sequence interface (`__len__`, `__getitem__`,
    `__setitem__`).
    """

    def get_texture_sequence(self):
        """Get a TextureSequence.

        :rtype: `TextureSequence`

        .. versionadded:: 1.1
        """
        raise NotImplementedError('abstract')

    @property
    def texture_sequence(self):
        """Access this image sequence as a texture sequence.
        
        :deprecated: Use `get_texture_sequence`

        :type: `TextureSequence`
        """
        return self.get_texture_sequence()

    def get_animation(self, period, loop=True):
        """Create an animation over this image sequence for the given constant
        framerate.

        :Parameters
            `period` : float
                Number of seconds to display each frame.
            `loop` : bool
                If True, the animation will loop continuously.

        :rtype: :py:class:`~pyglet.image.Animation`

        .. versionadded:: 1.1
        """
        return Animation.from_image_sequence(self, period, loop)

    def __getitem__(self, slice):
        """Retrieve a (list of) image.
        
        :rtype: AbstractImage
        """
        raise NotImplementedError('abstract')

    def __setitem__(self, slice, image):
        """Replace one or more images in the sequence.
        
        :Parameters:
            `image` : `~pyglet.image.AbstractImage`
                The replacement image.  The actual instance may not be used,
                depending on this implementation.

        """
        raise NotImplementedError('abstract')

    def __len__(self):
        raise NotImplementedError('abstract')

    def __iter__(self):
        """Iterate over the images in sequence.

        :rtype: Iterator

        .. versionadded:: 1.1
        """
        raise NotImplementedError('abstract')


class TextureSequence(AbstractImageSequence):
    """Interface for a sequence of textures.

    Typical implementations store multiple :py:class:`~pyglet.image.TextureRegion` s within one
    :py:class:`~pyglet.image.Texture` so as to minimise state changes.
    """

    def get_texture_sequence(self):
        return self


class UniformTextureSequence(TextureSequence):
    """Interface for a sequence of textures, each with the same dimensions.

    :Ivariables:
        `item_width` : int
            Width of each texture in the sequence.
        `item_height` : int
            Height of each texture in the sequence.
    
    """

    def _get_item_width(self):
        raise NotImplementedError('abstract')

    def _get_item_height(self):
        raise NotImplementedError('abstract')

    @property
    def item_width(self):
        return self._get_item_width()

    @property
    def item_height(self):
        return self._get_item_height()


class ImageData(AbstractImage):
    """An image represented as a string of unsigned bytes.

    :Ivariables:
        `data` : str
            Pixel data, encoded according to `format` and `pitch`.
        `format` : str
            The format string to use when reading or writing `data`.
        `pitch` : int
            Number of bytes per row.  Negative values indicate a top-to-bottom
            arrangement.

    Setting the `format` and `pitch` instance variables and reading `data` is
    deprecated; use `get_data` and `set_data` in new applications.  (Reading
    `format` and `pitch` to obtain the current encoding is not deprecated).
    """

    _swap1_pattern = re.compile(asbytes('(.)'), re.DOTALL)
    _swap2_pattern = re.compile(asbytes('(.)(.)'), re.DOTALL)
    _swap3_pattern = re.compile(asbytes('(.)(.)(.)'), re.DOTALL)
    _swap4_pattern = re.compile(asbytes('(.)(.)(.)(.)'), re.DOTALL)

    _current_texture = None
    _current_mipmap_texture = None

    def __init__(self, width, height, format, data, pitch=None):
        """Initialise image data.

        :Parameters:
            `width` : int
                Width of image data
            `height` : int
                Height of image data
            `format` : str
                A valid format string, such as 'RGB', 'RGBA', 'ARGB', etc.
            `data` : sequence
                String or array/list of bytes giving the decoded data.
            `pitch` : int or None
                If specified, the number of bytes per row.  Negative values
                indicate a top-to-bottom arrangement.  Defaults to 
                ``width * len(format)``.

        """
        super(ImageData, self).__init__(width, height)

        self._current_format = self._desired_format = format.upper()
        self._current_data = data
        if not pitch:
            pitch = width * len(format)
        self._current_pitch = self.pitch = pitch
        self.mipmap_images = []

    def __getstate__(self):
        return {
            'width': self.width,
            'height': self.height,
            '_current_data': self.get_data(self._current_format, self._current_pitch),
            '_current_format': self._current_format,
            '_desired_format': self._desired_format,
            '_current_pitch': self._current_pitch,
            'pitch': self.pitch,
            'mipmap_images': self.mipmap_images
        }

    def get_image_data(self):
        return self

    def _set_format(self, fmt):
        self._desired_format = fmt.upper()
        self._current_texture = None

    @property
    def format(self):
        """Format string of the data.  Read-write.
        
        :type: str
        """
        return self._desired_format

    @format.setter
    def format(self, fmt):
        self._set_format(fmt)

    def _get_data(self):
        if self._current_pitch != self.pitch or \
                        self._current_format != self.format:
            self._current_data = self._convert(self.format, self.pitch)
            self._current_format = self.format
            self._current_pitch = self.pitch

        self._ensure_string_data()
        return self._current_data

    def _set_data(self, data):
        self._current_data = data
        self._current_format = self.format
        self._current_pitch = self.pitch
        self._current_texture = None
        self._current_mipmapped_texture = None

    @property
    def data(self):
        """The byte data of the image.  Read-write.

        :deprecated: Use `get_data` and `set_data`.
        
        :type: sequence of bytes, or str
        """
        return self._get_data()

    @data.setter
    def data(self, data):
        self._set_data(data)

    def get_data(self, format, pitch):
        """Get the byte data of the image.

        :Parameters:
            `format` : str
                Format string of the return data.
            `pitch` : int
                Number of bytes per row.  Negative values indicate a
                top-to-bottom arrangement.

        .. versionadded:: 1.1

        :rtype: sequence of bytes, or str
        """
        if format == self._current_format and pitch == self._current_pitch:
            return self._current_data
        return self._convert(format, pitch)

    def set_data(self, format, pitch, data):
        """Set the byte data of the image.

        :Parameters:
            `format` : str
                Format string of the return data.
            `pitch` : int
                Number of bytes per row.  Negative values indicate a
                top-to-bottom arrangement.
            `data` : str or sequence of bytes
                Image data.

        .. versionadded:: 1.1
        """
        self._current_format = format
        self._current_pitch = pitch
        self._current_data = data
        self._current_texture = None
        self._current_mipmapped_texture = None

    def set_mipmap_image(self, level, image):
        """Set a mipmap image for a particular level.

        The mipmap image will be applied to textures obtained via
        `get_mipmapped_texture`.

        :Parameters:
            `level` : int
                Mipmap level to set image at, must be >= 1.
            `image` : AbstractImage
                Image to set.  Must have correct dimensions for that mipmap
                level (i.e., width >> level, height >> level)
        """

        if level == 0:
            raise ImageException(
                'Cannot set mipmap image at level 0 (it is this image)')

        if not _is_pow2(self.width) or not _is_pow2(self.height):
            raise ImageException(
                'Image dimensions must be powers of 2 to use mipmaps.')

        # Check dimensions of mipmap
        width, height = self.width, self.height
        for i in range(level):
            width >>= 1
            height >>= 1
        if width != image.width or height != image.height:
            raise ImageException(
                'Mipmap image has wrong dimensions for level %d' % level)

        # Extend mipmap_images list to required level
        self.mipmap_images += [None] * (level - len(self.mipmap_images))
        self.mipmap_images[level - 1] = image

    def create_texture(self, cls, rectangle=False, force_rectangle=False):
        """Create a texture containing this image.

        If the image's dimensions are not powers of 2, a TextureRegion of
        a larger Texture will be returned that matches the dimensions of this
        image.

        :Parameters:
            `cls` : class (subclass of Texture)
                Class to construct.
            `rectangle` : bool
                ``True`` if a rectangle can be created; see
                `AbstractImage.get_texture`.

                .. versionadded:: 1.1
            `force_rectangle` : bool
                ``True`` if a rectangle must be created; see
                `AbstractImage.get_texture`.

                .. versionadded:: 1.1.4

        :rtype: cls or cls.region_class
        """
        internalformat = self._get_internalformat(self.format)
        texture = cls.create(self.width, self.height, internalformat,
                             rectangle, force_rectangle)
        if self.anchor_x or self.anchor_y:
            texture.anchor_x = self.anchor_x
            texture.anchor_y = self.anchor_y

        self.blit_to_texture(texture.target, texture.level,
                             self.anchor_x, self.anchor_y, 0, None)

        return texture

    def get_texture(self, rectangle=False, force_rectangle=False):
        if (not self._current_texture or
                (not self._current_texture._is_rectangle and force_rectangle)):
            self._current_texture = self.create_texture(Texture,
                                                        rectangle,
                                                        force_rectangle)
        return self._current_texture

    def get_mipmapped_texture(self):
        """Return a Texture with mipmaps.  
        
        If :py:class:`~pyglet.image.set_mipmap_Image` has been called with at least one image, the set
        of images defined will be used.  Otherwise, mipmaps will be
        automatically generated.

        The texture dimensions must be powers of 2 to use mipmaps.

        :rtype: :py:class:`~pyglet.image.Texture`

        .. versionadded:: 1.1
        """
        if self._current_mipmap_texture:
            return self._current_mipmap_texture

        if not _is_pow2(self.width) or not _is_pow2(self.height):
            raise ImageException(
                'Image dimensions must be powers of 2 to use mipmaps.')

        texture = Texture.create_for_size(GL_TEXTURE_2D, self.width, self.height)
        if self.anchor_x or self.anchor_y:
            texture.anchor_x = self.anchor_x
            texture.anchor_y = self.anchor_y

        internalformat = self._get_internalformat(self.format)

        glBindTexture(texture.target, texture.id)
        glTexParameteri(texture.target, GL_TEXTURE_MIN_FILTER, GL_LINEAR_MIPMAP_LINEAR)

        if self.mipmap_images:
            self.blit_to_texture(texture.target, texture.level,
                                 self.anchor_x, self.anchor_y, 0, internalformat)
            level = 0
            for image in self.mipmap_images:
                level += 1
                if image:
                    image.blit_to_texture(texture.target, level,
                                          self.anchor_x, self.anchor_y, 0, internalformat)
                    # TODO: should set base and max mipmap level if some mipmaps
                    # are missing.
        elif gl_info.have_version(1, 4):
            glTexParameteri(texture.target, GL_GENERATE_MIPMAP, GL_TRUE)
            self.blit_to_texture(texture.target, texture.level,
                                 self.anchor_x, self.anchor_y, 0, internalformat)
        else:
            raise NotImplementedError('TODO: gluBuild2DMipmaps')

        self._current_mipmap_texture = texture
        return texture

    def get_region(self, x, y, width, height):
        """Retrieve a rectangular region of this image data.

        :Parameters:
            `x` : int
                Left edge of region.
            `y` : int
                Bottom edge of region.
            `width` : int
                Width of region.
            `height` : int
                Height of region.

        :rtype: ImageDataRegion
        """
        return ImageDataRegion(x, y, width, height, self)

    def blit(self, x, y, z=0, width=None, height=None):
        self.get_texture().blit(x, y, z, width, height)

    def blit_to_texture(self, target, level, x, y, z, internalformat=None):
        """Draw this image to to the currently bound texture at `target`.

        This image's anchor point will be aligned to the given `x` and `y`
        coordinates.  If the currently bound texture is a 3D texture, the `z`
        parameter gives the image slice to blit into.

        If `internalformat` is specified, glTexImage is used to initialise
        the texture; otherwise, glTexSubImage is used to update a region.
        """
        x -= self.anchor_x
        y -= self.anchor_y

        data_format = self.format
        data_pitch = abs(self._current_pitch)

        # Determine pixel format from format string
        matrix = None
        format, type = self._get_gl_format_and_type(data_format)
        if format is None:
            if (len(data_format) in (3, 4) and
                    gl_info.have_extension('GL_ARB_imaging')):
                # Construct a color matrix to convert to GL_RGBA
                def component_column(component):
                    try:
                        pos = 'RGBA'.index(component)
                        return [0] * pos + [1] + [0] * (3 - pos)
                    except ValueError:
                        return [0, 0, 0, 0]

                # pad to avoid index exceptions
                lookup_format = data_format + 'XXX'
                matrix = (component_column(lookup_format[0]) +
                          component_column(lookup_format[1]) +
                          component_column(lookup_format[2]) +
                          component_column(lookup_format[3]))
                format = {
                    3: GL_RGB,
                    4: GL_RGBA}.get(len(data_format))
                type = GL_UNSIGNED_BYTE

                glMatrixMode(GL_COLOR)
                glPushMatrix()
                glLoadMatrixf((GLfloat * 16)(*matrix))
            else:
                # Need to convert data to a standard form
                data_format = {
                    1: 'L',
                    2: 'LA',
                    3: 'RGB',
                    4: 'RGBA'}.get(len(data_format))
                format, type = self._get_gl_format_and_type(data_format)

        # Workaround: don't use GL_UNPACK_ROW_LENGTH
        if gl.current_context._workaround_unpack_row_length:
            data_pitch = self.width * len(data_format)

        # Get data in required format (hopefully will be the same format it's
        # already in, unless that's an obscure format, upside-down or the
        # driver is old).
        data = self._convert(data_format, data_pitch)

        if data_pitch & 0x1:
            alignment = 1
        elif data_pitch & 0x2:
            alignment = 2
        else:
            alignment = 4
        row_length = data_pitch // len(data_format)
        glPushClientAttrib(GL_CLIENT_PIXEL_STORE_BIT)
        glPixelStorei(GL_UNPACK_ALIGNMENT, alignment)
        glPixelStorei(GL_UNPACK_ROW_LENGTH, row_length)
        self._apply_region_unpack()

        if target == GL_TEXTURE_3D:
            assert not internalformat
            glTexSubImage3D(target, level,
                            x, y, z,
                            self.width, self.height, 1,
                            format, type,
                            data)
        elif internalformat:
            glTexImage2D(target, level,
                         internalformat,
                         self.width, self.height,
                         0,
                         format, type,
                         data)
        else:
            glTexSubImage2D(target, level,
                            x, y,
                            self.width, self.height,
                            format, type,
                            data)
        glPopClientAttrib()

        if matrix:
            glPopMatrix()
            glMatrixMode(GL_MODELVIEW)

        # Flush image upload before data get GC'd.
        glFlush()

    def _apply_region_unpack(self):
        pass

    def _convert(self, format, pitch):
        """Return data in the desired format; does not alter this instance's
        current format or pitch.
        """
        if format == self._current_format and pitch == self._current_pitch:
            if type(self._current_data) is str:
                return asbytes(self._current_data)
            return self._current_data

        self._ensure_string_data()
        data = self._current_data
        current_pitch = self._current_pitch
        current_format = self._current_format
        sign_pitch = current_pitch // abs(current_pitch)
        if format != self._current_format:
            # Create replacement string, e.g. r'\4\1\2\3' to convert RGBA to
            # ARGB
            repl = asbytes('')
            for c in format:
                try:
                    idx = current_format.index(c) + 1
                except ValueError:
                    idx = 1
                repl += asbytes(r'\%d' % idx)

            if len(current_format) == 1:
                swap_pattern = self._swap1_pattern
            elif len(current_format) == 2:
                swap_pattern = self._swap2_pattern
            elif len(current_format) == 3:
                swap_pattern = self._swap3_pattern
            elif len(current_format) == 4:
                swap_pattern = self._swap4_pattern
            else:
                raise ImageException(
                    'Current image format is wider than 32 bits.')

            packed_pitch = self.width * len(current_format)
            if abs(self._current_pitch) != packed_pitch:
                # Pitch is wider than pixel data, need to go row-by-row.
                rows = re.findall(
                    asbytes('.') * abs(self._current_pitch), data, re.DOTALL)
                rows = [swap_pattern.sub(repl, r[:packed_pitch]) for r in rows]
                data = asbytes('').join(rows)
            else:
                # Rows are tightly packed, apply regex over whole image.
                data = swap_pattern.sub(repl, data)

            # After conversion, rows will always be tightly packed
            current_pitch = sign_pitch * (len(format) * self.width)

        if pitch != current_pitch:
            diff = abs(current_pitch) - abs(pitch)
            if diff > 0:
                # New pitch is shorter than old pitch, chop bytes off each row
                pattern = re.compile(
                    asbytes('(%s)%s' % ('.' * abs(pitch), '.' * diff)), re.DOTALL)
                data = pattern.sub(asbytes(r'\1'), data)
            elif diff < 0:
                # New pitch is longer than old pitch, add '0' bytes to each row
                pattern = re.compile(
                    asbytes('(%s)' % ('.' * abs(current_pitch))), re.DOTALL)
                pad = '.' * -diff
                data = pattern.sub(asbytes(r'\1%s' % pad), data)

            if current_pitch * pitch < 0:
                # Pitch differs in sign, swap row order
                rows = re.findall(asbytes('.') * abs(pitch), data, re.DOTALL)
                rows.reverse()
                data = asbytes('').join(rows)

        return asbytes(data)

    def _ensure_string_data(self):
        if type(self._current_data) is not bytes_type:
            buf = create_string_buffer(len(self._current_data))
            memmove(buf, self._current_data, len(self._current_data))
            self._current_data = buf.raw

    def _get_gl_format_and_type(self, format):
        if format == 'I':
            return GL_LUMINANCE, GL_UNSIGNED_BYTE
        elif format == 'L':
            return GL_LUMINANCE, GL_UNSIGNED_BYTE
        elif format == 'LA':
            return GL_LUMINANCE_ALPHA, GL_UNSIGNED_BYTE
        elif format == 'R':
            return GL_RED, GL_UNSIGNED_BYTE
        elif format == 'G':
            return GL_GREEN, GL_UNSIGNED_BYTE
        elif format == 'B':
            return GL_BLUE, GL_UNSIGNED_BYTE
        elif format == 'A':
            return GL_ALPHA, GL_UNSIGNED_BYTE
        elif format == 'RGB':
            return GL_RGB, GL_UNSIGNED_BYTE
        elif format == 'RGBA':
            return GL_RGBA, GL_UNSIGNED_BYTE
        elif (format == 'ARGB' and
                  gl_info.have_extension('GL_EXT_bgra') and
                  gl_info.have_extension('GL_APPLE_packed_pixels')):
            return GL_BGRA, GL_UNSIGNED_INT_8_8_8_8_REV
        elif (format == 'ABGR' and
                  gl_info.have_extension('GL_EXT_abgr')):
            return GL_ABGR_EXT, GL_UNSIGNED_BYTE
        elif (format == 'BGR' and
                  gl_info.have_extension('GL_EXT_bgra')):
            return GL_BGR, GL_UNSIGNED_BYTE
        elif (format == 'BGRA' and
                  gl_info.have_extension('GL_EXT_bgra')):
            return GL_BGRA, GL_UNSIGNED_BYTE

        return None, None

    def _get_internalformat(self, format):
        if len(format) == 4:
            return GL_RGBA
        elif len(format) == 3:
            return GL_RGB
        elif len(format) == 2:
            return GL_LUMINANCE_ALPHA
        elif format == 'A':
            return GL_ALPHA
        elif format == 'L':
            return GL_LUMINANCE
        elif format == 'I':
            return GL_INTENSITY
        return GL_RGBA


class ImageDataRegion(ImageData):
    def __init__(self, x, y, width, height, image_data):
        super(ImageDataRegion, self).__init__(width, height,
                                              image_data._current_format, image_data._current_data,
                                              image_data._current_pitch)
        self.x = x
        self.y = y

    def __getstate__(self):
        return {
            'width': self.width,
            'height': self.height,
            '_current_data':
                self.get_data(self._current_format, self._current_pitch),
            '_current_format': self._current_format,
            '_desired_format': self._desired_format,
            '_current_pitch': self._current_pitch,
            'pitch': self.pitch,
            'mipmap_images': self.mipmap_images,
            'x': self.x,
            'y': self.y
        }

    def _get_data(self):
        # Crop the data first
        x1 = len(self._current_format) * self.x
        x2 = len(self._current_format) * (self.x + self.width)

        self._ensure_string_data()
        data = self._convert(self._current_format, abs(self._current_pitch))
        rows = re.findall(b'.' * abs(self._current_pitch), data, re.DOTALL)
        rows = [row[x1:x2] for row in rows[self.y:self.y + self.height]]
        self._current_data = b''.join(rows)
        self._current_pitch = self.width * len(self._current_format)
        self._current_texture = None
        self.x = 0
        self.y = 0

        return super(ImageDataRegion, self)._get_data()

    def _set_data(self, data):
        self.x = 0
        self.y = 0
        super(ImageDataRegion, self)._set_data(data)

    @property
    def data(self):
        return self._get_data()

    @data.setter
    def data(self, data):
        self._set_data(data)

    def get_data(self, format, pitch):
        x1 = len(self._current_format) * self.x
        x2 = len(self._current_format) * (self.x + self.width)

        self._ensure_string_data()
        data = self._convert(self._current_format, abs(self._current_pitch))
        rows = re.findall(asbytes('.') * abs(self._current_pitch), data,
                          re.DOTALL)
        rows = [row[x1:x2] for row in rows[self.y:self.y + self.height]]
        self._current_data = asbytes('').join(rows)
        self._current_pitch = self.width * len(self._current_format)
        self._current_texture = None
        self.x = 0
        self.y = 0

        return super(ImageDataRegion, self).get_data(format, pitch)

    def _apply_region_unpack(self):
        glPixelStorei(GL_UNPACK_SKIP_PIXELS, self.x)
        glPixelStorei(GL_UNPACK_SKIP_ROWS, self.y)

    def _ensure_string_data(self):
        super(ImageDataRegion, self)._ensure_string_data()

    def get_region(self, x, y, width, height):
        x += self.x
        y += self.y
        return super(ImageDataRegion, self).get_region(x, y, width, height)


class CompressedImageData(AbstractImage):
    """Image representing some compressed data suitable for direct uploading
    to driver.
    """

    _current_texture = None
    _current_mipmapped_texture = None

    def __init__(self, width, height, gl_format, data,
                 extension=None, decoder=None):
        """Construct a CompressedImageData with the given compressed data.

        :Parameters:
            `width` : int
                Width of image
            `height` : int
                Height of image
            `gl_format` : int
                GL constant giving format of compressed data; for example,
                ``GL_COMPRESSED_RGBA_S3TC_DXT5_EXT``.
            `data` : sequence
                String or array/list of bytes giving compressed image data.
            `extension` : str or None
                If specified, gives the name of a GL extension to check for
                before creating a texture.
            `decoder` : function(data, width, height) -> AbstractImage
                A function to decode the compressed data, to be used if the
                required extension is not present.
                
        """
        if not _is_pow2(width) or not _is_pow2(height):
            raise ImageException('Dimensions of %r must be powers of 2' % self)

        super(CompressedImageData, self).__init__(width, height)
        self.data = data
        self.gl_format = gl_format
        self.extension = extension
        self.decoder = decoder
        self.mipmap_data = []

    def set_mipmap_data(self, level, data):
        """Set data for a mipmap level.

        Supplied data gives a compressed image for the given mipmap level.
        The image must be of the correct dimensions for the level 
        (i.e., width >> level, height >> level); but this is not checked.  If
        any mipmap levels are specified, they are used; otherwise, mipmaps for
        `mipmapped_texture` are generated automatically.

        :Parameters:
            `level` : int
                Level of mipmap image to set.
            `data` : sequence
                String or array/list of bytes giving compressed image data.
                Data must be in same format as specified in constructor.

        """
        # Extend mipmap_data list to required level
        self.mipmap_data += [None] * (level - len(self.mipmap_data))
        self.mipmap_data[level - 1] = data

    def _have_extension(self):
        return self.extension is None or gl_info.have_extension(self.extension)

    def _verify_driver_supported(self):
        """Assert that the extension required for this image data is
        supported.

        Raises `ImageException` if not.
        """

        if not self._have_extension():
            raise ImageException('%s is required to decode %r' % \
                                 (self.extension, self))

    def get_texture(self, rectangle=False, force_rectangle=False):
        if force_rectangle:
            raise ImageException(
                'Compressed texture rectangles not supported')

        if self._current_texture:
            return self._current_texture

        texture = Texture.create_for_size(
            GL_TEXTURE_2D, self.width, self.height)
        if self.anchor_x or self.anchor_y:
            texture.anchor_x = self.anchor_x
            texture.anchor_y = self.anchor_y

        glBindTexture(texture.target, texture.id)
        glTexParameteri(texture.target, GL_TEXTURE_MIN_FILTER, texture.min_filter)
        glTexParameteri(texture.target, GL_TEXTURE_MAG_FILTER, texture.mag_filter)

        if self._have_extension():
            glCompressedTexImage2DARB(texture.target, texture.level,
                                      self.gl_format,
                                      self.width, self.height, 0,
                                      len(self.data), self.data)
        else:
            image = self.decoder(self.data, self.width, self.height)
            texture = image.get_texture()
            assert texture.width == self.width
            assert texture.height == self.height

        glFlush()
        self._current_texture = texture
        return texture

    def get_mipmapped_texture(self):
        if self._current_mipmap_texture:
            return self._current_mipmap_texture

        if not self._have_extension():
            # TODO mip-mapped software decoded compressed textures.  For now,
            # just return a non-mipmapped texture.
            return self.get_texture()

        texture = Texture.create_for_size(
            GL_TEXTURE_2D, self.width, self.height)
        if self.anchor_x or self.anchor_y:
            texture.anchor_x = self.anchor_x
            texture.anchor_y = self.anchor_y

        glBindTexture(texture.target, texture.id)

        glTexParameteri(texture.target, GL_TEXTURE_MIN_FILTER,
                        GL_LINEAR_MIPMAP_LINEAR)

        if not self.mipmap_data:
            if not gl_info.have_version(1, 4):
                raise ImageException(
                    'Require GL 1.4 to generate mipmaps for compressed textures')
            glTexParameteri(texture.target, GL_GENERATE_MIPMAP, GL_TRUE)

        glCompressedTexImage2DARB(texture.target, texture.level,
                                  self.gl_format,
                                  self.width, self.height, 0,
                                  len(self.data), self.data)

        width, height = self.width, self.height
        level = 0
        for data in self.mipmap_data:
            width >>= 1
            height >>= 1
            level += 1
            glCompressedTexImage2DARB(texture.target, level,
                                      self.gl_format,
                                      width, height, 0,
                                      len(data), data)

        glFlush()

        self._current_mipmap_texture = texture
        return texture

    def blit_to_texture(self, target, level, x, y, z):
        self._verify_driver_supported()

        if target == GL_TEXTURE_3D:
            glCompressedTexSubImage3DARB(target, level,
                                         x - self.anchor_x, y - self.anchor_y, z,
                                         self.width, self.height, 1,
                                         self.gl_format,
                                         len(self.data), self.data)
        else:
            glCompressedTexSubImage2DARB(target, level,
                                         x - self.anchor_x, y - self.anchor_y,
                                         self.width, self.height,
                                         self.gl_format,
                                         len(self.data), self.data)


def _nearest_pow2(v):
    # From http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
    # Credit: Sean Anderson
    v -= 1
    v |= v >> 1
    v |= v >> 2
    v |= v >> 4
    v |= v >> 8
    v |= v >> 16
    return v + 1


def _is_pow2(v):
    # http://graphics.stanford.edu/~seander/bithacks.html#DetermineIfPowerOf2
    return (v & (v - 1)) == 0


class Texture(AbstractImage):
    """An image loaded into video memory that can be efficiently drawn
    to the framebuffer.

    Typically you will get an instance of Texture by accessing the `texture`
    member of any other AbstractImage.

    :Ivariables:
        `region_class` : class (subclass of TextureRegion)
            Class to use when constructing regions of this texture.
        `tex_coords` : tuple
            12-tuple of float, named (u1, v1, r1, u2, v2, r2, ...).  u, v, r
            give the 3D texture coordinates for vertices 1-4.  The vertices
            are specified in the order bottom-left, bottom-right, top-right
            and top-left.
        `target` : int
            The GL texture target (e.g., ``GL_TEXTURE_2D``).
        `level` : int
            The mipmap level of this texture.

    """

    region_class = None  # Set to TextureRegion after it's defined
    tex_coords = (0., 0., 0., 1., 0., 0., 1., 1., 0., 0., 1., 0.)
    tex_coords_order = (0, 1, 2, 3)
    level = 0
    images = 1
    x = y = z = 0

    def __init__(self, width, height, target, id):
        super(Texture, self).__init__(width, height)
        self.target = target
        self.id = id
        self._context = gl.current_context

    def delete(self):
        """Delete the texture from video memory.

        :deprecated: Textures are automatically released during object
            finalization.
        """
        warnings.warn(
            'Texture.delete() is deprecated; textures are '
            'released through GC now')
        self._context.delete_texture(self.id)
        self.id = 0

    def __del__(self):
        try:
            self._context.delete_texture(self.id)
        except:
            pass

    @classmethod
    def create(cls, width, height, internalformat=GL_RGBA,
               rectangle=False, force_rectangle=False, min_filter=GL_LINEAR, mag_filter=GL_LINEAR):
        """Create an empty Texture.

        If `rectangle` is ``False`` or the appropriate driver extensions are
        not available, a larger texture than requested will be created, and
        a :py:class:`~pyglet.image.TextureRegion` corresponding to the requested size will be
        returned.

        :Parameters:
            `width` : int
                Width of the texture.
            `height` : int
                Height of the texture.
            `internalformat` : int
                GL constant giving the internal format of the texture; for
                example, ``GL_RGBA``.
            `rectangle` : bool
                ``True`` if a rectangular texture is permitted.  See
                `AbstractImage.get_texture`.
            `force_rectangle` : bool
                ``True`` if a rectangular texture is required.  See
                `AbstractImage.get_texture`.  
                
                .. versionadded:: 1.1.4.
            `min_filter` : int
                The minifaction filter used for this texture, commonly ``GL_LINEAR`` or ``GL_NEAREST``
            `mag_filter` : int
                The magnification filter used for this texture, commonly ``GL_LINEAR`` or ``GL_NEAREST``

        :rtype: :py:class:`~pyglet.image.Texture`
        
        .. versionadded:: 1.1
        """
        target = GL_TEXTURE_2D
        if rectangle or force_rectangle:
            if not force_rectangle and _is_pow2(width) and _is_pow2(height):
                rectangle = False
            elif gl_info.have_extension('GL_ARB_texture_rectangle'):
                target = GL_TEXTURE_RECTANGLE_ARB
                rectangle = True
            elif gl_info.have_extension('GL_NV_texture_rectangle'):
                target = GL_TEXTURE_RECTANGLE_NV
                rectangle = True
            else:
                rectangle = False

        if force_rectangle and not rectangle:
            raise ImageException('Texture rectangle extensions not available')

        if rectangle:
            texture_width = width
            texture_height = height
        else:
            texture_width = _nearest_pow2(width)
            texture_height = _nearest_pow2(height)

        id = GLuint()
        glGenTextures(1, byref(id))
        glBindTexture(target, id.value)
        glTexParameteri(target, GL_TEXTURE_MIN_FILTER, min_filter)
        glTexParameteri(target, GL_TEXTURE_MAG_FILTER, mag_filter)

        blank = (GLubyte * (texture_width * texture_height * 4))()
        glTexImage2D(target, 0,
                     internalformat,
                     texture_width, texture_height,
                     0,
                     GL_RGBA, GL_UNSIGNED_BYTE,
                     blank)

        texture = cls(texture_width, texture_height, target, id.value)
        texture.min_filter = min_filter
        texture.mag_filter = mag_filter
        if rectangle:
            texture._is_rectangle = True
            texture.tex_coords = (0., 0., 0.,
                                  width, 0., 0.,
                                  width, height, 0.,
                                  0., height, 0.)

        glFlush()

        if texture_width == width and texture_height == height:
            return texture

        return texture.get_region(0, 0, width, height)

    @classmethod
    def create_for_size(cls, target, min_width, min_height,
                        internalformat=None, min_filter=GL_LINEAR, mag_filter=GL_LINEAR):
        """Create a Texture with dimensions at least min_width, min_height.
        On return, the texture will be bound.

        :Parameters:
            `target` : int
                GL constant giving texture target to use, typically
                ``GL_TEXTURE_2D``.
            `min_width` : int
                Minimum width of texture (may be increased to create a power
                of 2).
            `min_height` : int
                Minimum height of texture (may be increased to create a power
                of 2).
            `internalformat` : int
                GL constant giving internal format of texture; for example,
                ``GL_RGBA``.  If unspecified, the texture will not be
                initialised (only the texture name will be created on the
                instance).   If specified, the image will be initialised
                to this format with zero'd data.
            `min_filter` : int
                The minifaction filter used for this texture, commonly ``GL_LINEAR`` or ``GL_NEAREST``
            `mag_filter` : int
                The magnification filter used for this texture, commonly ``GL_LINEAR`` or ``GL_NEAREST``

        :rtype: :py:class:`~pyglet.image.Texture`
        """
        if target not in (GL_TEXTURE_RECTANGLE_NV, GL_TEXTURE_RECTANGLE_ARB):
            width = _nearest_pow2(min_width)
            height = _nearest_pow2(min_height)
            tex_coords = cls.tex_coords
        else:
            width = min_width
            height = min_height
            tex_coords = (0., 0., 0.,
                          width, 0., 0.,
                          width, height, 0.,
                          0., height, 0.)
        id = GLuint()
        glGenTextures(1, byref(id))
        glBindTexture(target, id.value)
        glTexParameteri(target, GL_TEXTURE_MIN_FILTER, min_filter)
        glTexParameteri(target, GL_TEXTURE_MAG_FILTER, mag_filter)

        if internalformat is not None:
            blank = (GLubyte * (width * height * 4))()
            glTexImage2D(target, 0,
                         internalformat,
                         width, height,
                         0,
                         GL_RGBA, GL_UNSIGNED_BYTE,
                         blank)
            glFlush()

        texture = cls(width, height, target, id.value)
        texture.min_filter = min_filter
        texture.mag_filter = mag_filter
        texture.tex_coords = tex_coords
        return texture

    def get_image_data(self, z=0):
        """Get the image data of this texture.

        Changes to the returned instance will not be reflected in this
        texture.

        :Parameters:
            `z` : int
                For 3D textures, the image slice to retrieve.

        :rtype: :py:class:`~pyglet.image.ImageData`
        """
        glBindTexture(self.target, self.id)

        # Always extract complete RGBA data.  Could check internalformat
        # to only extract used channels. XXX
        format = 'RGBA'
        gl_format = GL_RGBA

        glPushClientAttrib(GL_CLIENT_PIXEL_STORE_BIT)
        glPixelStorei(GL_PACK_ALIGNMENT, 1)
        buffer = \
            (GLubyte * (self.width * self.height * self.images * len(format)))()
        glGetTexImage(self.target, self.level,
                      gl_format, GL_UNSIGNED_BYTE, buffer)
        glPopClientAttrib()

        data = ImageData(self.width, self.height, format, buffer)
        if self.images > 1:
            data = data.get_region(0, z * self.height, self.width, self.height)
        return data

    @property
    def image_data(self):
        """An ImageData view of this texture.  
        
        Changes to the returned instance will not be reflected in this
        texture.  If the texture is a 3D texture, the first image will be 
        returned.  See also :py:meth:`~pyglet.image.ImageData.get_image_data`.  Read-only.

        :deprecated: Use :py:meth:`~pyglet.image.ImageData.get_image_data`.
        
        :type: :py:class:`~pyglet.image.ImageData`
        """
        return self.get_image_data()

    def get_texture(self, rectangle=False, force_rectangle=False):
        if force_rectangle and not self._is_rectangle:
            raise ImageException('Texture is not a rectangle.')
        return self

    # no implementation of blit_to_texture yet (could use aux buffer)

    def blit(self, x, y, z=0, width=None, height=None):
        t = self.tex_coords
        x1 = x - self.anchor_x
        y1 = y - self.anchor_y
        x2 = x1 + (width is None and self.width or width)
        y2 = y1 + (height is None and self.height or height)
        array = (GLfloat * 32)(
            t[0], t[1], t[2], 1.,
            x1, y1, z, 1.,
            t[3], t[4], t[5], 1.,
            x2, y1, z, 1.,
            t[6], t[7], t[8], 1.,
            x2, y2, z, 1.,
            t[9], t[10], t[11], 1.,
            x1, y2, z, 1.)

        glPushAttrib(GL_ENABLE_BIT)
        glEnable(self.target)
        glBindTexture(self.target, self.id)
        glPushClientAttrib(GL_CLIENT_VERTEX_ARRAY_BIT)
        glInterleavedArrays(GL_T4F_V4F, 0, array)
        glDrawArrays(GL_QUADS, 0, 4)
        glPopClientAttrib()
        glPopAttrib()

    def blit_into(self, source, x, y, z):
        glBindTexture(self.target, self.id)
        source.blit_to_texture(self.target, self.level, x, y, z)

    def get_region(self, x, y, width, height):
        return self.region_class(x, y, 0, width, height, self)

    def get_transform(self, flip_x=False, flip_y=False, rotate=0):
        """Create a copy of this image applying a simple transformation.

        The transformation is applied to the texture coordinates only;
        :py:meth:`~pyglet.image.ImageData.get_image_data` will return the untransformed data.  The
        transformation is applied around the anchor point.

        :Parameters:
            `flip_x` : bool
                If True, the returned image will be flipped horizontally.
            `flip_y` : bool
                If True, the returned image will be flipped vertically.
            `rotate` : int
                Degrees of clockwise rotation of the returned image.  Only 
                90-degree increments are supported.

        :rtype: :py:class:`~pyglet.image.TextureRegion`
        """
        transform = self.get_region(0, 0, self.width, self.height)
        bl, br, tr, tl = 0, 1, 2, 3
        transform.anchor_x = self.anchor_x
        transform.anchor_y = self.anchor_y
        if flip_x:
            bl, br, tl, tr = br, bl, tr, tl
            transform.anchor_x = self.width - self.anchor_x
        if flip_y:
            bl, br, tl, tr = tl, tr, bl, br
            transform.anchor_y = self.height - self.anchor_y
        rotate %= 360
        if rotate < 0:
            rotate += 360
        if rotate == 0:
            pass
        elif rotate == 90:
            bl, br, tr, tl = br, tr, tl, bl
            transform.anchor_x, transform.anchor_y = \
                transform.anchor_y, \
                transform.width - transform.anchor_x
        elif rotate == 180:
            bl, br, tr, tl = tr, tl, bl, br
            transform.anchor_x = transform.width - transform.anchor_x
            transform.anchor_y = transform.height - transform.anchor_y
        elif rotate == 270:
            bl, br, tr, tl = tl, bl, br, tr
            transform.anchor_x, transform.anchor_y = \
                transform.height - transform.anchor_y, \
                transform.anchor_x
        else:
            assert False, 'Only 90 degree rotations are supported.'
        if rotate in (90, 270):
            transform.width, transform.height = \
                transform.height, transform.width
        transform._set_tex_coords_order(bl, br, tr, tl)
        return transform

    def _set_tex_coords_order(self, bl, br, tr, tl):
        tex_coords = (self.tex_coords[:3],
                      self.tex_coords[3:6],
                      self.tex_coords[6:9],
                      self.tex_coords[9:])
        self.tex_coords = \
            tex_coords[bl] + tex_coords[br] + tex_coords[tr] + tex_coords[tl]

        order = self.tex_coords_order
        self.tex_coords_order = \
            (order[bl], order[br], order[tr], order[tl])


class TextureRegion(Texture):
    """A rectangular region of a texture, presented as if it were
    a separate texture.
    """

    def __init__(self, x, y, z, width, height, owner):
        super(TextureRegion, self).__init__(
            width, height, owner.target, owner.id)

        self.x = x
        self.y = y
        self.z = z
        self.owner = owner
        owner_u1 = owner.tex_coords[0]
        owner_v1 = owner.tex_coords[1]
        owner_u2 = owner.tex_coords[3]
        owner_v2 = owner.tex_coords[7]
        scale_u = owner_u2 - owner_u1
        scale_v = owner_v2 - owner_v1
        u1 = x / owner.width * scale_u + owner_u1
        v1 = y / owner.height * scale_v + owner_v1
        u2 = (x + width) / owner.width * scale_u + owner_u1
        v2 = (y + height) / owner.height * scale_v + owner_v1
        r = z / owner.images + owner.tex_coords[2]
        self.tex_coords = (u1, v1, r, u2, v1, r, u2, v2, r, u1, v2, r)

    def get_image_data(self):
        image_data = self.owner.get_image_data(self.z)
        return image_data.get_region(self.x, self.y, self.width, self.height)

    def get_region(self, x, y, width, height):
        x += self.x
        y += self.y
        region = self.region_class(x, y, self.z, width, height, self.owner)
        region._set_tex_coords_order(*self.tex_coords_order)
        return region

    def blit_into(self, source, x, y, z):
        self.owner.blit_into(source, x + self.x, y + self.y, z + self.z)

    def __del__(self):
        # only the owner Texture should handle deletion
        pass


Texture.region_class = TextureRegion


class Texture3D(Texture, UniformTextureSequence):
    """A texture with more than one image slice.

    Use `create_for_images` or `create_for_image_grid` classmethod to
    construct.
    """
    item_width = 0
    item_height = 0
    items = ()

    @classmethod
    def create_for_images(cls, images, internalformat=GL_RGBA):
        item_width = images[0].width
        item_height = images[0].height
        for image in images:
            if image.width != item_width or image.height != item_height:
                raise ImageException('Images do not have same dimensions.')

        depth = len(images)
        if not gl_info.have_version(2, 0):
            depth = _nearest_pow2(depth)

        texture = cls.create_for_size(GL_TEXTURE_3D, item_width, item_height)
        if images[0].anchor_x or images[0].anchor_y:
            texture.anchor_x = images[0].anchor_x
            texture.anchor_y = images[0].anchor_y

        texture.images = depth

        blank = (GLubyte * (texture.width * texture.height * texture.images))()
        glBindTexture(texture.target, texture.id)
        glTexImage3D(texture.target, texture.level,
                     internalformat,
                     texture.width, texture.height, texture.images, 0,
                     GL_ALPHA, GL_UNSIGNED_BYTE,
                     blank)

        items = []
        for i, image in enumerate(images):
            item = cls.region_class(0, 0, i, item_width, item_height, texture)
            items.append(item)
            image.blit_to_texture(texture.target, texture.level,
                                  image.anchor_x, image.anchor_y, i)

        glFlush()

        texture.items = items
        texture.item_width = item_width
        texture.item_height = item_height
        return texture

    @classmethod
    def create_for_image_grid(cls, grid, internalformat=GL_RGBA):
        return cls.create_for_images(grid[:], internalformat)

    def __len__(self):
        return len(self.items)

    def __getitem__(self, index):
        return self.items[index]

    def __setitem__(self, index, value):
        if type(index) is slice:
            for item, image in zip(self[index], value):
                image.blit_to_texture(self.target, self.level,
                                      image.anchor_x, image.anchor_y, item.z)
        else:
            value.blit_to_texture(self.target, self.level,
                                  value.anchor_x, value.anchor_y, self[index].z)

    def __iter__(self):
        return iter(self.items)


class TileableTexture(Texture):
    """A texture that can be tiled efficiently.

    Use :py:class:`~pyglet.image.create_for_Image` classmethod to construct.
    """

    def __init__(self, width, height, target, id):
        if not _is_pow2(width) or not _is_pow2(height):
            raise ImageException(
                'TileableTexture requires dimensions that are powers of 2')
        super(TileableTexture, self).__init__(width, height, target, id)

    def get_region(self, x, y, width, height):
        raise ImageException('Cannot get region of %r' % self)

    def blit_tiled(self, x, y, z, width, height):
        """Blit this texture tiled over the given area.
        
        The image will be tiled with the bottom-left corner of the destination
        rectangle aligned with the anchor point of this texture.
        """
        u1 = self.anchor_x / self.width
        v1 = self.anchor_y / self.height
        u2 = u1 + width / self.width
        v2 = v1 + height / self.height
        w, h = width, height
        t = self.tex_coords
        array = (GLfloat * 32)(
            u1, v1, t[2], 1.,
            x, y, z, 1.,
            u2, v1, t[5], 1.,
            x + w, y, z, 1.,
            u2, v2, t[8], 1.,
            x + w, y + h, z, 1.,
            u1, v2, t[11], 1.,
            x, y + h, z, 1.)

        glPushAttrib(GL_ENABLE_BIT)
        glEnable(self.target)
        glBindTexture(self.target, self.id)
        glPushClientAttrib(GL_CLIENT_VERTEX_ARRAY_BIT)
        glInterleavedArrays(GL_T4F_V4F, 0, array)
        glDrawArrays(GL_QUADS, 0, 4)
        glPopClientAttrib()
        glPopAttrib()

    @classmethod
    def create_for_image(cls, image):
        if not _is_pow2(image.width) or not _is_pow2(image.height):
            # Potentially unnecessary conversion if a GL format exists.
            image = image.get_image_data()
            texture_width = _nearest_pow2(image.width)
            texture_height = _nearest_pow2(image.height)
            newdata = c_buffer(texture_width * texture_height * 4)
            gluScaleImage(GL_RGBA,
                          image.width, image.height,
                          GL_UNSIGNED_BYTE,
                          image.get_data('RGBA', image.width * 4),
                          texture_width,
                          texture_height,
                          GL_UNSIGNED_BYTE,
                          newdata)
            image = ImageData(texture_width, texture_height, 'RGBA',
                              newdata)

        image = image.get_image_data()
        return image.create_texture(cls)


class DepthTexture(Texture):
    """A texture with depth samples (typically 24-bit)."""

    def blit_into(self, source, x, y, z):
        glBindTexture(self.target, self.id)
        source.blit_to_texture(self.level, x, y, z)


class BufferManager(object):
    """Manages the set of framebuffers for a context.

    Use :py:func:`~pyglet.image.get_buffer_manager` to obtain the instance of this class for the
    current context.
    """

    def __init__(self):
        self.color_buffer = None
        self.depth_buffer = None

        aux_buffers = GLint()
        glGetIntegerv(GL_AUX_BUFFERS, byref(aux_buffers))
        self.free_aux_buffers = [GL_AUX0,
                                 GL_AUX1,
                                 GL_AUX2,
                                 GL_AUX3][:aux_buffers.value]

        stencil_bits = GLint()
        glGetIntegerv(GL_STENCIL_BITS, byref(stencil_bits))
        self.free_stencil_bits = list(range(stencil_bits.value))

        self.refs = []

    def get_viewport(self):
        """Get the current OpenGL viewport dimensions.

        :rtype: 4-tuple of float.
        :return: Left, top, right and bottom dimensions.
        """
        viewport = (GLint * 4)()
        glGetIntegerv(GL_VIEWPORT, viewport)
        return viewport

    def get_color_buffer(self):
        """Get the color buffer.

        :rtype: :py:class:`~pyglet.image.ColorBufferImage`
        """
        viewport = self.get_viewport()
        viewport_width = viewport[2]
        viewport_height = viewport[3]
        if (not self.color_buffer or
                    viewport_width != self.color_buffer.width or
                    viewport_height != self.color_buffer.height):
            self.color_buffer = ColorBufferImage(*viewport)
        return self.color_buffer

    def get_aux_buffer(self):
        """Get a free auxiliary buffer.

        If not aux buffers are available, `ImageException` is raised.  Buffers
        are released when they are garbage collected.
        
        :rtype: :py:class:`~pyglet.image.ColorBufferImage`
        """
        if not self.free_aux_buffers:
            raise ImageException('No free aux buffer is available.')

        gl_buffer = self.free_aux_buffers.pop(0)
        viewport = self.get_viewport()
        buffer = ColorBufferImage(*viewport)
        buffer.gl_buffer = gl_buffer

        def release_buffer(ref, self=self):
            self.free_aux_buffers.insert(0, gl_buffer)

        self.refs.append(weakref.ref(buffer, release_buffer))

        return buffer

    def get_depth_buffer(self):
        """Get the depth buffer.

        :rtype: :py:class:`~pyglet.image.DepthBufferImage`
        """
        viewport = self.get_viewport()
        viewport_width = viewport[2]
        viewport_height = viewport[3]
        if (not self.depth_buffer or
                    viewport_width != self.depth_buffer.width or
                    viewport_height != self.depth_buffer.height):
            self.depth_buffer = DepthBufferImage(*viewport)
        return self.depth_buffer

    def get_buffer_mask(self):
        """Get a free bitmask buffer.

        A bitmask buffer is a buffer referencing a single bit in the stencil
        buffer.  If no bits are free, `ImageException` is raised.  Bits are
        released when the bitmask buffer is garbage collected.

        :rtype: :py:class:`~pyglet.image.BufferImageMask`
        """
        if not self.free_stencil_bits:
            raise ImageException('No free stencil bits are available.')

        stencil_bit = self.free_stencil_bits.pop(0)
        x, y, width, height = self.get_viewport()
        buffer = BufferImageMask(x, y, width, height)
        buffer.stencil_bit = stencil_bit

        def release_buffer(ref, self=self):
            self.free_stencil_bits.insert(0, stencil_bit)

        self.refs.append(weakref.ref(buffer, release_buffer))

        return buffer


def get_buffer_manager():
    """Get the buffer manager for the current OpenGL context.
    
    :rtype: :py:class:`~pyglet.image.BufferManager`
    """
    context = gl.current_context
    if not hasattr(context, 'image_buffer_manager'):
        context.image_buffer_manager = BufferManager()
    return context.image_buffer_manager


# XXX BufferImage could be generalised to support EXT_framebuffer_object's
# renderbuffer.
class BufferImage(AbstractImage):
    """An abstract framebuffer.
    """
    #: The OpenGL read and write target for this buffer.
    gl_buffer = GL_BACK

    #: The OpenGL format constant for image data.
    gl_format = 0

    #: The format string used for image data.
    format = ''

    owner = None

    # TODO: enable methods

    def __init__(self, x, y, width, height):
        self.x = x
        self.y = y
        self.width = width
        self.height = height

    def get_image_data(self):
        buffer = (GLubyte * (len(self.format) * self.width * self.height))()

        x = self.x
        y = self.y
        if self.owner:
            x += self.owner.x
            y += self.owner.y

        glReadBuffer(self.gl_buffer)
        glPushClientAttrib(GL_CLIENT_PIXEL_STORE_BIT)
        glPixelStorei(GL_PACK_ALIGNMENT, 1)
        glReadPixels(x, y, self.width, self.height,
                     self.gl_format, GL_UNSIGNED_BYTE, buffer)
        glPopClientAttrib()

        return ImageData(self.width, self.height, self.format, buffer)

    def get_region(self, x, y, width, height):
        if self.owner:
            return self.owner.get_region(x + self.x, y + self.y, width, height)

        region = self.__class__(x + self.x, y + self.y, width, height)
        region.gl_buffer = self.gl_buffer
        region.owner = self
        return region


class ColorBufferImage(BufferImage):
    """A color framebuffer.

    This class is used to wrap both the primary color buffer (i.e., the back
    buffer) or any one of the auxiliary buffers.
    """
    gl_format = GL_RGBA
    format = 'RGBA'

    def get_texture(self, rectangle=False, force_rectangle=False):
        texture = Texture.create(self.width, self.height, GL_RGBA,
                                 rectangle, force_rectangle)
        self.blit_to_texture(texture.target, texture.level,
                             self.anchor_x, self.anchor_y, 0)
        return texture

    def blit_to_texture(self, target, level, x, y, z):
        glReadBuffer(self.gl_buffer)
        glCopyTexSubImage2D(target, level,
                            x - self.anchor_x, y - self.anchor_y,
                            self.x, self.y, self.width, self.height)


class DepthBufferImage(BufferImage):
    """The depth buffer.
    """
    gl_format = GL_DEPTH_COMPONENT
    format = 'L'

    def get_texture(self, rectangle=False, force_rectangle=False):
        assert rectangle == False and force_rectangle == False, \
            'Depth textures cannot be rectangular'
        if not _is_pow2(self.width) or not _is_pow2(self.height):
            raise ImageException(
                'Depth texture requires that buffer dimensions be powers of 2')

        texture = DepthTexture.create_for_size(GL_TEXTURE_2D,
                                               self.width, self.height)
        if self.anchor_x or self.anchor_y:
            texture.anchor_x = self.anchor_x
            texture.anchor_y = self.anchor_y

        glReadBuffer(self.gl_buffer)
        glCopyTexImage2D(texture.target, 0,
                         GL_DEPTH_COMPONENT,
                         self.x, self.y, self.width, self.height,
                         0)
        return texture

    def blit_to_texture(self, target, level, x, y, z):
        glReadBuffer(self.gl_buffer)
        glCopyTexSubImage2D(target, level,
                            x - self.anchor_x, y - self.anchor_y,
                            self.x, self.y, self.width, self.height)


class BufferImageMask(BufferImage):
    """A single bit of the stencil buffer.
    """
    gl_format = GL_STENCIL_INDEX
    format = 'L'

    # TODO mask methods


class ImageGrid(AbstractImage, AbstractImageSequence):
    """An imaginary grid placed over an image allowing easy access to
    regular regions of that image.

    The grid can be accessed either as a complete image, or as a sequence
    of images.  The most useful applications are to access the grid
    as a :py:class:`~pyglet.image.TextureGrid`::

        image_grid = ImageGrid(...)
        texture_grid = image_grid.get_texture_sequence()

    or as a :py:class:`~pyglet.image.Texture3D`::

        image_grid = ImageGrid(...)
        texture_3d = Texture3D.create_for_image_grid(image_grid)

    """
    _items = ()
    _texture_grid = None

    def __init__(self, image, rows, columns,
                 item_width=None, item_height=None,
                 row_padding=0, column_padding=0):
        """Construct a grid for the given image.

        You can specify parameters for the grid, for example setting
        the padding between cells.  Grids are always aligned to the 
        bottom-left corner of the image.

        :Parameters:
            `image` : AbstractImage
                Image over which to construct the grid.
            `rows` : int
                Number of rows in the grid.
            `columns` : int
                Number of columns in the grid.
            `item_width` : int
                Width of each column.  If unspecified, is calculated such
                that the entire image width is used.
            `item_height` : int
                Height of each row.  If unspecified, is calculated such that
                the entire image height is used.
            `row_padding` : int
                Pixels separating adjacent rows.  The padding is only
                inserted between rows, not at the edges of the grid.
            `column_padding` : int
                Pixels separating adjacent columns.  The padding is only 
                inserted between columns, not at the edges of the grid.
        """
        super(ImageGrid, self).__init__(image.width, image.height)

        if item_width is None:
            item_width = \
                (image.width - column_padding * (columns - 1)) // columns
        if item_height is None:
            item_height = \
                (image.height - row_padding * (rows - 1)) // rows
        self.image = image
        self.rows = rows
        self.columns = columns
        self.item_width = item_width
        self.item_height = item_height
        self.row_padding = row_padding
        self.column_padding = column_padding

    def get_texture(self, rectangle=False, force_rectangle=False):
        return self.image.get_texture(rectangle, force_rectangle)

    def get_image_data(self):
        return self.image.get_image_data()

    def get_texture_sequence(self):
        if not self._texture_grid:
            self._texture_grid = TextureGrid(self)
        return self._texture_grid

    def __len__(self):
        return self.rows * self.columns

    def _update_items(self):
        if not self._items:
            self._items = []
            y = 0
            for row in range(self.rows):
                x = 0
                for col in range(self.columns):
                    self._items.append(self.image.get_region(
                        x, y, self.item_width, self.item_height))
                    x += self.item_width + self.column_padding
                y += self.item_height + self.row_padding

    def __getitem__(self, index):
        self._update_items()
        if type(index) is tuple:
            row, column = index
            assert row >= 0 and column >= 0 and row < self.rows and column < self.columns
            return self._items[row * self.columns + column]
        else:
            return self._items[index]

    def __iter__(self):
        self._update_items()
        return iter(self._items)


class TextureGrid(TextureRegion, UniformTextureSequence):
    """A texture containing a regular grid of texture regions.

    To construct, create an :py:class:`~pyglet.image.ImageGrid` first::

        image_grid = ImageGrid(...)
        texture_grid = TextureGrid(image_grid)

    The texture grid can be accessed as a single texture, or as a sequence
    of :py:class:`~pyglet.image.TextureRegion`.  When accessing as a sequence, you can specify
    integer indexes, in which the images are arranged in rows from the
    bottom-left to the top-right::

        # assume the texture_grid is 3x3:
        current_texture = texture_grid[3] # get the middle-left image

    You can also specify tuples in the sequence methods, which are addressed
    as ``row, column``::

        # equivalent to the previous example:
        current_texture = texture_grid[1, 0]

    When using tuples in a slice, the returned sequence is over the
    rectangular region defined by the slice::

        # returns center, center-right, center-top, top-right images in that
        # order:
        images = texture_grid[(1,1):]
        # equivalent to
        images = texture_grid[(1,1):(3,3)]

    """
    items = ()
    rows = 1
    columns = 1
    item_width = 0
    item_height = 0

    def __init__(self, grid):
        image = grid.get_texture()
        if isinstance(image, TextureRegion):
            owner = image.owner
        else:
            owner = image

        super(TextureGrid, self).__init__(
            image.x, image.y, image.z, image.width, image.height, owner)

        items = []
        y = 0
        for row in range(grid.rows):
            x = 0
            for col in range(grid.columns):
                items.append(
                    self.get_region(x, y, grid.item_width, grid.item_height))
                x += grid.item_width + grid.column_padding
            y += grid.item_height + grid.row_padding

        self.items = items
        self.rows = grid.rows
        self.columns = grid.columns
        self.item_width = grid.item_width
        self.item_height = grid.item_height

    def get(self, row, column):
        return self[(row, column)]

    def __getitem__(self, index):
        if type(index) is slice:
            if type(index.start) is not tuple and \
                            type(index.stop) is not tuple:
                return self.items[index]
            else:
                row1 = 0
                col1 = 0
                row2 = self.rows
                col2 = self.columns
                if type(index.start) is tuple:
                    row1, col1 = index.start
                elif type(index.start) is int:
                    row1 = index.start // self.columns
                    col1 = index.start % self.columns
                assert row1 >= 0 and col1 >= 0 and row1 < self.rows and col1 < self.columns

                if type(index.stop) is tuple:
                    row2, col2 = index.stop
                elif type(index.stop) is int:
                    row2 = index.stop // self.columns
                    col2 = index.stop % self.columns
                assert row2 >= 0 and col2 >= 0 and row2 <= self.rows and col2 <= self.columns

                result = []
                i = row1 * self.columns
                for row in range(row1, row2):
                    result += self.items[i + col1:i + col2]
                    i += self.columns
                return result
        else:
            if type(index) is tuple:
                row, column = index
                assert row >= 0 and column >= 0 and row < self.rows and column < self.columns
                return self.items[row * self.columns + column]
            elif type(index) is int:
                return self.items[index]

    def __setitem__(self, index, value):
        if type(index) is slice:
            for region, image in zip(self[index], value):
                if image.width != self.item_width or image.height != self.item_height:
                    raise ImageException('Image has incorrect dimensions')
                image.blit_into(region, image.anchor_x, image.anchor_y, 0)
        else:
            image = value
            if image.width != self.item_width or image.height != self.item_height:
                raise ImageException('Image has incorrect dimensions')
            image.blit_into(self[index], image.anchor_x, image.anchor_y, 0)

    def __len__(self):
        return len(self.items)

    def __iter__(self):
        return iter(self.items)


# --------------------------------------------------------------------------
# Animation stuff here.  Vote on if this should be in pyglet.image.animation
# or just leave it tacked on here.

# TODO: 
#       conversion Animation ->  media.Source
#       move to another module?  
#          pyglet.animation?
#          pyglet.image.animation?

def load_animation(filename, file=None, decoder=None):
    """Load an animation from a file.

    Currently, the only supported format is GIF.

    :Parameters:
        `filename` : str
            Used to guess the animation format, and to load the file if `file`
            is unspecified.
        `file` : file-like object or None
            File object containing the animation stream.
        `decoder` : ImageDecoder or None
            If unspecified, all decoders that are registered for the filename
            extension are tried.  If none succeed, the exception from the
            first decoder is raised.

    :rtype: Animation
    """
    if not file:
        file = open(filename, 'rb')
    if not hasattr(file, 'seek'):
        file = BytesIO(file.read())

    if decoder:
        return decoder.decode(file, filename)
    else:
        first_exception = None
        for decoder in _codecs.get_animation_decoders(filename):
            try:
                image = decoder.decode_animation(file, filename)
                return image
            except _codecs.ImageDecodeException as e:
                first_exception = first_exception or e
                file.seek(0)

        if not first_exception:
            raise _codecs.ImageDecodeException('No image decoders are available')
        raise first_exception


class Animation(object):
    """Sequence of images with timing information.

    If no frames of the animation have a duration of ``None``, the animation
    loops continuously; otherwise the animation stops at the first frame with
    duration of ``None``.

    :Ivariables:
        `frames` : list of `~pyglet.image.AnimationFrame`
            The frames that make up the animation.

    """

    def __init__(self, frames):
        """Create an animation directly from a list of frames.

        :Parameters:
            `frames` : list of `~pyglet.image.AnimationFrame`
                The frames that make up the animation.

        """
        assert len(frames)
        self.frames = frames

    def add_to_texture_bin(self, bin):
        """Add the images of the animation to a :py:class:`~pyglet.image.atlas.TextureBin`.

        The animation frames are modified in-place to refer to the texture bin
        regions.

        :Parameters:
            `bin` : `~pyglet.image.atlas.TextureBin`
                Texture bin to upload animation frames into.

        """
        for frame in self.frames:
            frame.image = bin.add(frame.image)

    def get_transform(self, flip_x=False, flip_y=False, rotate=0):
        """Create a copy of this animation applying a simple transformation.

        The transformation is applied around the image's anchor point of
        each frame.  The texture data is shared between the original animation
        and the transformed animation.

        :Parameters:
            `flip_x` : bool
                If True, the returned animation will be flipped horizontally.
            `flip_y` : bool
                If True, the returned animation will be flipped vertically.
            `rotate` : int
                Degrees of clockwise rotation of the returned animation.  Only
                90-degree increments are supported.

        :rtype: :py:class:`~pyglet.image.Animation`
        """
        frames = [AnimationFrame(frame.image.get_texture().get_transform(flip_x, flip_y, rotate),
                                 frame.duration) for frame in self.frames]
        return Animation(frames)

    def get_duration(self):
        """Get the total duration of the animation in seconds.

        :rtype: float
        """
        return sum([frame.duration for frame in self.frames if frame.duration is not None])

    def get_max_width(self):
        """Get the maximum image frame width.

        This method is useful for determining texture space requirements: due
        to the use of ``anchor_x`` the actual required playback area may be
        larger.

        :rtype: int
        """
        return max([frame.image.width for frame in self.frames])

    def get_max_height(self):
        """Get the maximum image frame height.

        This method is useful for determining texture space requirements: due
        to the use of ``anchor_y`` the actual required playback area may be
        larger.

        :rtype: int
        """
        return max([frame.image.height for frame in self.frames])

    @classmethod
    def from_image_sequence(cls, sequence, period, loop=True):
        """Create an animation from a list of images and a constant framerate.

        :Parameters:
            `sequence` : list of `~pyglet.image.AbstractImage`
                Images that make up the animation, in sequence.
            `period` : float
                Number of seconds to display each image.
            `loop` : bool
                If True, the animation will loop continuously.

        :rtype: :py:class:`~pyglet.image.Animation`
        """
        frames = [AnimationFrame(image, period) for image in sequence]
        if not loop:
            frames[-1].duration = None
        return cls(frames)


class AnimationFrame(object):
    """A single frame of an animation.
    """

    def __init__(self, image, duration):
        """Create an animation frame from an image.

        :Parameters:
            `image` : `~pyglet.image.AbstractImage`
                The image of this frame.
            `duration` : float
                Number of seconds to display the frame, or ``None`` if it is
                the last frame in the animation.

        """
        self.image = image
        self.duration = duration

    def __repr__(self):
        return 'AnimationFrame(%r, %r)' % (self.image, self.duration)


# Initialise default codecs
_codecs.add_default_image_codecs()
