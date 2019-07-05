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
from builtins import map
from builtins import str

__docformat__ = 'restructuredtext'
__version__ = '$Id: $'

# TODO Tiger and later: need to set kWindowApplicationScaledAttribute for DPI
# independence?

import math

from pyglet.font import base
import pyglet.image

from pyglet.libs.darwin.cocoapy import *


class QuartzGlyphRenderer(base.GlyphRenderer):
    def __init__(self, font):
        super(QuartzGlyphRenderer, self).__init__(font)
        self.font = font

    def render(self, text):
        # Using CTLineDraw seems to be the only way to make sure that the text
        # is drawn with the specified font when that font is a graphics font loaded from
        # memory.  For whatever reason, [NSAttributedString drawAtPoint:] ignores
        # the graphics font if it not registered with the font manager.
        # So we just use CTLineDraw for both graphics fonts and installed fonts.

        ctFont = self.font.ctFont

        # Create an attributed string using text and font.
        attributes = c_void_p(cf.CFDictionaryCreateMutable(None, 1, cf.kCFTypeDictionaryKeyCallBacks, cf.kCFTypeDictionaryValueCallBacks))
        cf.CFDictionaryAddValue(attributes, kCTFontAttributeName, ctFont)
        string = c_void_p(cf.CFAttributedStringCreate(None, CFSTR(text), attributes))

        # Create a CTLine object to render the string.
        line = c_void_p(ct.CTLineCreateWithAttributedString(string))
        cf.CFRelease(string)
        cf.CFRelease(attributes)
        
        # Get a bounding rectangle for glyphs in string.
        count = len(text)
        chars = (UniChar * count)(*list(map(ord,str(text))))
        glyphs = (CGGlyph * count)()
        ct.CTFontGetGlyphsForCharacters(ctFont, chars, glyphs, count)
        rect = ct.CTFontGetBoundingRectsForGlyphs(ctFont, 0, glyphs, None, count)

        # Get advance for all glyphs in string.
        advance = ct.CTFontGetAdvancesForGlyphs(ctFont, 0, glyphs, None, count)

        # Set image parameters:
        # We add 2 pixels to the bitmap width and height so that there will be a 1-pixel border
        # around the glyph image when it is placed in the texture atlas.  This prevents
        # weird artifacts from showing up around the edges of the rendered glyph textures.
        # We adjust the baseline and lsb of the glyph by 1 pixel accordingly.
        width = max(int(math.ceil(rect.size.width) + 2), 1)
        height = max(int(math.ceil(rect.size.height) + 2), 1)
        baseline = -int(math.floor(rect.origin.y)) + 1
        lsb = int(math.floor(rect.origin.x)) - 1
        advance = int(round(advance))

        # Create bitmap context.
        bitsPerComponent = 8
        bytesPerRow = 4*width
        colorSpace = c_void_p(quartz.CGColorSpaceCreateDeviceRGB())
        bitmap = c_void_p(quartz.CGBitmapContextCreate(
                None, 
                width, 
                height, 
                bitsPerComponent, 
                bytesPerRow, 
                colorSpace, 
                kCGImageAlphaPremultipliedLast))

        # Draw text to bitmap context.
        quartz.CGContextSetShouldAntialias(bitmap, True)
        quartz.CGContextSetTextPosition(bitmap, -lsb, baseline)
        ct.CTLineDraw(line, bitmap)
        cf.CFRelease(line)

        # Create an image to get the data out.
        imageRef = c_void_p(quartz.CGBitmapContextCreateImage(bitmap))

        bytesPerRow = quartz.CGImageGetBytesPerRow(imageRef)
        dataProvider = c_void_p(quartz.CGImageGetDataProvider(imageRef))
        imageData = c_void_p(quartz.CGDataProviderCopyData(dataProvider))
        buffersize = cf.CFDataGetLength(imageData)
        buffer = (c_byte * buffersize)()
        byteRange = CFRange(0, buffersize)
        cf.CFDataGetBytes(imageData, byteRange, buffer)

        quartz.CGImageRelease(imageRef)
        quartz.CGDataProviderRelease(imageData)
        cf.CFRelease(bitmap)
        cf.CFRelease(colorSpace)

        glyph_image = pyglet.image.ImageData(width, height, 'RGBA', buffer, bytesPerRow)

        glyph = self.font.create_glyph(glyph_image)
        glyph.set_bearings(baseline, lsb, advance)
        t = list(glyph.tex_coords)
        glyph.tex_coords = t[9:12] + t[6:9] + t[3:6] + t[:3]
        
        return glyph


class QuartzFont(base.Font):
    glyph_renderer_class = QuartzGlyphRenderer
    _loaded_CGFont_table = {}

    def _lookup_font_with_family_and_traits(self, family, traits):
        # This method searches the _loaded_CGFont_table to find a loaded
        # font of the given family with the desired traits.  If it can't find
        # anything with the exact traits, it tries to fall back to whatever
        # we have loaded that's close.  If it can't find anything in the
        # given family at all, it returns None.

        # Check if we've loaded the font with the specified family.
        if family not in self._loaded_CGFont_table:
            return None
        # Grab a dictionary of all fonts in the family, keyed by traits.
        fonts = self._loaded_CGFont_table[family]
        if not fonts:
            return None
        # Return font with desired traits if it is available. 
        if traits in fonts:
            return fonts[traits]
        # Otherwise try to find a font with some of the traits.
        for (t, f) in fonts.items():
            if traits & t:
                return f
        # Otherwise try to return a regular font.
        if 0 in fonts:
            return fonts[0]
        # Otherwise return whatever we have.
        return list(fonts.values())[0]


    def _create_font_descriptor(self, family_name, traits):
        # Create an attribute dictionary.
        attributes = c_void_p(cf.CFDictionaryCreateMutable(None, 0, cf.kCFTypeDictionaryKeyCallBacks, cf.kCFTypeDictionaryValueCallBacks))
        # Add family name to attributes.
        cfname = CFSTR(family_name)
        cf.CFDictionaryAddValue(attributes, kCTFontFamilyNameAttribute, cfname)
        cf.CFRelease(cfname)
        # Construct a CFNumber to represent the traits.
        itraits = c_int32(traits)
        symTraits = c_void_p(cf.CFNumberCreate(None, kCFNumberSInt32Type, byref(itraits)))
        if symTraits:
            # Construct a dictionary to hold the traits values.
            traitsDict = c_void_p(cf.CFDictionaryCreateMutable(None, 0, cf.kCFTypeDictionaryKeyCallBacks, cf.kCFTypeDictionaryValueCallBacks))
            if traitsDict:
                # Add CFNumber traits to traits dictionary.
                cf.CFDictionaryAddValue(traitsDict, kCTFontSymbolicTrait, symTraits)
                # Add traits dictionary to attributes.
                cf.CFDictionaryAddValue(attributes, kCTFontTraitsAttribute, traitsDict)
                cf.CFRelease(traitsDict)
            cf.CFRelease(symTraits)
        # Create font descriptor with attributes.
        descriptor = c_void_p(ct.CTFontDescriptorCreateWithAttributes(attributes))
        cf.CFRelease(attributes)
        return descriptor

    def __init__(self, name, size, bold=False, italic=False, dpi=None):
        super(QuartzFont, self).__init__()

        if not name: name = 'Helvetica'

        # I don't know what is the right thing to do here.
        if dpi is None: dpi = 96
        size = size * dpi / 72.0
        
        # Construct traits value.
        traits = 0
        if bold: traits |= kCTFontBoldTrait
        if italic: traits |= kCTFontItalicTrait

        name = str(name)
        # First see if we can find an appropriate font from our table of loaded fonts.
        cgFont = self._lookup_font_with_family_and_traits(name, traits)
        if cgFont:
            # Use cgFont from table to create a CTFont object with the specified size.
            self.ctFont = c_void_p(ct.CTFontCreateWithGraphicsFont(cgFont, size, None, None))
        else:
            # Create a font descriptor for given name and traits and use it to create font.
            descriptor = self._create_font_descriptor(name, traits)
            self.ctFont = c_void_p(ct.CTFontCreateWithFontDescriptor(descriptor, size, None))

            cf.CFRelease(descriptor)
            assert self.ctFont, "Couldn't load font: " + name

        self.ascent = int(math.ceil(ct.CTFontGetAscent(self.ctFont)))
        self.descent = -int(math.ceil(ct.CTFontGetDescent(self.ctFont)))

    def __del__(self):
        cf.CFRelease(self.ctFont)

    @classmethod
    def have_font(cls, name):
        name = str(name)
        if name in cls._loaded_CGFont_table: return True
        # Try to create the font to see if it exists.
        # TODO: Find a better way to check.
        cfstring = CFSTR(name)
        cgfont = c_void_p(quartz.CGFontCreateWithFontName(cfstring))
        cf.CFRelease(cfstring)
        if cgfont:
            cf.CFRelease(cgfont)
            return True
        return False

    @classmethod
    def add_font_data(cls, data):
        # Create a cgFont with the data.  There doesn't seem to be a way to 
        # register a font loaded from memory such that the operating system will 
        # find it later.  So instead we just store the cgFont in a table where
        # it can be found by our __init__ method.
        # Note that the iOS CTFontManager *is* able to register graphics fonts,
        # however this method is missing from CTFontManager on MacOS 10.6
        dataRef = c_void_p(cf.CFDataCreate(None, data, len(data)))
        provider = c_void_p(quartz.CGDataProviderCreateWithCFData(dataRef))
        cgFont = c_void_p(quartz.CGFontCreateWithDataProvider(provider))

        cf.CFRelease(dataRef)
        quartz.CGDataProviderRelease(provider)

        # Create a template CTFont from the graphics font so that we can get font info.
        ctFont = c_void_p(ct.CTFontCreateWithGraphicsFont(cgFont, 1, None, None))

        # Get info about the font to use as key in our font table.
        string = c_void_p(ct.CTFontCopyFamilyName(ctFont))
        familyName = str(cfstring_to_string(string))
        cf.CFRelease(string)

        string = c_void_p(ct.CTFontCopyFullName(ctFont))
        fullName = str(cfstring_to_string(string))
        cf.CFRelease(string)

        traits = ct.CTFontGetSymbolicTraits(ctFont)
        cf.CFRelease(ctFont)

        # Store font in table. We store it under both its family name and its
        # full name, since its not always clear which one will be looked up.
        if familyName not in cls._loaded_CGFont_table:
            cls._loaded_CGFont_table[familyName] = {}
        cls._loaded_CGFont_table[familyName][traits] = cgFont

        if fullName not in cls._loaded_CGFont_table:
            cls._loaded_CGFont_table[fullName] = {}
        cls._loaded_CGFont_table[fullName][traits] = cgFont

        
