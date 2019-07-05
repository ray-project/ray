"""
Query system Windows fonts with pure Python.

Public domain work by anatoly techtonik <techtonik@gmail.com>
Use MIT License if public domain doesn't make sense for you.



The task: Get monospace font for an application in the order of
preference.

A problem: Font ID in Windows is its name. Windows doesn't provide
any information about filenames they contained in. From two different
files with the same font name you can get only one.

Windows also doesn't have a clear concept of _generic font family_
familiar from CSS specification. Here is how fontquery maps Windows
LOGFONT properties to generic CSS font families:

  serif      -   (LOGFONT.lfPitchAndFamily >> 4) == FF_ROMAN
  sans-serif -   (LOGFONT.lfPitchAndFamily >> 4) == FF_SWISS
  cursive    -   (LOGFONT.lfPitchAndFamily >> 4) == FF_SCRIPT
  fantasy    -   (LOGFONT.lfPitchAndFamily >> 4) == FF_DECORATIVE
  monospace  -   (lf.lfPitchAndFamily & 0b11) == FIXED_PITCH

NOTE: ATM, May 2015, the Microsoft documentation related to monospace
is misleading due to poor wording:
 - FF_MODERN in the description of LOGFONT structure tells
   "Fonts with constant stroke width (monospace), with or without serifs.
    Monospace fonts are usually modern.
    Pica, Elite, and CourierNew are examples.
   "
   
   Stroke width is the 'pen width', not glyph width. It should read

   "Fonts with constant stroke width, with or without serifs.
    Monospace fonts are usually modern, but not all modern are monospace
   "

PYGLET NOTE:
Examination of all fonts in a windows xp machine shows that all fonts
with

  fontentry.vector and fontentry.family != FF_DONTCARE

are rendered fine.


Use cases:
 [x] get the list of all available system font names
 [ ] get the list of all fonts for generic family
 [ ] get the list of all fonts for specific charset
 [ ] check if specific font is available

Considerations:
 - performance of querying all system fonts is not measured
 - Windows doesn't allow to get filenames of the fonts, so if there
   are two fonts with the same name, one will be missing

MSDN:

    If you request a font named Palatino, but no such font is available
on the system, the font mapper will substitute a font that has similar
attributes but a different name.

   [ ] check if font chosen by the system has required family

    To get the appropriate font, call EnumFontFamiliesEx with the
desired font characteristics in the LOGFONT structure, then retrieve the
appropriate typeface name and create the font using CreateFont or
CreateFontIndirect.

"""
from __future__ import print_function
from builtins import object
DEBUG = False

__all__ = ['have_font', 'font_list']

__version__ = '0.3'
__url__ = 'https://bitbucket.org/techtonik/fontquery'

import sys
PY3K = sys.version_info >= (3, 0)

#-- INTRO: MAINTAIN CACHED FONTS DB --

# [ ] make it Django/NDB style model definition
class FontEntry(object):
  """
  Font classification.
  Level 0:
  - name
  - vector (True if font is vector, False for raster fonts)
  - format: ttf | ...
  """
  def __init__(self, name, vector, format, monospace, family):
    self.name = name
    self.vector = vector
    self.format = format
    self.monospace = monospace
    self.family = family

# List of FontEntry objects
FONTDB = []


#-- CHAPTER 1: GET ALL SYSTEM FONTS USING EnumFontFamiliesEx FROM GDI --

"""
Q: Why GDI? Why not GDI+? 
A: Wikipedia:

    Because of the additional text processing and resolution independence
capabilities in GDI+, text rendering is performed by the CPU [2] and it
is nearly an order of magnitude slower than in hardware accelerated GDI.[3]
Chris Jackson published some tests indicating that a piece of text
rendering code he had written could render 99,000 glyphs per second in GDI,
but the same code using GDI+ rendered 16,600 glyphs per second.
"""

import ctypes
from ctypes import wintypes

user32 = ctypes.windll.user32
gdi32 = ctypes.windll.gdi32

# --- define necessary data structures from wingdi.h

# for calling ANSI functions of Windows API (end with A) TCHAR is
# defined as single char, for Unicode ones (end witn W) it is WCHAR
CHAR = ctypes.c_char    # Python 2.7 compatibility
TCHAR = CHAR
BYTE = ctypes.c_ubyte   # http://bugs.python.org/issue16376

# charset codes for LOGFONT structure
ANSI_CHARSET    = 0
ARABIC_CHARSET  = 178
BALTIC_CHARSET  = 186
CHINESEBIG5_CHARSET = 136
DEFAULT_CHARSET = 1
  # - charset for current system locale -
  #   means function can be called several times
  #   for the single font (for each charset)
EASTEUROPE_CHARSET = 238
GB2312_CHARSET = 134
GREEK_CHARSET  = 161
HANGUL_CHARSET = 129
HEBREW_CHARSET = 177
JOHAB_CHARSET  = 130
MAC_CHARSET = 77
OEM_CHARSET = 255  # OS dependent system charset
RUSSIAN_CHARSET  = 204
SHIFTJIS_CHARSET = 128
SYMBOL_CHARSET   = 2
THAI_CHARSET     = 222
TURKISH_CHARSET  = 162
VIETNAMESE_CHARSET = 163

# build lookup dictionary to get charset name from its code
CHARSET_NAMES = {}
for (name, value) in locals().copy().items():
  if name.endswith('_CHARSET'):
    CHARSET_NAMES[value] = name

# font pitch constants ('fixed pitch' means 'monospace')
DEFAULT_PITCH = 0
FIXED_PITCH = 1
VARIABLE_PITCH = 2

# Windows font family constants
FF_DONTCARE = 0   # Don't care or don't know
FF_ROMAN = 1      # with serifs, proportional
FF_SWISS = 2      # w/out serifs, proportional
FF_MODERN = 3     # constant stroke width
FF_SCRIPT = 4     # handwritten
FF_DECORATIVE = 5 # novelty


class LOGFONT(ctypes.Structure):
  # EnumFontFamiliesEx examines only 3 fields:
  #  - lfCharSet
  #  - lfFaceName  - empty string enumerates one font in each available
  #                  typeface name, valid typeface name gets all fonts
  #                  with that name
  #  - lfPitchAndFamily - must be set to 0 [ ]
  _fields_ = [
    ('lfHeight', wintypes.LONG),
      # value > 0  specifies the largest size of *char cell* to match
      #            char cell = char height + internal leading
      # value = 0  makes matched use default height for search
      # value < 0  specifies the largest size of *char height* to match
    ('lfWidth',  wintypes.LONG),
      # average width also in *logical units*, which are pixels in
      # default _mapping mode_ (MM_TEXT) for device
    ('lfEscapement',  wintypes.LONG),
      # string baseline rotation in tenths of degrees
    ('lfOrientation', wintypes.LONG),
      # character rotation in tenths of degrees
    ('lfWeight', wintypes.LONG),
      # 0 through 1000  400 is normal, 700 is bold, 0 is default
    ('lfItalic', BYTE),
    ('lfUnderline', BYTE),
    ('lfStrikeOut', BYTE),
    ('lfCharSet',   BYTE),
      # ANSI_CHARSET, BALTIC_CHARSET, ... - see *_CHARSET constants above
    ('lfOutPrecision', BYTE),
      # many constants how the output must match height, width, pitch etc.
      # OUT_DEFAULT_PRECIS
      # [ ] TODO
    ('lfClipPrecision', BYTE),
      # how to clip characters, no useful properties, leave default value
      # CLIP_DEFAULT_PRECIS
    ('lfQuality', BYTE),
      # ANTIALIASED_QUALITY
      # CLEARTYPE_QUALITY
      # DEFAULT_QUALITY 
      # DRAFT_QUALITY
      # NONANTIALIASED_QUALITY
      # PROOF_QUALITY
    ('lfPitchAndFamily', BYTE),
      # DEFAULT_PITCH
      # FIXED_PITCH      - authoritative for monospace
      # VARIABLE_PITCH
      #    stacked with any of 
      # FF_DECORATIVE   - novelty
      # FF_DONTCARE     - default font
      # FF_MODERN       - stroke width ('pen width') near constant
      # FF_ROMAN        - proportional (variable char width) with serifs
      # FF_SCRIPT       - handwritten
      # FF_SWISS        - proportional without serifs
    ('lfFaceName', TCHAR*32)]
      # typeface name of the font - null-terminated string

class FONTSIGNATURE(ctypes.Structure):
  # supported code pages and Unicode subranges for the font
  # needed for NEWTEXTMETRICEX structure
  _fields_ = [
    ('sUsb', wintypes.DWORD*4),  # 128-bit Unicode subset bitfield (USB)
    ('sCsb', wintypes.DWORD*2)]  # 64-bit, code-page bitfield (CPB)

class NEWTEXTMETRIC(ctypes.Structure):
  # physical font attributes for True Type fonts
  # needed for NEWTEXTMETRICEX structure
  _fields_ = [
    ('tmHeight', wintypes.LONG),
    ('tmAscent', wintypes.LONG),
    ('tmDescent', wintypes.LONG),
    ('tmInternalLeading', wintypes.LONG),
    ('tmExternalLeading', wintypes.LONG),
    ('tmAveCharWidth', wintypes.LONG),
    ('tmMaxCharWidth', wintypes.LONG),
    ('tmWeight', wintypes.LONG),
    ('tmOverhang', wintypes.LONG),
    ('tmDigitizedAspectX', wintypes.LONG),
    ('tmDigitizedAspectY', wintypes.LONG),
    ('mFirstChar', TCHAR),
    ('mLastChar', TCHAR),
    ('mDefaultChar', TCHAR),
    ('mBreakChar', TCHAR),
    ('tmItalic', BYTE),
    ('tmUnderlined', BYTE),
    ('tmStruckOut', BYTE),
    ('tmPitchAndFamily', BYTE),
    ('tmCharSet', BYTE),
    ('tmFlags', wintypes.DWORD),
    ('ntmSizeEM', wintypes.UINT),
    ('ntmCellHeight', wintypes.UINT),
    ('ntmAvgWidth', wintypes.UINT)]

class NEWTEXTMETRICEX(ctypes.Structure):
  # physical font attributes for True Type fonts
  # needed for FONTENUMPROC callback function
  _fields_ = [
    ('ntmTm', NEWTEXTMETRIC),
    ('ntmFontSig', FONTSIGNATURE)]


# type for a function that is called by the system for
# each font during execution of EnumFontFamiliesEx
FONTENUMPROC = ctypes.WINFUNCTYPE(
  ctypes.c_int,  # return non-0 to continue enumeration, 0 to stop
  ctypes.POINTER(LOGFONT), 
  ctypes.POINTER(NEWTEXTMETRICEX),
  wintypes.DWORD,  # font type, a combination of
                   #   DEVICE_FONTTYPE
                   #   RASTER_FONTTYPE 
                   #   TRUETYPE_FONTTYPE 
  wintypes.LPARAM
)

# When running 64 bit windows, some types are not 32 bit, so Python/ctypes guesses wrong
gdi32.EnumFontFamiliesExA.argtypes = [
        wintypes.HDC,
        ctypes.POINTER(LOGFONT),
        FONTENUMPROC,
        wintypes.LPARAM,
        wintypes.DWORD]


def _enum_font_names(logfont, textmetricex, fonttype, param):
  """callback function to be executed during EnumFontFamiliesEx
     call for each font name. it stores names in global variable
  """
  global FONTDB

  lf = logfont.contents
  name = lf.lfFaceName
  if PY3K:
    # [ ] check this works
    name = name.decode('utf-8')

  # detect font type (vector|raster) and format (ttf)
  # [ ] use Windows constant TRUETYPE_FONTTYPE
  if fonttype & 4:
    vector = True
    format = 'ttf'
  else:
    vector = False
    # [ ] research Windows raster format structure
    format = 'unknown'

  pitch = lf.lfPitchAndFamily & 0b11
  family = lf.lfPitchAndFamily >> 4

  # [ ] check FIXED_PITCH, VARIABLE_PITCH and FF_MODERN
  #     combination
  #
  # FP T NM     400 CHARSET:   0  DFKai-SB
  # FP T NM     400 CHARSET: 136  DFKai-SB
  # FP T NM     400 CHARSET:   0  @DFKai-SB
  # FP T NM     400 CHARSET: 136  @DFKai-SB
  # VP T M      400 CHARSET:   0  OCR A Extended

  monospace = (pitch == FIXED_PITCH)

  charset = lf.lfCharSet

  FONTDB.append(FontEntry(name, vector, format, monospace, family))

  if DEBUG:
    info = ''
 
    if pitch == FIXED_PITCH:
      info += 'FP '
    elif pitch == VARIABLE_PITCH: 
      info += 'VP '
    else:
      info += '   '

    # [ ] check exact fonttype values meaning
    info += '%s ' % {0:'U', 1:'R', 4:'T'}[fonttype]

    if monospace:
      info += 'M  '
    else:
      info += 'NM '

    style = [' ']*3
    if lf.lfItalic:
      style[0] = 'I'
    if lf.lfUnderline:
      style[1] = 'U'
    if lf.lfStrikeOut:
      style[2] = 'S'
    info += ''.join(style)

    info += ' %s' % lf.lfWeight

    #if pitch == FIXED_PITCH:
    if 1:
      print('%s CHARSET: %3s  %s' % (info, lf.lfCharSet, lf.lfFaceName))

  return 1   # non-0 to continue enumeration
enum_font_names = FONTENUMPROC(_enum_font_names)

# --- /define


# --- prepare and call EnumFontFamiliesEx

def query(charset=DEFAULT_CHARSET):
  """
  Prepare and call EnumFontFamiliesEx.

  query()
    - return tuple with sorted list of all available system fonts
  query(charset=ANSI_CHARSET)
    - return tuple sorted list of system fonts supporting ANSI charset

  """
  global FONTDB

  # 1. Get device context of the entire screen
  hdc = user32.GetDC(None)

  # 2. Call EnumFontFamiliesExA (ANSI version)

  # 2a. Call with empty font name to query all available fonts
  #     (or fonts for the specified charset)
  #
  # NOTES:
  #
  #  * there are fonts that don't support ANSI charset
  #  * for DEFAULT_CHARSET font is passed to callback function as
  #    many times as charsets it supports

  # [ ] font name should be less than 32 symbols with terminating \0
  # [ ] check double purpose - enumerate all available font names
  #      - enumerate all available charsets for a single font
  #      - other params?

  logfont = LOGFONT(0, 0, 0, 0, 0, 0, 0, 0, charset,
                    0, 0, 0, 0, b'\0')
  FONTDB = []  # clear cached FONTDB for enum_font_names callback
  res = gdi32.EnumFontFamiliesExA(
          hdc,   # handle to device context
          ctypes.byref(logfont),
          enum_font_names, # pointer to callback function
          0,  # lParam  - application-supplied data
          0)  # dwFlags - reserved = 0
  # res here is the last value returned by callback function

  # 3. Release DC
  user32.ReleaseDC(None, hdc)

  return FONTDB


# --- Public API ---

def have_font(name, refresh=False):
  """
  Return True if font with specified `name` is present. The result
  of querying system font names is cached. Set `refresh` parameter
  to True to purge cache and reload font information.
  """
  if not FONTDB or refresh:
    query()
  if any(f.name == name for f in FONTDB):
    return True
  else:
    return False

def font_list(vector_only=False, monospace_only=False):
  """Return list of system installed font names."""

  if not FONTDB:
    query()

  fonts = FONTDB
  if vector_only:
    fonts = [f for f in fonts if f.vector]
  if monospace_only:
    fonts = [f for f in fonts if f.monospace]

  return sorted([f.name for f in fonts])

if __name__ == '__main__':
  import sys
  if sys.argv[1:] == ['debug']:
    DEBUG = True

  if sys.argv[1:] == ['test'] or DEBUG:
    print('Running tests..')
    # test have_font (Windows)
    test_arial = have_font('Arial')
    print('Have font "Arial"? %s' % test_arial)
    print('Have font "missing-one"? %s' % have_font('missing-one'))
    # test cache is not rebuilt
    FONTDB = [FontEntry('stub', False, '', False, FF_MODERN)]
    assert(have_font('Arial') != test_arial)
    # test cache is rebiult
    assert(have_font('Arial', refresh=True) == test_arial)
    if not DEBUG:
      sys.exit()

  if sys.argv[1:] == ['vector']:
    fonts = font_list(vector_only=True)
  elif sys.argv[1:] == ['mono']:
    fonts = font_list(monospace_only=True)
  elif sys.argv[1:] == ['vector','mono']:
    fonts = font_list(vector_only=True, monospace_only=True)
  else:
    fonts = font_list()
  print('\n'.join(fonts))

  if DEBUG:
    print("Total: %s" % len(font_list()))




#-- CHAPTER 2: WORK WITH FONT DIMENSIONS --
#
# Essential info about font metrics http://support.microsoft.com/kb/32667
# And about logical units at http://www.winprog.org/tutorial/fonts.html

# x. Convert desired font size from points into logical units (pixels)

# By default logical for the screen units are pixels. This is defined
# by default MM_TEXT mapping mode.

# Point is ancient unit of measurement for physical size of a font.
# 10pt is equal to 3.527mm. To make sure a char on screen has physical
# size equal to 3.527mm, we need to know display size to calculate how
# many pixels are in 3.527mm, and then fetch font that best matches
# this size.

# Essential info about conversion http://support.microsoft.com/kb/74299

# x.1 Get pixels per inch using GetDeviceCaps() or ...


#-- CHAPTER 3: LAYERED FONT API --
#
# y. Font object with several layers of info

# Font object should contains normalized font information. This
# information is split according to usage. For example, level 0 property
# is font id - its name. Level 1 can be information about loaded font
# characters - in pyglet it could be cached/used glyphs and video memory
# taken by those glyphs.

# [ ] (pyglet) investigate if it is possible to get video memory size
#              occupied by the font glyphs

# [ ] (pyglet) investigate if it is possible to unload font from video
#              memory if its unused
