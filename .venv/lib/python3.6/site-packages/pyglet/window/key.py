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

"""Key constants and utilities for pyglet.window.

Usage::

    from pyglet.window import Window
    from pyglet.window import key

    window = Window()

    @window.event
    def on_key_press(symbol, modifiers):
        # Symbolic names:
        if symbol == key.RETURN:

        # Alphabet keys:
        elif symbol == key.Z:

        # Number keys:
        elif symbol == key._1:

        # Number keypad keys:
        elif symbol == key.NUM_1:

        # Modifiers:
        if modifiers & key.MOD_CTRL:

"""
from builtins import str

from pyglet import compat_platform

__docformat__ = 'restructuredtext'
__version__ = '$Id$'


class KeyStateHandler(dict):
    """Simple handler that tracks the state of keys on the keyboard. If a
    key is pressed then this handler holds a True value for it.

    For example::

        >>> win = window.Window
        >>> keyboard = key.KeyStateHandler()
        >>> win.push_handlers(keyboard)

        # Hold down the "up" arrow...

        >>> keyboard[key.UP]
        True
        >>> keyboard[key.DOWN]
        False

    """
    def on_key_press(self, symbol, modifiers):
        self[symbol] = True

    def on_key_release(self, symbol, modifiers):
        self[symbol] = False

    def __getitem__(self, key):
        return self.get(key, False)


def modifiers_string(modifiers):
    """Return a string describing a set of modifiers.

    Example::

        >>> modifiers_string(MOD_SHIFT | MOD_CTRL)
        'MOD_SHIFT|MOD_CTRL'

    :Parameters:
        `modifiers` : int
            Bitwise combination of modifier constants.

    :rtype: str
    """
    mod_names = []
    if modifiers & MOD_SHIFT:
        mod_names.append('MOD_SHIFT')
    if modifiers & MOD_CTRL:
        mod_names.append('MOD_CTRL')
    if modifiers & MOD_ALT:
        mod_names.append('MOD_ALT')
    if modifiers & MOD_CAPSLOCK:
        mod_names.append('MOD_CAPSLOCK')
    if modifiers & MOD_NUMLOCK:
        mod_names.append('MOD_NUMLOCK')
    if modifiers & MOD_SCROLLLOCK:
        mod_names.append('MOD_SCROLLLOCK')
    if modifiers & MOD_COMMAND:
        mod_names.append('MOD_COMMAND')
    if modifiers & MOD_OPTION:
        mod_names.append('MOD_OPTION')
    if modifiers & MOD_FUNCTION:
        mod_names.append('MOD_FUNCTION')
    return '|'.join(mod_names)


def symbol_string(symbol):
    """Return a string describing a key symbol.

    Example::

        >>> symbol_string(BACKSPACE)
        'BACKSPACE'

    :Parameters:
        `symbol` : int
            Symbolic key constant.

    :rtype: str
    """
    if symbol < 1 << 32:
        return _key_names.get(symbol, str(symbol))
    else:
        return 'user_key(%x)' % (symbol >> 32)


def motion_string(motion):
    """Return a string describing a text motion.

    Example::

        >>> motion_string(MOTION_NEXT_WORD)
        'MOTION_NEXT_WORD'

    :Parameters:
        `motion` : int
            Text motion constant.

    :rtype: str
    """
    return _motion_names.get(motion, str(motion))


def user_key(scancode):
    """Return a key symbol for a key not supported by pyglet.

    This can be used to map virtual keys or scancodes from unsupported
    keyboard layouts into a machine-specific symbol.  The symbol will
    be meaningless on any other machine, or under a different keyboard layout.

    Applications should use user-keys only when user explicitly binds them
    (for example, mapping keys to actions in a game options screen).
    """
    assert scancode > 0
    return scancode << 32

# Modifier mask constants
MOD_SHIFT       = 1 << 0
MOD_CTRL        = 1 << 1
MOD_ALT         = 1 << 2
MOD_CAPSLOCK    = 1 << 3
MOD_NUMLOCK     = 1 << 4
MOD_WINDOWS     = 1 << 5
MOD_COMMAND     = 1 << 6
MOD_OPTION      = 1 << 7
MOD_SCROLLLOCK  = 1 << 8
MOD_FUNCTION    = 1 << 9

#: Accelerator modifier.  On Windows and Linux, this is ``MOD_CTRL``, on
#: Mac OS X it's ``MOD_COMMAND``.
MOD_ACCEL = MOD_CTRL
if compat_platform == 'darwin':
    MOD_ACCEL = MOD_COMMAND


# Key symbol constants

# ASCII commands
BACKSPACE     = 0xff08
TAB           = 0xff09
LINEFEED      = 0xff0a
CLEAR         = 0xff0b
RETURN        = 0xff0d
ENTER         = 0xff0d      # synonym
PAUSE         = 0xff13
SCROLLLOCK    = 0xff14
SYSREQ        = 0xff15
ESCAPE        = 0xff1b
SPACE         = 0xff20

# Cursor control and motion
HOME          = 0xff50
LEFT          = 0xff51
UP            = 0xff52
RIGHT         = 0xff53
DOWN          = 0xff54
PAGEUP        = 0xff55
PAGEDOWN      = 0xff56
END           = 0xff57
BEGIN         = 0xff58

# Misc functions
DELETE        = 0xffff
SELECT        = 0xff60
PRINT         = 0xff61
EXECUTE       = 0xff62
INSERT        = 0xff63
UNDO          = 0xff65
REDO          = 0xff66
MENU          = 0xff67
FIND          = 0xff68
CANCEL        = 0xff69
HELP          = 0xff6a
BREAK         = 0xff6b
MODESWITCH    = 0xff7e
SCRIPTSWITCH  = 0xff7e
FUNCTION      = 0xffd2

# Text motion constants: these are allowed to clash with key constants
MOTION_UP                = UP
MOTION_RIGHT             = RIGHT
MOTION_DOWN              = DOWN
MOTION_LEFT              = LEFT
MOTION_NEXT_WORD         = 1
MOTION_PREVIOUS_WORD     = 2
MOTION_BEGINNING_OF_LINE = 3
MOTION_END_OF_LINE       = 4
MOTION_NEXT_PAGE         = PAGEDOWN
MOTION_PREVIOUS_PAGE     = PAGEUP
MOTION_BEGINNING_OF_FILE = 5
MOTION_END_OF_FILE       = 6
MOTION_BACKSPACE         = BACKSPACE
MOTION_DELETE            = DELETE

# Number pad
NUMLOCK       = 0xff7f
NUM_SPACE     = 0xff80
NUM_TAB       = 0xff89
NUM_ENTER     = 0xff8d
NUM_F1        = 0xff91
NUM_F2        = 0xff92
NUM_F3        = 0xff93
NUM_F4        = 0xff94
NUM_HOME      = 0xff95
NUM_LEFT      = 0xff96
NUM_UP        = 0xff97
NUM_RIGHT     = 0xff98
NUM_DOWN      = 0xff99
NUM_PRIOR     = 0xff9a
NUM_PAGE_UP   = 0xff9a
NUM_NEXT      = 0xff9b
NUM_PAGE_DOWN = 0xff9b
NUM_END       = 0xff9c
NUM_BEGIN     = 0xff9d
NUM_INSERT    = 0xff9e
NUM_DELETE    = 0xff9f
NUM_EQUAL     = 0xffbd
NUM_MULTIPLY  = 0xffaa
NUM_ADD       = 0xffab
NUM_SEPARATOR = 0xffac
NUM_SUBTRACT  = 0xffad
NUM_DECIMAL   = 0xffae
NUM_DIVIDE    = 0xffaf

NUM_0         = 0xffb0
NUM_1         = 0xffb1
NUM_2         = 0xffb2
NUM_3         = 0xffb3
NUM_4         = 0xffb4
NUM_5         = 0xffb5
NUM_6         = 0xffb6
NUM_7         = 0xffb7
NUM_8         = 0xffb8
NUM_9         = 0xffb9

# Function keys
F1            = 0xffbe
F2            = 0xffbf
F3            = 0xffc0
F4            = 0xffc1
F5            = 0xffc2
F6            = 0xffc3
F7            = 0xffc4
F8            = 0xffc5
F9            = 0xffc6
F10           = 0xffc7
F11           = 0xffc8
F12           = 0xffc9
F13           = 0xffca
F14           = 0xffcb
F15           = 0xffcc
F16           = 0xffcd
F17           = 0xffce
F18           = 0xffcf
F19           = 0xffd0
F20           = 0xffd1

# Modifiers
LSHIFT        = 0xffe1
RSHIFT        = 0xffe2
LCTRL         = 0xffe3
RCTRL         = 0xffe4
CAPSLOCK      = 0xffe5
LMETA         = 0xffe7
RMETA         = 0xffe8
LALT          = 0xffe9
RALT          = 0xffea
LWINDOWS      = 0xffeb
RWINDOWS      = 0xffec
LCOMMAND      = 0xffed
RCOMMAND      = 0xffee
LOPTION       = 0xffef
ROPTION       = 0xfff0

# Latin-1
SPACE         = 0x020
EXCLAMATION   = 0x021
DOUBLEQUOTE   = 0x022
HASH          = 0x023
POUND         = 0x023  # synonym
DOLLAR        = 0x024
PERCENT       = 0x025
AMPERSAND     = 0x026
APOSTROPHE    = 0x027
PARENLEFT     = 0x028
PARENRIGHT    = 0x029
ASTERISK      = 0x02a
PLUS          = 0x02b
COMMA         = 0x02c
MINUS         = 0x02d
PERIOD        = 0x02e
SLASH         = 0x02f
_0            = 0x030
_1            = 0x031
_2            = 0x032
_3            = 0x033
_4            = 0x034
_5            = 0x035
_6            = 0x036
_7            = 0x037
_8            = 0x038
_9            = 0x039
COLON         = 0x03a
SEMICOLON     = 0x03b
LESS          = 0x03c
EQUAL         = 0x03d
GREATER       = 0x03e
QUESTION      = 0x03f
AT            = 0x040
BRACKETLEFT   = 0x05b
BACKSLASH     = 0x05c
BRACKETRIGHT  = 0x05d
ASCIICIRCUM   = 0x05e
UNDERSCORE    = 0x05f
GRAVE         = 0x060
QUOTELEFT     = 0x060
A             = 0x061
B             = 0x062
C             = 0x063
D             = 0x064
E             = 0x065
F             = 0x066
G             = 0x067
H             = 0x068
I             = 0x069
J             = 0x06a
K             = 0x06b
L             = 0x06c
M             = 0x06d
N             = 0x06e
O             = 0x06f
P             = 0x070
Q             = 0x071
R             = 0x072
S             = 0x073
T             = 0x074
U             = 0x075
V             = 0x076
W             = 0x077
X             = 0x078
Y             = 0x079
Z             = 0x07a
BRACELEFT     = 0x07b
BAR           = 0x07c
BRACERIGHT    = 0x07d
ASCIITILDE    = 0x07e

_key_names = {}
_motion_names = {}
for _name, _value in locals().copy().items():
    if _name[:2] != '__' and _name.upper() == _name and \
       not _name.startswith('MOD_'):
        if _name.startswith('MOTION_'):
            _motion_names[_value] = _name
        else:
            _key_names[_value] = _name

