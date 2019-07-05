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
from __future__ import absolute_import

__docformat__ = 'restructuredtext'
__version__ = '$Id: $'

from pyglet.window import key
from .constants import *

keymap = {
    ord('A'): key.A,
    ord('B'): key.B,
    ord('C'): key.C,
    ord('D'): key.D,
    ord('E'): key.E,
    ord('F'): key.F,
    ord('G'): key.G,
    ord('H'): key.H,
    ord('I'): key.I,
    ord('J'): key.J,
    ord('K'): key.K,
    ord('L'): key.L,
    ord('M'): key.M,
    ord('N'): key.N,
    ord('O'): key.O,
    ord('P'): key.P,
    ord('Q'): key.Q,
    ord('R'): key.R,
    ord('S'): key.S,
    ord('T'): key.T,
    ord('U'): key.U,
    ord('V'): key.V,
    ord('W'): key.W,
    ord('X'): key.X,
    ord('Y'): key.Y,
    ord('Z'): key.Z,
    ord('0'): key._0,
    ord('1'): key._1,
    ord('2'): key._2,
    ord('3'): key._3,
    ord('4'): key._4,
    ord('5'): key._5,
    ord('6'): key._6,
    ord('7'): key._7,
    ord('8'): key._8,
    ord('9'): key._9,
    ord('\b'): key.BACKSPACE,

    # By experiment:
    0x14: key.CAPSLOCK,
    0x5d: key.MENU,

#    VK_LBUTTON: , 
#    VK_RBUTTON: , 
    VK_CANCEL: key.CANCEL, 
#    VK_MBUTTON: , 
#    VK_BACK: , 
    VK_TAB: key.TAB, 
#    VK_CLEAR: , 
    VK_RETURN: key.RETURN, 
    VK_SHIFT: key.LSHIFT, 
    VK_CONTROL: key.LCTRL, 
    VK_MENU: key.LALT, 
    VK_PAUSE: key.PAUSE, 
#    VK_CAPITAL: , 
#    VK_KANA: , 
#    VK_HANGEUL: , 
#    VK_HANGUL: , 
#    VK_JUNJA: , 
#    VK_FINAL: , 
#    VK_HANJA: , 
#    VK_KANJI: , 
    VK_ESCAPE: key.ESCAPE, 
#    VK_CONVERT: , 
#    VK_NONCONVERT: , 
#    VK_ACCEPT: , 
#    VK_MODECHANGE: , 
    VK_SPACE: key.SPACE, 
    VK_PRIOR: key.PAGEUP, 
    VK_NEXT: key.PAGEDOWN, 
    VK_END: key.END, 
    VK_HOME: key.HOME, 
    VK_LEFT: key.LEFT, 
    VK_UP: key.UP, 
    VK_RIGHT: key.RIGHT, 
    VK_DOWN: key.DOWN, 
#    VK_SELECT: , 
    VK_PRINT: key.PRINT, 
#    VK_EXECUTE: , 
#    VK_SNAPSHOT: , 
    VK_INSERT: key.INSERT, 
    VK_DELETE: key.DELETE, 
    VK_HELP: key.HELP, 
    VK_LWIN: key.LWINDOWS, 
    VK_RWIN: key.RWINDOWS, 
#    VK_APPS: , 
    VK_NUMPAD0: key.NUM_0, 
    VK_NUMPAD1: key.NUM_1, 
    VK_NUMPAD2: key.NUM_2, 
    VK_NUMPAD3: key.NUM_3, 
    VK_NUMPAD4: key.NUM_4, 
    VK_NUMPAD5: key.NUM_5, 
    VK_NUMPAD6: key.NUM_6, 
    VK_NUMPAD7: key.NUM_7, 
    VK_NUMPAD8: key.NUM_8, 
    VK_NUMPAD9: key.NUM_9, 
    VK_MULTIPLY: key.NUM_MULTIPLY, 
    VK_ADD: key.NUM_ADD, 
#    VK_SEPARATOR: , 
    VK_SUBTRACT: key.NUM_SUBTRACT, 
    VK_DECIMAL: key.NUM_DECIMAL, 
    VK_DIVIDE: key.NUM_DIVIDE, 
    VK_F1: key.F1, 
    VK_F2: key.F2, 
    VK_F3: key.F3, 
    VK_F4: key.F4, 
    VK_F5: key.F5, 
    VK_F6: key.F6, 
    VK_F7: key.F7, 
    VK_F8: key.F8, 
    VK_F9: key.F9, 
    VK_F10: key.F10, 
    VK_F11: key.F11, 
    VK_F12: key.F12, 
    VK_F13: key.F13, 
    VK_F14: key.F14, 
    VK_F15: key.F15, 
    VK_F16: key.F16, 
#    VK_F17: , 
#    VK_F18: , 
#    VK_F19: , 
#    VK_F20: , 
#    VK_F21: , 
#    VK_F22: , 
#    VK_F23: , 
#    VK_F24: , 
    VK_NUMLOCK: key.NUMLOCK, 
    VK_SCROLL: key.SCROLLLOCK, 
    VK_LSHIFT: key.LSHIFT, 
    VK_RSHIFT: key.RSHIFT, 
    VK_LCONTROL: key.LCTRL, 
    VK_RCONTROL: key.RCTRL, 
    VK_LMENU: key.LALT, 
    VK_RMENU: key.RALT, 
#    VK_PROCESSKEY: , 
#    VK_ATTN: , 
#    VK_CRSEL: , 
#    VK_EXSEL: , 
#    VK_EREOF: , 
#    VK_PLAY: , 
#    VK_ZOOM: , 
#    VK_NONAME: , 
#    VK_PA1: , 
#    VK_OEM_CLEAR: , 
#    VK_XBUTTON1: , 
#    VK_XBUTTON2: , 
#    VK_VOLUME_MUTE: , 
#    VK_VOLUME_DOWN: , 
#    VK_VOLUME_UP: , 
#    VK_MEDIA_NEXT_TRACK: , 
#    VK_MEDIA_PREV_TRACK: , 
#    VK_MEDIA_PLAY_PAUSE: , 
#    VK_BROWSER_BACK: , 
#    VK_BROWSER_FORWARD: , 
}

# Keys that must be translated via MapVirtualKey, as the virtual key code
# is language and keyboard dependent.
chmap = {
    ord('!'): key.EXCLAMATION,
    ord('"'): key.DOUBLEQUOTE,
    ord('#'): key.HASH,
    ord('$'): key.DOLLAR,
    ord('%'): key.PERCENT,
    ord('&'): key.AMPERSAND,
    ord("'"): key.APOSTROPHE,
    ord('('): key.PARENLEFT,
    ord(')'): key.PARENRIGHT,
    ord('*'): key.ASTERISK,
    ord('+'): key.PLUS,
    ord(','): key.COMMA,
    ord('-'): key.MINUS,
    ord('.'): key.PERIOD,
    ord('/'): key.SLASH,
    ord(':'): key.COLON,
    ord(';'): key.SEMICOLON,
    ord('<'): key.LESS,
    ord('='): key.EQUAL,
    ord('>'): key.GREATER,
    ord('?'): key.QUESTION,
    ord('@'): key.AT,
    ord('['): key.BRACKETLEFT,
    ord('\\'): key.BACKSLASH,
    ord(']'): key.BRACKETRIGHT,
    ord('\x5e'): key.ASCIICIRCUM,
    ord('_'): key.UNDERSCORE,
    ord('\x60'): key.GRAVE,
    ord('`'): key.QUOTELEFT,
    ord('{'): key.BRACELEFT,
    ord('|'): key.BAR,
    ord('}'): key.BRACERIGHT,
    ord('~'): key.ASCIITILDE,
}
