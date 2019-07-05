from builtins import object
from pyglet.libs.darwin.cocoapy import *

# This class is a wrapper around NSCursor which prevents us from
# sending too many hide or unhide messages in a row.  Apparently
# NSCursor treats them like retain/release messages, which can be
# problematic when we are e.g. switching between window & fullscreen.
class SystemCursor(object):
    cursor_is_hidden = False
    @classmethod
    def hide(cls):
        if not cls.cursor_is_hidden:
            send_message('NSCursor', 'hide')
            cls.cursor_is_hidden = True
    @classmethod
    def unhide(cls):
        if cls.cursor_is_hidden:
            send_message('NSCursor', 'unhide')
            cls.cursor_is_hidden = False
