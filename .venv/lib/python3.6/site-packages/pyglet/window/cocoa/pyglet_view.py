from builtins import object
from pyglet.window import key, mouse
from pyglet.libs.darwin.quartzkey import keymap, charmap

from pyglet.libs.darwin.cocoapy import *

NSTrackingArea = ObjCClass('NSTrackingArea')

# Event data helper functions.

def getMouseDelta(nsevent):
    dx = nsevent.deltaX()
    dy = -nsevent.deltaY()
    return int(round(dx)), int(round(dy))

def getMousePosition(self, nsevent):
    in_window = nsevent.locationInWindow()
    in_window = self.convertPoint_fromView_(in_window, None)
    x = int(in_window.x)
    y = int(in_window.y)
    # Must record mouse position for BaseWindow.draw_mouse_cursor to work.
    self._window._mouse_x = x
    self._window._mouse_y = y
    return x, y

def getModifiers(nsevent):
    modifiers = 0
    modifierFlags = nsevent.modifierFlags()
    if modifierFlags & NSAlphaShiftKeyMask:
        modifiers |= key.MOD_CAPSLOCK
    if modifierFlags & NSShiftKeyMask:
        modifiers |= key.MOD_SHIFT
    if modifierFlags & NSControlKeyMask:
        modifiers |= key.MOD_CTRL
    if modifierFlags & NSAlternateKeyMask:
        modifiers |= key.MOD_ALT
        modifiers |= key.MOD_OPTION
    if modifierFlags & NSCommandKeyMask:
        modifiers |= key.MOD_COMMAND
    if modifierFlags & NSFunctionKeyMask:
        modifiers |= key.MOD_FUNCTION
    return modifiers


def getSymbol(nsevent):
    symbol = keymap.get(nsevent.keyCode(), None)
    if symbol is not None:
        return symbol

    chars = cfstring_to_string(nsevent.charactersIgnoringModifiers())
    if chars:
        return charmap[chars[0].upper()]

    return None


class PygletView_Implementation(object):
    PygletView = ObjCSubclass('NSView', 'PygletView')

    @PygletView.method(b'@'+NSRectEncoding+PyObjectEncoding)
    def initWithFrame_cocoaWindow_(self, frame, window):

        # The tracking area is used to get mouseEntered, mouseExited, and cursorUpdate 
        # events so that we can custom set the mouse cursor within the view.
        self._tracking_area = None

        self = ObjCInstance(send_super(self, 'initWithFrame:', frame, argtypes=[NSRect]))

        if not self:
            return None

        # CocoaWindow object.
        self._window = window
        self.updateTrackingAreas()

        # Create an instance of PygletTextView to handle text events. 
        # We must do this because NSOpenGLView doesn't conform to the
        # NSTextInputClient protocol by default, and the insertText: method will
        # not do the right thing with respect to translating key sequences like
        # "Option-e", "e" if the protocol isn't implemented.  So the easiest
        # thing to do is to subclass NSTextView which *does* implement the
        # protocol and let it handle text input.
        PygletTextView = ObjCClass('PygletTextView')
        self._textview = PygletTextView.alloc().initWithCocoaWindow_(window)
        # Add text view to the responder chain.
        self.addSubview_(self._textview)
        return self

    @PygletView.method('v')
    def dealloc(self):
        self._window = None
        #send_message(self.objc_self, 'removeFromSuperviewWithoutNeedingDisplay')
        self._textview.release()
        self._textview = None
        self._tracking_area.release()
        self._tracking_area = None
        send_super(self, 'dealloc')        

    @PygletView.method('v')
    def updateTrackingAreas(self):
        # This method is called automatically whenever the tracking areas need to be
        # recreated, for example when window resizes.
        if self._tracking_area:
            self.removeTrackingArea_(self._tracking_area)
            self._tracking_area.release()
            self._tracking_area = None

        tracking_options = NSTrackingMouseEnteredAndExited | NSTrackingActiveInActiveApp | NSTrackingCursorUpdate
        frame = self.frame()

        self._tracking_area = NSTrackingArea.alloc().initWithRect_options_owner_userInfo_(
            frame,              # rect
            tracking_options,   # options
            self,     # owner
            None)               # userInfo

        self.addTrackingArea_(self._tracking_area)
    
    @PygletView.method('B')
    def canBecomeKeyView(self):
        return True

    @PygletView.method('B')
    def isOpaque(self):
        return True

    ## Event responders.

    # This method is called whenever the view changes size.
    @PygletView.method(b'v'+NSSizeEncoding) 
    def setFrameSize_(self, size):
        send_super(self, 'setFrameSize:', size, argtypes=[NSSize])

        # This method is called when view is first installed as the
        # contentView of window.  Don't do anything on first call.
        # This also helps ensure correct window creation event ordering.
        if not self._window.context.canvas:
            return

        width, height = int(size.width), int(size.height)
        self._window.switch_to()
        self._window.context.update_geometry()
        self._window.dispatch_event("on_resize", width, height)
        self._window.dispatch_event("on_expose")
        # Can't get app.event_loop.enter_blocking() working with Cocoa, because
        # when mouse clicks on the window's resize control, Cocoa enters into a
        # mini-event loop that only responds to mouseDragged and mouseUp events.
        # This means that using NSTimer to call idle() won't work.  Our kludge
        # is to override NSWindow's nextEventMatchingMask_etc method and call
        # idle() from there.
        if self.inLiveResize():
            from pyglet import app
            if app.event_loop is not None:
                app.event_loop.idle()

    @PygletView.method('v@')
    def pygletKeyDown_(self, nsevent):
        symbol = getSymbol(nsevent)
        modifiers = getModifiers(nsevent)
        self._window.dispatch_event('on_key_press', symbol, modifiers)

    @PygletView.method('v@')
    def pygletKeyUp_(self, nsevent):
        symbol = getSymbol(nsevent)
        modifiers = getModifiers(nsevent)
        self._window.dispatch_event('on_key_release', symbol, modifiers)

    @PygletView.method('v@')
    def pygletFlagsChanged_(self, nsevent):
        # Handles on_key_press and on_key_release events for modifier keys.
        # Note that capslock is handled differently than other keys; it acts
        # as a toggle, so on_key_release is only sent when it's turned off.

        # TODO: Move these constants somewhere else.
        # Undocumented left/right modifier masks found by experimentation:
        NSLeftShiftKeyMask      = 1 << 1
        NSRightShiftKeyMask     = 1 << 2
        NSLeftControlKeyMask    = 1 << 0
        NSRightControlKeyMask   = 1 << 13
        NSLeftAlternateKeyMask  = 1 << 5
        NSRightAlternateKeyMask = 1 << 6
        NSLeftCommandKeyMask    = 1 << 3
        NSRightCommandKeyMask   = 1 << 4

        maskForKey = { key.LSHIFT : NSLeftShiftKeyMask,
                       key.RSHIFT : NSRightShiftKeyMask,
                       key.LCTRL : NSLeftControlKeyMask,
                       key.RCTRL : NSRightControlKeyMask,
                       key.LOPTION : NSLeftAlternateKeyMask,
                       key.ROPTION : NSRightAlternateKeyMask,
                       key.LCOMMAND : NSLeftCommandKeyMask,
                       key.RCOMMAND : NSRightCommandKeyMask,
                       key.CAPSLOCK : NSAlphaShiftKeyMask,
                       key.FUNCTION : NSFunctionKeyMask }

        symbol = keymap.get(nsevent.keyCode(), None)

        # Ignore this event if symbol is not a modifier key.  We must check this
        # because e.g., we receive a flagsChanged message when using CMD-tab to
        # switch applications, with symbol == "a" when command key is released.
        if symbol is None or symbol not in maskForKey: 
            return

        modifiers = getModifiers(nsevent)
        modifierFlags = nsevent.modifierFlags()

        if symbol and modifierFlags & maskForKey[symbol]:
            self._window.dispatch_event('on_key_press', symbol, modifiers)
        else:
            self._window.dispatch_event('on_key_release', symbol, modifiers)

    # Overriding this method helps prevent system beeps for unhandled events.
    @PygletView.method('B@')
    def performKeyEquivalent_(self, nsevent):
        # Let arrow keys and certain function keys pass through the responder
        # chain so that the textview can handle on_text_motion events.
        modifierFlags = nsevent.modifierFlags()
        if modifierFlags & NSNumericPadKeyMask:
            return False
        if modifierFlags & NSFunctionKeyMask:
            ch = cfstring_to_string(nsevent.charactersIgnoringModifiers())
            if ch in (NSHomeFunctionKey, NSEndFunctionKey, 
                      NSPageUpFunctionKey, NSPageDownFunctionKey):
                return False
        # Send the key equivalent to the main menu to perform menu items.
        NSApp = ObjCClass('NSApplication').sharedApplication()
        NSApp.mainMenu().performKeyEquivalent_(nsevent)
        # Indicate that we've handled the event so system won't beep.
        return True

    @PygletView.method('v@')
    def mouseMoved_(self, nsevent):
        if self._window._mouse_ignore_motion:
            self._window._mouse_ignore_motion = False
            return
        # Don't send on_mouse_motion events if we're not inside the content rectangle.
        if not self._window._mouse_in_window:
            return
        x, y = getMousePosition(self, nsevent)
        dx, dy = getMouseDelta(nsevent)
        self._window.dispatch_event('on_mouse_motion', x, y, dx, dy)
    
    @PygletView.method('v@')
    def scrollWheel_(self, nsevent):
        x, y = getMousePosition(self, nsevent)
        scroll_x, scroll_y = getMouseDelta(nsevent)
        self._window.dispatch_event('on_mouse_scroll', x, y, scroll_x, scroll_y)
    
    @PygletView.method('v@')
    def mouseDown_(self, nsevent):
        x, y = getMousePosition(self, nsevent)
        buttons = mouse.LEFT
        modifiers = getModifiers(nsevent)
        self._window.dispatch_event('on_mouse_press', x, y, buttons, modifiers)
    
    @PygletView.method('v@')
    def mouseDragged_(self, nsevent):
        x, y = getMousePosition(self, nsevent)
        dx, dy = getMouseDelta(nsevent)
        buttons = mouse.LEFT
        modifiers = getModifiers(nsevent)
        self._window.dispatch_event('on_mouse_drag', x, y, dx, dy, buttons, modifiers)

    @PygletView.method('v@')
    def mouseUp_(self, nsevent):
        x, y = getMousePosition(self, nsevent)
        buttons = mouse.LEFT
        modifiers = getModifiers(nsevent)
        self._window.dispatch_event('on_mouse_release', x, y, buttons, modifiers)
    
    @PygletView.method('v@')
    def rightMouseDown_(self, nsevent):
        x, y = getMousePosition(self, nsevent)
        buttons = mouse.RIGHT
        modifiers = getModifiers(nsevent)
        self._window.dispatch_event('on_mouse_press', x, y, buttons, modifiers)
    
    @PygletView.method('v@')
    def rightMouseDragged_(self, nsevent):
        x, y = getMousePosition(self, nsevent)
        dx, dy = getMouseDelta(nsevent)
        buttons = mouse.RIGHT
        modifiers = getModifiers(nsevent)
        self._window.dispatch_event('on_mouse_drag', x, y, dx, dy, buttons, modifiers)
    
    @PygletView.method('v@')
    def rightMouseUp_(self, nsevent):
        x, y = getMousePosition(self, nsevent)
        buttons = mouse.RIGHT
        modifiers = getModifiers(nsevent)
        self._window.dispatch_event('on_mouse_release', x, y, buttons, modifiers)
    
    @PygletView.method('v@')
    def otherMouseDown_(self, nsevent):
        x, y = getMousePosition(self, nsevent)
        buttons = mouse.MIDDLE
        modifiers = getModifiers(nsevent)
        self._window.dispatch_event('on_mouse_press', x, y, buttons, modifiers)
    
    @PygletView.method('v@')
    def otherMouseDragged_(self, nsevent):
        x, y = getMousePosition(self, nsevent)
        dx, dy = getMouseDelta(nsevent)
        buttons = mouse.MIDDLE
        modifiers = getModifiers(nsevent)
        self._window.dispatch_event('on_mouse_drag', x, y, dx, dy, buttons, modifiers)
    
    @PygletView.method('v@')
    def otherMouseUp_(self, nsevent):
        x, y = getMousePosition(self, nsevent)
        buttons = mouse.MIDDLE
        modifiers = getModifiers(nsevent)
        self._window.dispatch_event('on_mouse_release', x, y, buttons, modifiers)

    @PygletView.method('v@')
    def mouseEntered_(self, nsevent):
        x, y = getMousePosition(self, nsevent)
        self._window._mouse_in_window = True
        # Don't call self._window.set_mouse_platform_visible() from here.
        # Better to do it from cursorUpdate:
        self._window.dispatch_event('on_mouse_enter', x, y)

    @PygletView.method('v@')
    def mouseExited_(self, nsevent):
        x, y = getMousePosition(self, nsevent)
        self._window._mouse_in_window = False
        if not self._window._is_mouse_exclusive:
            self._window.set_mouse_platform_visible()
        self._window.dispatch_event('on_mouse_leave', x, y)

    @PygletView.method('v@')
    def cursorUpdate_(self, nsevent):
        # Called when mouse cursor enters view.  Unlike mouseEntered:,
        # this method will be called if the view appears underneath a
        # motionless mouse cursor, as can happen during window creation,
        # or when switching into fullscreen mode.  
        # BUG: If the mouse enters the window via the resize control at the
        # the bottom right corner, the resize control will set the cursor
        # to the default arrow and screw up our cursor tracking.
        self._window._mouse_in_window = True
        if not self._window._is_mouse_exclusive:
            self._window.set_mouse_platform_visible()


PygletView = ObjCClass('PygletView')
