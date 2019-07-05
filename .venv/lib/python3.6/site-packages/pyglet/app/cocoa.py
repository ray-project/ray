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
__docformat__ = 'restructuredtext'
__version__ = '$Id: $'

from pyglet.app.base import PlatformEventLoop
from pyglet.libs.darwin.cocoapy import *

NSApplication = ObjCClass('NSApplication')
NSMenu = ObjCClass('NSMenu')
NSMenuItem = ObjCClass('NSMenuItem')
NSAutoreleasePool = ObjCClass('NSAutoreleasePool')
NSDate = ObjCClass('NSDate')
NSEvent = ObjCClass('NSEvent')
NSUserDefaults = ObjCClass('NSUserDefaults')


class AutoReleasePool(object):
    def __enter__(self):
        self.pool = NSAutoreleasePool.alloc().init()
        return self.pool

    def __exit__(self, exc_type, exc_value, traceback):
        self.pool.drain()
        del self.pool


def add_menu_item(menu, title, action, key):
    with AutoReleasePool():
        title = CFSTR(title)
        action = get_selector(action)
        key = CFSTR(key)
        menuItem = NSMenuItem.alloc().initWithTitle_action_keyEquivalent_(
            title, action, key)
        menu.addItem_(menuItem)

        # cleanup
        title.release()
        key.release()
        menuItem.release()

def create_menu():
    with AutoReleasePool():
        appMenu = NSMenu.alloc().init()

        # Hide still doesn't work!?
        add_menu_item(appMenu, 'Hide!', 'hide:', 'h')
        appMenu.addItem_(NSMenuItem.separatorItem())
        add_menu_item(appMenu, 'Quit!', 'terminate:', 'q')

        menubar = NSMenu.alloc().init()
        appMenuItem = NSMenuItem.alloc().init()
        appMenuItem.setSubmenu_(appMenu)
        menubar.addItem_(appMenuItem)
        NSApp = NSApplication.sharedApplication()
        NSApp.setMainMenu_(menubar)

        # cleanup
        appMenu.release()
        menubar.release()
        appMenuItem.release()


class CocoaEventLoop(PlatformEventLoop):

    def __init__(self):
        super(CocoaEventLoop, self).__init__()
        with AutoReleasePool():
            # Prepare the default application.
            self.NSApp = NSApplication.sharedApplication()
            if self.NSApp.isRunning():
                # Application was already started by GUI library (e.g. wxPython).
                return
            if not self.NSApp.mainMenu():
                create_menu()
            self.NSApp.setActivationPolicy_(NSApplicationActivationPolicyRegular)
            # Prevent Lion / Mountain Lion from automatically saving application state.
            # If we don't do this, new windows will not display on 10.8 after finishLaunching
            # has been called.  
            defaults = NSUserDefaults.standardUserDefaults()
            ignoreState = CFSTR("ApplePersistenceIgnoreState")
            if not defaults.objectForKey_(ignoreState):
                defaults.setBool_forKey_(True, ignoreState)
            self._finished_launching = False

    def start(self):
        with AutoReleasePool():
            if not self.NSApp.isRunning() and not self._finished_launching:
                # finishLaunching should be called only once. However isRunning will not
                # guard this, as we are not using the normal event loop.
                self.NSApp.finishLaunching()
                self.NSApp.activateIgnoringOtherApps_(True)
                self._finished_launching = True

    def step(self, timeout=None):
        with AutoReleasePool():
            self.dispatch_posted_events()

            # Determine the timeout date.
            if timeout is None:
                # Using distantFuture as untilDate means that nextEventMatchingMask
                # will wait until the next event comes along.
                timeout_date = NSDate.distantFuture()
            else:
                timeout_date = NSDate.dateWithTimeIntervalSinceNow_(timeout)

            # Retrieve the next event (if any).  We wait for an event to show up
            # and then process it, or if timeout_date expires we simply return.
            # We only process one event per call of step().
            self._is_running.set()
            event = self.NSApp.nextEventMatchingMask_untilDate_inMode_dequeue_(
                NSAnyEventMask, timeout_date, NSDefaultRunLoopMode, True)

            # Dispatch the event (if any).
            if event is not None:
                event_type = event.type()
                if event_type != NSApplicationDefined:
                    # Send out event as normal.  Responders will still receive
                    # keyUp:, keyDown:, and flagsChanged: events.
                    self.NSApp.sendEvent_(event)

                    # Resend key events as special pyglet-specific messages
                    # which supplant the keyDown:, keyUp:, and flagsChanged: messages
                    # because NSApplication translates multiple key presses into key
                    # equivalents before sending them on, which means that some keyUp:
                    # messages are never sent for individual keys.   Our pyglet-specific
                    # replacements ensure that we see all the raw key presses & releases.
                    # We also filter out key-down repeats since pyglet only sends one
                    # on_key_press event per key press.
                    if event_type == NSKeyDown and not event.isARepeat():
                        self.NSApp.sendAction_to_from_(get_selector("pygletKeyDown:"), None, event)
                    elif event_type == NSKeyUp:
                        self.NSApp.sendAction_to_from_(get_selector("pygletKeyUp:"), None, event)
                    elif event_type == NSFlagsChanged:
                        self.NSApp.sendAction_to_from_(get_selector("pygletFlagsChanged:"), None, event)

                self.NSApp.updateWindows()
                did_time_out = False
            else:
                did_time_out = True

            self._is_running.clear()

            return did_time_out

    def stop(self):
        pass

    def notify(self):
        with AutoReleasePool():
            notifyEvent = NSEvent.otherEventWithType_location_modifierFlags_timestamp_windowNumber_context_subtype_data1_data2_(
                NSApplicationDefined, # type
                NSPoint(0.0, 0.0),    # location
                0,                    # modifierFlags
                0,                    # timestamp
                0,                    # windowNumber
                None,                 # graphicsContext
                0,                    # subtype
                0,                    # data1
                0,                    # data2
                )

            self.NSApp.postEvent_atStart_(notifyEvent, False)

