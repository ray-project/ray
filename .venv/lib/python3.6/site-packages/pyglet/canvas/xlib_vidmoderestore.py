#!/usr/bin/python
# $Id: $

'''Fork a child process and inform it of mode changes to each screen.  The
child waits until the parent process dies, and then connects to each X server 
with a mode change and restores the mode.

This emulates the behaviour of Windows and Mac, so that resolution changes
made by an application are not permanent after the program exits, even if
the process is terminated uncleanly.

The child process is communicated to via a pipe, and watches for parent
death with a Linux extension signal handler.
'''
from builtins import object

import ctypes
import os
import signal
import struct
import threading

from pyglet.libs.x11 import xlib
from pyglet.compat import asbytes
try:
    from pyglet.libs.x11 import xf86vmode
except:
    # No xf86vmode... should not be switching modes.
    pass

_restore_mode_child_installed = False
_restorable_screens = set()
_mode_write_pipe = None

# Mode packets tell the child process how to restore a given display and
# screen.  Only one packet should be sent per display/screen (more would
# indicate redundancy or incorrect restoration).  Packet format is:
#   display (max 256 chars), 
#   screen
#   width
#   height
#   rate
class ModePacket(object):
    format = '256siHHI'
    size = struct.calcsize(format)
    def __init__(self, display, screen, width, height, rate):
        self.display = display
        self.screen = screen
        self.width = width
        self.height = height
        self.rate = rate

    def encode(self):
        return struct.pack(self.format, self.display, self.screen,
                           self.width, self.height, self.rate)

    @classmethod
    def decode(cls, data):
        display, screen, width, height, rate = \
            struct.unpack(cls.format, data)
        return cls(display.strip(asbytes('\0')), screen, width, height, rate)

    def __repr__(self):
        return '%s(%r, %r, %r, %r, %r)' % (
            self.__class__.__name__, self.display, self.screen,
            self.width, self.height, self.rate)

    def set(self):
        display = xlib.XOpenDisplay(self.display)
        modes, n_modes = get_modes_array(display, self.screen)
        mode = get_matching_mode(modes, n_modes, 
                                 self.width, self.height, self.rate)
        if mode is not None:
            xf86vmode.XF86VidModeSwitchToMode(display, self.screen, mode)
        free_modes_array(modes, n_modes)
        xlib.XCloseDisplay(display)

def get_modes_array(display, screen):
    count = ctypes.c_int()
    modes = ctypes.POINTER(ctypes.POINTER(xf86vmode.XF86VidModeModeInfo))()
    xf86vmode.XF86VidModeGetAllModeLines(display, screen, count, modes)
    return modes, count.value

def get_matching_mode(modes, n_modes, width, height, rate):
    # Copy modes out of list and free list
    for i in range(n_modes):
        mode = modes.contents[i]
        if (mode.hdisplay == width and 
            mode.vdisplay == height and 
            mode.dotclock == rate):
            return mode
    return None

def free_modes_array(modes, n_modes):
    for i in range(n_modes):
        mode = modes.contents[i]
        if mode.privsize:
            xlib.XFree(mode.private)
    xlib.XFree(modes)

def _install_restore_mode_child():
    global _mode_write_pipe
    global _restore_mode_child_installed

    if _restore_mode_child_installed:
        return

    # Parent communicates to child by sending "mode packets" through a pipe:
    mode_read_pipe, _mode_write_pipe = os.pipe()

    if os.fork() == 0:
        # Child process (watches for parent to die then restores video mode(s).
        os.close(_mode_write_pipe)

        # Set up SIGHUP to be the signal for when the parent dies.
        PR_SET_PDEATHSIG = 1
        libc = ctypes.cdll.LoadLibrary('libc.so.6')
        libc.prctl.argtypes = (ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong,
                               ctypes.c_ulong, ctypes.c_ulong)
        libc.prctl(PR_SET_PDEATHSIG, signal.SIGHUP, 0, 0, 0)

        # SIGHUP indicates the parent has died.  The child lock is unlocked, it
        # stops reading from the mode packet pipe and restores video modes on
        # all displays/screens it knows about.
        def _sighup(signum, frame):
            parent_wait_lock.release();
        parent_wait_lock = threading.Lock();
        parent_wait_lock.acquire()
        signal.signal(signal.SIGHUP, _sighup)

        # Wait for parent to die and read packets from parent pipe
        packets = []
        buffer = asbytes('')
        while parent_wait_lock.locked():
            try:
                data = os.read(mode_read_pipe, ModePacket.size)
                buffer += data
                # Decode packets
                while len(buffer) >= ModePacket.size:
                    packet = ModePacket.decode(buffer[:ModePacket.size])
                    packets.append(packet)
                    buffer = buffer[ModePacket.size:]
            except OSError:
                pass # Interrupted system call

        for packet in packets:
            packet.set()
        os._exit(0)
        
    else:
        # Parent process.  Clean up pipe then continue running program as
        # normal.  Send mode packets through pipe as additional
        # displays/screens are mode switched.
        os.close(mode_read_pipe)
        _restore_mode_child_installed = True

def set_initial_mode(mode):
    _install_restore_mode_child()

    display = xlib.XDisplayString(mode.screen.display._display)
    screen = mode.screen.display.x_screen

    # Only store one mode per screen.
    if (display, screen) in _restorable_screens:
        return

    packet = ModePacket(display, screen, mode.width, mode.height, mode.rate)

    os.write(_mode_write_pipe, packet.encode())
    _restorable_screens.add((display, screen))


