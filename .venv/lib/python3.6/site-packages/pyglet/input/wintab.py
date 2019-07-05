#!/usr/bin/python
# $Id:$

from __future__ import print_function
from builtins import range
from builtins import object
import ctypes

import pyglet
from pyglet.input.base import DeviceOpenException
from pyglet.input.base import Tablet, TabletCursor, TabletCanvas

from pyglet.libs.win32 import libwintab as wintab
lib = wintab.lib

def wtinfo(category, index, buffer):
    size = lib.WTInfoW(category, index, None)
    assert size <= ctypes.sizeof(buffer)
    lib.WTInfoW(category, index, ctypes.byref(buffer))
    return buffer

def wtinfo_string(category, index):
    size = lib.WTInfoW(category, index, None)
    buffer = ctypes.create_unicode_buffer(size)
    lib.WTInfoW(category, index, buffer)
    return buffer.value

def wtinfo_uint(category, index):
    buffer = wintab.UINT()
    lib.WTInfoW(category, index, ctypes.byref(buffer))
    return buffer.value

def wtinfo_word(category, index):
    buffer = wintab.WORD()
    lib.WTInfoW(category, index, ctypes.byref(buffer))
    return buffer.value 

def wtinfo_dword(category, index):
    buffer = wintab.DWORD()
    lib.WTInfoW(category, index, ctypes.byref(buffer))
    return buffer.value 

def wtinfo_wtpkt(category, index):
    buffer = wintab.WTPKT()
    lib.WTInfoW(category, index, ctypes.byref(buffer))
    return buffer.value 

def wtinfo_bool(category, index):
    buffer = wintab.BOOL()
    lib.WTInfoW(category, index, ctypes.byref(buffer))
    return bool(buffer.value)

class WintabTablet(Tablet):
    def __init__(self, index):
        self._device = wintab.WTI_DEVICES + index
        self.name = wtinfo_string(self._device, wintab.DVC_NAME).strip()
        self.id = wtinfo_string(self._device, wintab.DVC_PNPID)

        hardware = wtinfo_uint(self._device, wintab.DVC_HARDWARE)
        #phys_cursors = hardware & wintab.HWC_PHYSID_CURSORS
        
        n_cursors = wtinfo_uint(self._device, wintab.DVC_NCSRTYPES)
        first_cursor = wtinfo_uint(self._device, wintab.DVC_FIRSTCSR)

        self.pressure_axis = wtinfo(self._device, wintab.DVC_NPRESSURE,
                                    wintab.AXIS())

        self.cursors = []
        self._cursor_map = {}

        for i in range(n_cursors):
            cursor = WintabTabletCursor(self, i + first_cursor)
            if not cursor.bogus:
                self.cursors.append(cursor)
                self._cursor_map[i + first_cursor] = cursor

    def open(self, window):
        return WintabTabletCanvas(self, window)

class WintabTabletCanvas(TabletCanvas):
    def __init__(self, device, window, msg_base=wintab.WT_DEFBASE):
        super(WintabTabletCanvas, self).__init__(window)

        self.device = device
        self.msg_base = msg_base

        # Just use system context, for similarity w/ os x and xinput.
        # WTI_DEFCONTEXT detaches mouse from tablet, which is nice, but not
        # possible on os x afiak.
        self.context_info = context_info = wintab.LOGCONTEXT()
        wtinfo(wintab.WTI_DEFSYSCTX, 0, context_info)
        context_info.lcMsgBase = msg_base
        context_info.lcOptions |= wintab.CXO_MESSAGES

        # If you change this, change definition of PACKET also.
        context_info.lcPktData = (
            wintab.PK_CHANGED | wintab.PK_CURSOR | wintab.PK_BUTTONS | 
            wintab.PK_X | wintab.PK_Y | wintab.PK_Z | 
            wintab.PK_NORMAL_PRESSURE | wintab.PK_TANGENT_PRESSURE | 
            wintab.PK_ORIENTATION)
        context_info.lcPktMode = 0   # All absolute

        self._context = lib.WTOpenW(window._hwnd,
                                    ctypes.byref(context_info), True)
        if not self._context:
            raise DeviceOpenException("Couldn't open tablet context")

        window._event_handlers[msg_base + wintab.WT_PACKET] = \
            self._event_wt_packet
        window._event_handlers[msg_base + wintab.WT_PROXIMITY] = \
            self._event_wt_proximity

        self._current_cursor = None
        self._pressure_scale = device.pressure_axis.get_scale()
        self._pressure_bias = device.pressure_axis.get_bias()

    def close(self):
        lib.WTClose(self._context)
        self._context = None

        del self.window._event_handlers[self.msg_base + wintab.WT_PACKET]
        del self.window._event_handlers[self.msg_base + wintab.WT_PROXIMITY]

    def _set_current_cursor(self, cursor_type):
        if self._current_cursor:
            self.dispatch_event('on_leave', self._current_cursor)

        self._current_cursor = self.device._cursor_map.get(cursor_type, None)

        if self._current_cursor:
            self.dispatch_event('on_enter', self._current_cursor)

    @pyglet.window.win32.Win32EventHandler(0)
    def _event_wt_packet(self, msg, wParam, lParam):
        if lParam != self._context:
            return

        packet = wintab.PACKET()
        if lib.WTPacket(self._context, wParam, ctypes.byref(packet)) == 0:
            return

        if not packet.pkChanged:
            return

        window_x, window_y = self.window.get_location() # TODO cache on window
        window_y = self.window.screen.height - window_y - self.window.height
        x = packet.pkX - window_x
        y = packet.pkY - window_y
        pressure = (packet.pkNormalPressure + self._pressure_bias) * \
                        self._pressure_scale
        
        if self._current_cursor is None:
            self._set_current_cursor(packet.pkCursor)

        self.dispatch_event('on_motion', self._current_cursor,
            x, y, pressure, 0., 0.)

        print(packet.pkButtons)

    @pyglet.window.win32.Win32EventHandler(0)
    def _event_wt_proximity(self, msg, wParam, lParam):
        if wParam != self._context:
            return

        if not lParam & 0xffff0000:
            # Not a hardware proximity event
            return

        if not lParam & 0xffff:
            # Going out
            self.dispatch_event('on_leave', self._current_cursor)

        # If going in, proximity event will be generated by next event, which
        # can actually grab a cursor id.
        self._current_cursor = None
        
class WintabTabletCursor(object):
    def __init__(self, device, index):
        self.device = device
        self._cursor = wintab.WTI_CURSORS + index

        self.name = wtinfo_string(self._cursor, wintab.CSR_NAME).strip()
        self.active = wtinfo_bool(self._cursor, wintab.CSR_ACTIVE)
        pktdata = wtinfo_wtpkt(self._cursor, wintab.CSR_PKTDATA)
        
        # A whole bunch of cursors are reported by the driver, but most of
        # them are hogwash.  Make sure a cursor has at least X and Y data
        # before adding it to the device.
        self.bogus = not (pktdata & wintab.PK_X and pktdata & wintab.PK_Y)
        if self.bogus:
            return

        self.id = (wtinfo_dword(self._cursor, wintab.CSR_TYPE) << 32) | \
                  wtinfo_dword(self._cursor, wintab.CSR_PHYSID)
        
    def __repr__(self):
        return 'WintabCursor(%r)' % self.name

def get_spec_version():
    spec_version = wtinfo_word(wintab.WTI_INTERFACE, wintab.IFC_SPECVERSION)
    return spec_version

def get_interface_name():
    interface_name = wtinfo_string(wintab.WTI_INTERFACE, wintab.IFC_WINTABID)
    return interface_name

def get_implementation_version():
    impl_version = wtinfo_word(wintab.WTI_INTERFACE, wintab.IFC_IMPLVERSION)
    return impl_version

def get_tablets(display=None):
    # Require spec version 1.1 or greater
    if get_spec_version() < 0x101:
        return []

    n_devices = wtinfo_uint(wintab.WTI_INTERFACE, wintab.IFC_NDEVICES)
    devices = [WintabTablet(i) for i in range(n_devices)]
    return devices
