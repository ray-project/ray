#!/usr/bin/env python

'''
'''
from __future__ import division

__docformat__ = 'restructuredtext'
__version__ = '$Id: $'

from pyglet.input.base import Tablet, TabletCanvas
from pyglet.input.base import TabletCursor, DeviceOpenException
from pyglet.input.x11_xinput import XInputWindowEventDispatcher
from pyglet.input.x11_xinput import get_devices, DeviceResponder


try:
    from pyglet.libs.x11 import xinput as xi
    _have_xinput = True
except:
    _have_xinput = False


class XInputTablet(Tablet):
    name = 'XInput Tablet'

    def __init__(self, cursors):
        self.cursors = cursors

    def open(self, window):
        return XInputTabletCanvas(window, self.cursors)


class XInputTabletCanvas(DeviceResponder, TabletCanvas):
    def __init__(self, window, cursors):
        super(XInputTabletCanvas, self).__init__(window)
        self.cursors = cursors

        dispatcher = XInputWindowEventDispatcher.get_dispatcher(window)

        self.display = window.display
        self._open_devices = []
        self._cursor_map = {}
        for cursor in cursors:
            device = cursor.device
            device_id = device._device_id
            self._cursor_map[device_id] = cursor

            cursor.max_pressure = device.axes[2].max

            if self.display._display != device.display._display:
                raise DeviceOpenException('Window and device displays differ')

            open_device = xi.XOpenDevice(device.display._display, device_id)
            if not open_device:
                # Ignore this cursor; fail if no cursors added
                continue
            self._open_devices.append(open_device)

            dispatcher.open_device(device_id, open_device, self)

    def close(self):
        for device in self._open_devices:
            xi.XCloseDevice(self.display._display, device)

    def _motion(self, e):
        cursor = self._cursor_map.get(e.deviceid)
        x = e.x
        y = self.window.height - e.y
        pressure = e.axis_data[2] / float(cursor.max_pressure)
        self.dispatch_event('on_motion', cursor, x, y, pressure, 0.0, 0.0)

    def _proximity_in(self, e):
        cursor = self._cursor_map.get(e.deviceid)
        self.dispatch_event('on_enter', cursor)

    def _proximity_out(self, e):
        cursor = self._cursor_map.get(e.deviceid)
        self.dispatch_event('on_leave', cursor)


class XInputTabletCursor(TabletCursor):
    def __init__(self, device):
        super(XInputTabletCursor, self).__init__(device.name)
        self.device = device


def get_tablets(display=None):
    # Each cursor appears as a separate xinput device; find devices that look
    # like Wacom tablet cursors and amalgamate them into a single tablet. 
    valid_names = ('stylus', 'cursor', 'eraser', 'wacom', 'pen', 'pad')
    cursors = []
    devices = get_devices(display)
    for device in devices:
        dev_name = device.name.lower().split()
        if any(n in dev_name for n in valid_names) and len(device.axes) >= 3:
            cursors.append(XInputTabletCursor(device))

    if cursors:
        return [XInputTablet(cursors)]
    return []
