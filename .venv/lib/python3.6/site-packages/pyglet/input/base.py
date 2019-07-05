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

"""Interface classes for `pyglet.input`.

.. versionadded:: 1.2
"""
from __future__ import division
from builtins import object

__docformat__ = 'restructuredtext'
__version__ = '$Id: $'

import sys
from pyglet.event import EventDispatcher

_is_pyglet_docgen = hasattr(sys, 'is_pyglet_docgen') and sys.is_pyglet_docgen


class DeviceException(Exception):
    pass


class DeviceOpenException(DeviceException):
    pass


class DeviceExclusiveException(DeviceException):
    pass


class Device(object):
    """Input device.

    :Ivariables:
        display : `pyglet.canvas.Display`
            Display this device is connected to.
        name : str
            Name of the device, as described by the device firmware.
        manufacturer : str
            Name of the device manufacturer, or ``None`` if the information is
            not available.
    """

    def __init__(self, display, name):
        self.display = display
        self.name = name
        self.manufacturer = None

        # TODO: make private 
        self.is_open = False

    def open(self, window=None, exclusive=False):
        """Open the device to begin receiving input from it.

        :Parameters:
            `window` : Window
                Optional window to associate with the device.  The behaviour
                of this parameter is device and operating system dependant.
                It can usually be omitted for most devices.
            `exclusive` : bool
                If ``True`` the device will be opened exclusively so that no
                other application can use it.  The method will raise
                `DeviceExclusiveException` if the device cannot be opened this
                way (for example, because another application has already
                opened it).
        """

        if self.is_open:
            raise DeviceOpenException('Device is already open.')

        self.is_open = True

    def close(self):
        """Close the device. """
        self.is_open = False

    def get_controls(self):
        """Get a list of controls provided by the device.

        :rtype: list of `Control`
        """
        raise NotImplementedError('abstract')

    def __repr__(self):
        return '%s(name=%s)' % (self.__class__.__name__, self.name)


class Control(EventDispatcher):
    """Single value input provided by a device.

    A control's value can be queried when the device is open.  Event handlers
    can be attached to the control to be called when the value changes.

    The `min` and `max` properties are provided as advertised by the
    device; in some cases the control's value will be outside this range.

    :Ivariables:
        `name` : str
            Name of the control, or ``None`` if unknown
        `raw_name` : str
            Unmodified name of the control, as presented by the operating
            system; or ``None`` if unknown.
        `inverted` : bool
            If ``True``, the value reported is actually inverted from what the
            device reported; usually this is to provide consistency across
            operating systems.
    """

    def __init__(self, name, raw_name=None):
        self.name = name
        self.raw_name = raw_name
        self.inverted = False
        self._value = None

    @property
    def value(self):
        """Current value of the control.

        The range of the value is device-dependent; for absolute controls
        the range is given by ``min`` and ``max`` (however the value may exceed
        this range); for relative controls the range is undefined.

        :type: float
        """
        return self._value

    @value.setter
    def value(self, newvalue):
        if newvalue == self._value:
            return
        self._value = newvalue
        self.dispatch_event('on_change', newvalue)

    def __repr__(self):
        if self.name:
            return '%s(name=%s, raw_name=%s)' % (
                self.__class__.__name__, self.name, self.raw_name)
        else:
            return '%s(raw_name=%s)' % (self.__class__.__name__, self.raw_name)

    if _is_pyglet_docgen:
        def on_change(self, value):
            """The value changed.

            :Parameters:
                `value` : float
                    Current value of the control.

            :event:
            """

Control.register_event_type('on_change')


class RelativeAxis(Control):
    """An axis whose value represents a relative change from the previous
    value.
    """

    #: Name of the horizontal axis control
    X = 'x'
    #: Name of the vertical axis control
    Y = 'y'
    #: Name of the Z axis control.
    Z = 'z'
    #: Name of the rotational-X axis control
    RX = 'rx'
    #: Name of the rotational-Y axis control
    RY = 'ry'
    #: Name of the rotational-Z axis control
    RZ = 'rz'
    #: Name of the scroll wheel control
    WHEEL = 'wheel'

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, newvalue):
        self._value = newvalue
        self.dispatch_event('on_change', newvalue)


class AbsoluteAxis(Control):
    """An axis whose value represents a physical measurement from the device.

    The value is advertised to range over ``min`` and ``max``.

    :Ivariables:
        `min` : float
            Minimum advertised value.
        `max` : float 
            Maximum advertised value.
    """

    #: Name of the horizontal axis control
    X = 'x'
    #: Name of the vertical axis control
    Y = 'y'
    #: Name of the Z axis control.
    Z = 'z'
    #: Name of the rotational-X axis control
    RX = 'rx'
    #: Name of the rotational-Y axis control
    RY = 'ry'
    #: Name of the rotational-Z axis control
    RZ = 'rz'
    #: Name of the hat (POV) control, when a single control enumerates all of
    #: the hat's positions.
    HAT = 'hat'
    #: Name of the hat's (POV's) horizontal control, when the hat position is
    #: described by two orthogonal controls.
    HAT_X = 'hat_x'
    #: Name of the hat's (POV's) vertical control, when the hat position is
    #: described by two orthogonal controls.
    HAT_Y = 'hat_y'

    def __init__(self, name, min, max, raw_name=None):
        super(AbsoluteAxis, self).__init__(name, raw_name)
        
        self.min = min
        self.max = max


class Button(Control):
    """A control whose value is boolean. """

    @property
    def value(self):
        return bool(self._value)

    @value.setter
    def value(self, newvalue):
        if newvalue == self._value:
            return
        self._value = newvalue
        self.dispatch_event('on_change', bool(newvalue))
        if newvalue:
            self.dispatch_event('on_press')
        else:
            self.dispatch_event('on_release')

    if _is_pyglet_docgen:
        def on_press(self):
            """The button was pressed.

            :event:
            """

        def on_release(self):
            """The button was released.

            :event:
            """

Button.register_event_type('on_press')
Button.register_event_type('on_release')


class Joystick(EventDispatcher):
    """High-level interface for joystick-like devices.  This includes analogue
    and digital joysticks, gamepads, game controllers, and possibly even
    steering wheels and other input devices.  There is unfortunately no way to
    distinguish between these different device types.

    To use a joystick, first call `open`, then in your game loop examine
    the values of `x`, `y`, and so on.  These values are normalized to the
    range [-1.0, 1.0]. 

    To receive events when the value of an axis changes, attach an 
    on_joyaxis_motion event handler to the joystick.  The :py:class:`~pyglet.input.Joystick` instance,
    axis name, and current value are passed as parameters to this event.

    To handle button events, you should attach on_joybutton_press and 
    on_joy_button_release event handlers to the joystick.  Both the :py:class:`~pyglet.input.Joystick`
    instance and the index of the changed button are passed as parameters to 
    these events.

    Alternately, you may attach event handlers to each individual button in 
    `button_controls` to receive on_press or on_release events.
    
    To use the hat switch, attach an on_joyhat_motion event handler to the
    joystick.  The handler will be called with both the hat_x and hat_y values
    whenever the value of the hat switch changes.

    The device name can be queried to get the name of the joystick.

    :Ivariables:
        `device` : `Device`
            The underlying device used by this joystick interface.
        `x` : float
            Current X (horizontal) value ranging from -1.0 (left) to 1.0
            (right).
        `y` : float
            Current y (vertical) value ranging from -1.0 (top) to 1.0
            (bottom).
        `z` : float
            Current Z value ranging from -1.0 to 1.0.  On joysticks the Z
            value is usually the throttle control.  On game controllers the Z
            value is usually the secondary thumb vertical axis.
        `rx` : float
            Current rotational X value ranging from -1.0 to 1.0.
        `ry` : float
            Current rotational Y value ranging from -1.0 to 1.0.
        `rz` : float
            Current rotational Z value ranging from -1.0 to 1.0.  On joysticks
            the RZ value is usually the twist of the stick.  On game
            controllers the RZ value is usually the secondary thumb horizontal
            axis.
        `hat_x` : int
            Current hat (POV) horizontal position; one of -1 (left), 0
            (centered) or 1 (right).
        `hat_y` : int
            Current hat (POV) vertical position; one of -1 (bottom), 0
            (centered) or 1 (top).
        `buttons` : list of bool
            List of boolean values representing current states of the buttons.
            These are in order, so that button 1 has value at ``buttons[0]``,
            and so on.
        `x_control` : `AbsoluteAxis`
            Underlying control for `x` value, or ``None`` if not available.
        `y_control` : `AbsoluteAxis`
            Underlying control for `y` value, or ``None`` if not available.
        `z_control` : `AbsoluteAxis`
            Underlying control for `z` value, or ``None`` if not available.
        `rx_control` : `AbsoluteAxis`
            Underlying control for `rx` value, or ``None`` if not available.
        `ry_control` : `AbsoluteAxis`
            Underlying control for `ry` value, or ``None`` if not available.
        `rz_control` : `AbsoluteAxis`
            Underlying control for `rz` value, or ``None`` if not available.
        `hat_x_control` : `AbsoluteAxis`
            Underlying control for `hat_x` value, or ``None`` if not available.
        `hat_y_control` : `AbsoluteAxis`
            Underlying control for `hat_y` value, or ``None`` if not available.
        `button_controls` : list of `Button`
            Underlying controls for `buttons` values.
    """

    def __init__(self, device):
        self.device = device

        self.x = 0
        self.y = 0
        self.z = 0
        self.rx = 0
        self.ry = 0
        self.rz = 0
        self.hat_x = 0
        self.hat_y = 0
        self.buttons = []

        self.x_control = None
        self.y_control = None
        self.z_control = None
        self.rx_control = None
        self.ry_control = None
        self.rz_control = None
        self.hat_x_control = None
        self.hat_y_control = None
        self.button_controls = []

        def add_axis(control):
            name = control.name
            scale = 2.0 / (control.max - control.min)
            bias = -1.0 - control.min * scale
            if control.inverted:
                scale = -scale
                bias = -bias
            setattr(self, name + '_control', control)

            @control.event
            def on_change(value):
                normalized_value = value * scale + bias
                setattr(self, name, normalized_value)
                self.dispatch_event('on_joyaxis_motion', self, name, normalized_value)

        def add_button(control):
            i = len(self.buttons)
            self.buttons.append(False)
            self.button_controls.append(control)

            @control.event
            def on_change(value):
                self.buttons[i] = value
            
            @control.event
            def on_press():
                self.dispatch_event('on_joybutton_press', self, i)

            @control.event
            def on_release():
                self.dispatch_event('on_joybutton_release', self, i)

        def add_hat(control):
            # 8-directional hat encoded as a single control (Windows/Mac)
            self.hat_x_control = control
            self.hat_y_control = control
            
            @control.event
            def on_change(value):
                if value & 0xffff == 0xffff:
                    self.hat_x = self.hat_y = 0
                else:
                    if control.max > 8:  # DirectInput: scale value
                        value //= 0xfff
                    if 0 <= value < 8:
                        self.hat_x, self.hat_y = (( 0,  1),
                                                  ( 1,  1),
                                                  ( 1,  0),
                                                  ( 1, -1),
                                                  ( 0, -1),
                                                  (-1, -1),
                                                  (-1,  0),
                                                  (-1,  1))[value]
                    else:
                        # Out of range
                        self.hat_x = self.hat_y = 0
                self.dispatch_event('on_joyhat_motion', self, self.hat_x, self.hat_y)

        for control in device.get_controls():
            if isinstance(control, AbsoluteAxis):
                if control.name in ('x', 'y', 'z', 'rx', 'ry', 'rz', 'hat_x', 'hat_y'):
                    add_axis(control)
                elif control.name == 'hat':
                    add_hat(control)
            elif isinstance(control, Button):
                add_button(control)

    def open(self, window=None, exclusive=False):
        """Open the joystick device.  See `Device.open`. """
        self.device.open(window, exclusive)

    def close(self):
        """Close the joystick device.  See `Device.close`. """
        self.device.close()

    def on_joyaxis_motion(self, joystick, axis, value):
        """The value of a joystick axis changed.

        :Parameters:
            `joystick` : `Joystick`
                The joystick device whose axis changed.
            `axis` : string
                The name of the axis that changed.
            `value` : float
                The current value of the axis, normalized to [-1, 1].
        """

    def on_joybutton_press(self, joystick, button):
        """A button on the joystick was pressed.

        :Parameters:
            `joystick` : `Joystick`
                The joystick device whose button was pressed.
            `button` : int
                The index (in `button_controls`) of the button that was pressed.
        """
        
    def on_joybutton_release(self, joystick, button):
        """A button on the joystick was released.

        :Parameters:
            `joystick` : `Joystick`
                The joystick device whose button was released.
            `button` : int
                The index (in `button_controls`) of the button that was released.
        """

    def on_joyhat_motion(self, joystick, hat_x, hat_y):
        """The value of the joystick hat switch changed.

        :Parameters:
            `joystick` : `Joystick`
                The joystick device whose hat control changed.
            `hat_x` : int
                Current hat (POV) horizontal position; one of -1 (left), 0
                (centered) or 1 (right).
            `hat_y` : int
                Current hat (POV) vertical position; one of -1 (bottom), 0
                (centered) or 1 (top).
        """

Joystick.register_event_type('on_joyaxis_motion')
Joystick.register_event_type('on_joybutton_press')
Joystick.register_event_type('on_joybutton_release')
Joystick.register_event_type('on_joyhat_motion')


class AppleRemote(EventDispatcher):
    """High-level interface for Apple remote control.

    This interface provides access to the 6 button controls on the remote.
    Pressing and holding certain buttons on the remote is interpreted as
    a separate control.

    :Ivariables:
        `device` : `Device`
            The underlying device used by this interface.
        `left_control` : `Button`
            Button control for the left (prev) button.
        `left_hold_control` : `Button`
            Button control for holding the left button (rewind).
        `right_control` : `Button`
            Button control for the right (next) button.
        `right_hold_control` : `Button`
            Button control for holding the right button (fast forward).
        `up_control` : `Button`
            Button control for the up (volume increase) button.
        `down_control` : `Button`
            Button control for the down (volume decrease) button.
        `select_control` : `Button`
            Button control for the select (play/pause) button.
        `select_hold_control` : `Button`
            Button control for holding the select button.
        `menu_control` : `Button`
            Button control for the menu button.
        `menu_hold_control` : `Button`
            Button control for holding the menu button.
    """
    
    def __init__(self, device):
        def add_button(control):
            setattr(self, control.name + '_control', control)

            @control.event
            def on_press():
                self.dispatch_event('on_button_press', control.name)

            @control.event
            def on_release():
                self.dispatch_event('on_button_release', control.name)
            
        self.device = device
        for control in device.get_controls():
            if control.name in ('left', 'left_hold', 'right', 'right_hold', 'up', 'down', 
                                'menu', 'select', 'menu_hold', 'select_hold'):
                add_button(control)

    def open(self, window=None, exclusive=False):
        """Open the device.  See `Device.open`. """
        self.device.open(window, exclusive)

    def close(self):
        """Close the device.  See `Device.close`. """
        self.device.close()

    def on_button_press(self, button):
        """A button on the remote was pressed.

        Only the 'up' and 'down' buttons will generate an event when the
        button is first pressed.  All other buttons on the remote will wait
        until the button is released and then send both the press and release
        events at the same time.

        :Parameters:
            `button` : unicode
                The name of the button that was pressed. The valid names are
                'up', 'down', 'left', 'right', 'left_hold', 'right_hold',
                'menu', 'menu_hold', 'select', and 'select_hold'
                
        :event:
        """

    def on_button_release(self, button):
        """A button on the remote was released.

        The 'select_hold' and 'menu_hold' button release events are sent
        immediately after the corresponding press events regardless of
        whether or not the user has released the button.

        :Parameters:
            `button` : unicode
                The name of the button that was released. The valid names are
                'up', 'down', 'left', 'right', 'left_hold', 'right_hold',
                'menu', 'menu_hold', 'select', and 'select_hold'

        :event:
        """

AppleRemote.register_event_type('on_button_press')
AppleRemote.register_event_type('on_button_release')


class Tablet(object):
    """High-level interface to tablet devices.

    Unlike other devices, tablets must be opened for a specific window,
    and cannot be opened exclusively.  The `open` method returns a
    `TabletCanvas` object, which supports the events provided by the tablet.

    Currently only one tablet device can be used, though it can be opened on
    multiple windows.  If more than one tablet is connected, the behaviour is
    undefined.
    """

    def open(self, window):
        """Open a tablet device for a window.

        :Parameters:
            `window` : `Window`
                The window on which the tablet will be used.

        :rtype: `TabletCanvas`
        """
        raise NotImplementedError('abstract')


class TabletCanvas(EventDispatcher):
    """Event dispatcher for tablets.

    Use `Tablet.open` to obtain this object for a particular tablet device and
    window.  Events may be generated even if the tablet stylus is outside of
    the window; this is operating-system dependent.

    The events each provide the `TabletCursor` that was used to generate the
    event; for example, to distinguish between a stylus and an eraser.  Only
    one cursor can be used at a time, otherwise the results are undefined.

    :Ivariables:
        `window` : Window
            The window on which this tablet was opened.
    """
    # OS X: Active window receives tablet events only when cursor is in window
    # Windows: Active window receives all tablet events
    #
    # Note that this means enter/leave pairs are not always consistent (normal
    # usage).

    def __init__(self, window):
        self.window = window

    def close(self):
        """Close the tablet device for this window.
        """
        raise NotImplementedError('abstract')

    if _is_pyglet_docgen:
        def on_enter(self, cursor):
            """A cursor entered the proximity of the window.  The cursor may
            be hovering above the tablet surface, but outside of the window
            bounds, or it may have entered the window bounds.

            Note that you cannot rely on `on_enter` and `on_leave` events to
            be generated in pairs; some events may be lost if the cursor was
            out of the window bounds at the time.

            :Parameters:
                `cursor` : `TabletCursor`
                    The cursor that entered proximity.

            :event:
            """

        def on_leave(self, cursor):
            """A cursor left the proximity of the window.  The cursor may have
            moved too high above the tablet surface to be detected, or it may
            have left the bounds of the window.

            Note that you cannot rely on `on_enter` and `on_leave` events to
            be generated in pairs; some events may be lost if the cursor was
            out of the window bounds at the time.

            :Parameters:
                `cursor` : `TabletCursor`
                    The cursor that left proximity.

            :event:
            """

        def on_motion(self, cursor, x, y, pressure):
            """The cursor moved on the tablet surface.

            If `pressure` is 0, then the cursor is actually hovering above the
            tablet surface, not in contact.

            :Parameters:
                `cursor` : `TabletCursor`
                    The cursor that moved.
                `x` : int
                    The X position of the cursor, in window coordinates.
                `y` : int
                    The Y position of the cursor, in window coordinates.
                `pressure` : float
                    The pressure applied to the cursor, in range 0.0 (no
                    pressure) to 1.0 (full pressure).
                `tilt_x` : float
                    Currently undefined.
                `tilt_y` : float
                    Currently undefined.

            :event:
            """

TabletCanvas.register_event_type('on_enter')
TabletCanvas.register_event_type('on_leave')
TabletCanvas.register_event_type('on_motion')


class TabletCursor(object):
    """A distinct cursor used on a tablet.

    Most tablets support at least a *stylus* and an *erasor* cursor; this
    object is used to distinguish them when tablet events are generated.

    :Ivariables:
        `name` : str
            Name of the cursor.
    """

    # TODO well-defined names for stylus and eraser.

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self.name)
