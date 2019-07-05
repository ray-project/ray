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

"""Precise framerate calculation, scheduling and framerate limiting.

Measuring time
==============

The `tick` and `get_fps` functions can be used in conjunction to fulfil most
games' basic requirements::

    from pyglet import clock
    while True:
        dt = clock.tick()
        # ... update and render ...
        print 'FPS is %f' % clock.get_fps()

The ``dt`` value returned gives the number of seconds (as a float) since the
last "tick".

The `get_fps` function averages the framerate over a sliding window of
approximately 1 second.  (You can calculate the instantaneous framerate by
taking the reciprocal of ``dt``).

Always remember to `tick` the clock!

Limiting frame-rate
===================

The framerate can be limited::

    clock.set_fps_limit(60)

This causes :py:class:`~pyglet.clock.Clock` to sleep during each `tick` in an
attempt to keep the number of ticks (frames) per second below 60.

The implementation uses platform-dependent high-resolution sleep functions
to achieve better accuracy with busy-waiting than would be possible using
just the `time` module.

Scheduling
==========

You can schedule a function to be called every time the clock is ticked::

    def callback(dt):
        print '%f seconds since last callback' % dt

    clock.schedule(callback)

The `schedule_interval` method causes a function to be called every "n"
seconds::

    clock.schedule_interval(callback, .5)   # called twice a second

The `schedule_once` method causes a function to be called once "n" seconds
in the future::

    clock.schedule_once(callback, 5)        # called in 5 seconds

All of the `schedule` methods will pass on any additional args or keyword args
you specify to the callback function::

    def animate(dt, velocity, sprite):
       sprite.position += dt * velocity

    clock.schedule(animate, velocity=5.0, sprite=alien)

You can cancel a function scheduled with any of these methods using
`unschedule`::

    clock.unschedule(animate)

Displaying FPS
==============

The ClockDisplay class provides a simple FPS counter.  You should create
an instance of ClockDisplay once during the application's start up::

    fps_display = clock.ClockDisplay()

Call draw on the ClockDisplay object for each frame::

    fps_display.draw()

There are several options to change the font, color and text displayed
within the __init__ method.

Using multiple clocks
=====================

The clock functions are all relayed to an instance of
:py:class:`~pyglet.clock.Clock` which is initialised with the module.  You can
get this instance to use directly::

    clk = clock.get_default()

You can also replace the default clock with your own:

    myclk = clock.Clock()
    clock.set_default(myclk)

Each clock maintains its own set of scheduled functions and FPS
limiting/measurement.  Each clock must be "ticked" separately.

Multiple and derived clocks potentially allow you to separate "game-time" and
"wall-time", or to synchronise your clock to an audio or video stream instead
of the system clock.
"""
from __future__ import print_function
from __future__ import division
from builtins import range
from builtins import object

import time
import ctypes
from operator import attrgetter
from heapq import heappush, heappop, heappushpop
from collections import deque

import pyglet.lib
from pyglet import compat_platform


__docformat__ = 'restructuredtext'
__version__ = '$Id$'


if compat_platform in ('win32', 'cygwin'):

    class _ClockBase(object):
        def sleep(self, microseconds):
            time.sleep(microseconds * 1e-6)

    _default_time_function = time.clock

else:
    _c = pyglet.lib.load_library('c')
    _c.usleep.argtypes = [ctypes.c_ulong]

    class _ClockBase(object):
        def sleep(self, microseconds):
            _c.usleep(int(microseconds))

    _default_time_function = time.time


class _ScheduledItem(object):
    __slots__ = ['func', 'args', 'kwargs']

    def __init__(self, func, args, kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs


class _ScheduledIntervalItem(object):
    __slots__ = ['func', 'interval', 'last_ts', 'next_ts',
                 'args', 'kwargs']

    def __init__(self, func, interval, last_ts, next_ts, args, kwargs):
        self.func = func
        self.interval = interval
        self.last_ts = last_ts
        self.next_ts = next_ts
        self.args = args
        self.kwargs = kwargs

    def __lt__(self, other):
        try:
            return self.next_ts < other.next_ts
        except AttributeError:
            return self.next_ts < other


class Clock(_ClockBase):
    """Class for calculating and limiting framerate.

    It is also used for calling scheduled functions.
    """

    #: The minimum amount of time in seconds this clock will attempt to sleep
    #: for when framerate limiting.  Higher values will increase the
    #: accuracy of the limiting but also increase CPU usage while
    #: busy-waiting.  Lower values mean the process sleeps more often, but is
    #: prone to over-sleep and run at a potentially lower or uneven framerate
    #: than desired.
    #: On Windows, MIN_SLEEP is larger because the default timer resolution
    #: is set by default to 15 .6 ms.
    MIN_SLEEP = 0.008 if compat_platform in ('win32', 'cygwin') else 0.005

    #: The amount of time in seconds this clock subtracts from sleep values
    #: to compensate for lazy operating systems.
    SLEEP_UNDERSHOOT = MIN_SLEEP - 0.001

    # List of functions to call every tick.
    _schedule_items = None

    # List of schedule interval items kept in sort order.
    _schedule_interval_items = None

    # If True, a sleep(0) is inserted on every tick.
    _force_sleep = False

    def __init__(self, fps_limit=None, time_function=_default_time_function):
        """Initialise a Clock, with optional framerate limit and custom time function.

        :Parameters:
            `fps_limit` : float
                If not None, the maximum allowable framerate. Defaults
                to None. Deprecated in pyglet 1.2.
            `time_function` : function
                Function to return the elapsed time of the application,
                in seconds.  Defaults to time.time, but can be replaced
                to allow for easy time dilation effects or game pausing.

        """
        super(Clock, self).__init__()
        self.time = time_function
        self.next_ts = self.time()
        self.last_ts = None
        self.times = deque()

        self.set_fps_limit(fps_limit)
        self.cumulative_time = 0

        self._schedule_items = []
        self._schedule_interval_items = []
        self._current_interval_item = None

    def update_time(self):
        """Get the elapsed time since the last call to `update_time`.

        This updates the clock's internal measure of time and returns
        the difference since the last update (or since the clock was created).

        .. versionadded:: 1.2

        :rtype: float
        :return: The number of seconds since the last `update_time`, or 0
            if this was the first time it was called.
        """
        ts = self.time()
        if self.last_ts is None:
            delta_t = 0
        else:
            delta_t = ts - self.last_ts
            self.times.appendleft(delta_t)
            if len(self.times) > self.window_size:
                self.cumulative_time -= self.times.pop()
        self.cumulative_time += delta_t
        self.last_ts = ts

        return delta_t

    def call_scheduled_functions(self, dt):
        """Call scheduled functions that elapsed on the last `update_time`.

        .. versionadded:: 1.2

        :Parameters:
            dt : float
                The elapsed time since the last update to pass to each
                scheduled function.  This is *not* used to calculate which
                functions have elapsed.

        :rtype: bool
        :return: True if any functions were called, otherwise False.
        """
        now = self.last_ts
        result = False  # flag indicates if any function was called

        # handle items scheduled for every tick
        if self._schedule_items:
            result = True
            # duplicate list in case event unschedules itself
            for item in list(self._schedule_items):
                item.func(dt, *item.args, **item.kwargs)

        # check the next scheduled item that is not called each tick
        # if it is scheduled in the future, then exit
        interval_items = self._schedule_interval_items
        try:
            if interval_items[0].next_ts > now:
                return result

        # raised when the interval_items list is empty
        except IndexError:
            return result

        # NOTE: there is no special handling required to manage things
        #       that are scheduled during this loop, due to the heap
        self._current_interval_item = item = None
        get_soft_next_ts = self._get_soft_next_ts
        while interval_items:

            # the scheduler will hold onto a reference to an item in
            # case it needs to be rescheduled.  it is more efficient
            # to push and pop the heap at once rather than two operations
            if item is None:
                item = heappop(interval_items)
            else:
                item = heappushpop(interval_items, item)

            # a scheduled function may try and unschedule itself
            # so we need to keep a reference to the current
            # item no longer on heap to be able to check
            self._current_interval_item = item

            # if next item is scheduled in the future then break
            if item.next_ts > now:
                break

            # execute the callback
            item.func(now - item.last_ts, *item.args, **item.kwargs)

            if item.interval:

                # Try to keep timing regular, even if overslept this time;
                # but don't schedule in the past (which could lead to
                # infinitely-worsening error).
                item.next_ts = item.last_ts + item.interval
                item.last_ts = now

                # test the schedule for the next execution
                if item.next_ts <= now:
                    # the scheduled time of this item has already passed
                    # so it must be rescheduled
                    if now - item.next_ts < 0.05:
                        # missed execution time by 'reasonable' amount, so
                        # reschedule at normal interval
                        item.next_ts = now + item.interval
                    else:
                        # missed by significant amount, now many events have
                        # likely missed execution. do a soft reschedule to
                        # avoid lumping many events together.
                        # in this case, the next dt will not be accurate
                        item.next_ts = get_soft_next_ts(now, item.interval)
                        item.last_ts = item.next_ts - item.interval
            else:
                # not an interval, so this item will not be rescheduled
                self._current_interval_item = item = None

        if item is not None:
            heappush(interval_items, item)

        return True

    def tick(self, poll=False):
        """Signify that one frame has passed.

        This will call any scheduled functions that have elapsed.

        :Parameters:
            `poll` : bool
                If True, the function will call any scheduled functions
                but will not sleep or busy-wait for any reason.  Recommended
                for advanced applications managing their own sleep timers
                only.

                Since pyglet 1.1.

        :rtype: float
        :return: The number of seconds since the last "tick", or 0 if this was
            the first frame.
        """
        if poll:
            if self.period_limit:
                self.next_ts += self.period_limit
        else:
            if self.period_limit:
                self._limit()

            if self._force_sleep:
                self.sleep(0)

        delta_t = self.update_time()
        self.call_scheduled_functions(delta_t)
        return delta_t

    def _limit(self):
        """Sleep until the next frame is due.

        Called automatically by :meth:`.tick` if a framerate limit has been
        set.

        This method uses several heuristics to determine whether to
        sleep or busy-wait (or both).
        """
        ts = self.time()
        # Sleep to just before the desired time
        sleeptime = self.get_sleep_time(False)
        while sleeptime - self.SLEEP_UNDERSHOOT > self.MIN_SLEEP:
            self.sleep(1000000 * (sleeptime - self.SLEEP_UNDERSHOOT))
            sleeptime = self.get_sleep_time(False)

        # Busy-loop CPU to get closest to the mark
        sleeptime = self.next_ts - self.time()
        while sleeptime > 0:
            sleeptime = self.next_ts - self.time()

        if sleeptime < -2 * self.period_limit:
            # Missed the time by a long shot, let's reset the clock
            # print >> sys.stderr, 'Step %f' % -sleeptime
            self.next_ts = ts + 2 * self.period_limit
        else:
            # Otherwise keep the clock steady
            self.next_ts += self.period_limit

    def get_sleep_time(self, sleep_idle):
        """Get the time until the next item is scheduled.

        This method considers all scheduled items and the current
        ``fps_limit``, if any.

        Applications can choose to continue receiving updates at the
        maximum framerate during idle time (when no functions are scheduled),
        or they can sleep through their idle time and allow the CPU to
        switch to other processes or run in low-power mode.

        If `sleep_idle` is ``True`` the latter behaviour is selected, and
        ``None`` will be returned if there are no scheduled items.

        Otherwise, if `sleep_idle` is ``False``, a sleep time allowing
        the maximum possible framerate (considering ``fps_limit``) will
        be returned; or an earlier time if a scheduled function is ready.

        :Parameters:
            `sleep_idle` : bool
                If True, the application intends to sleep through its idle
                time; otherwise it will continue ticking at the maximum
                frame rate allowed.

        :rtype: float
        :return: Time until the next scheduled event in seconds, or ``None``
            if there is no event scheduled.

        .. versionadded:: 1.1
        """
        if self._schedule_items or not sleep_idle:
            if not self.period_limit:
                return 0.
            else:
                wake_time = self.next_ts
                if self._schedule_interval_items:
                    wake_time = min(wake_time,
                                    self._schedule_interval_items[0].next_ts)
                return max(wake_time - self.time(), 0.)

        if self._schedule_interval_items:
            return max(self._schedule_interval_items[0].next_ts - self.time(),
                       0)

        return None

    def set_fps_limit(self, fps_limit):
        """Set the framerate limit.

        The framerate limit applies only when a function is scheduled
        for every frame.  That is, the framerate limit can be exceeded by
        scheduling a function for a very small period of time.

        :Parameters:
            `fps_limit` : float
                Maximum frames per second allowed, or None to disable
                limiting.

        :deprecated: Use `pyglet.app.run` and `schedule_interval` instead.
        """
        if not fps_limit:
            self.period_limit = None
        else:
            self.period_limit = 1. / fps_limit
        self.window_size = fps_limit or 60

    def get_fps_limit(self):
        """Get the framerate limit.

        :rtype: float
        :return: The framerate limit previously set in the constructor or
            `set_fps_limit`, or None if no limit was set.
        """
        if self.period_limit:
            return 1. / self.period_limit
        else:
            return 0

    def get_fps(self):
        """Get the average FPS of recent history.

        The result is the average of a sliding window of the last "n" frames,
        where "n" is some number designed to cover approximately 1 second.

        :rtype: float
        :return: The measured frames per second.
        """
        if not self.cumulative_time:
            return 0
        return len(self.times) / self.cumulative_time

    def _get_nearest_ts(self):
        """Get the nearest timestamp.

        Schedule from now, unless now is sufficiently close to last_ts, in
        which case use last_ts.  This clusters together scheduled items that
        probably want to be scheduled together.  The old (pre 1.1.1)
        behaviour was to always use self.last_ts, and not look at ts.  The
        new behaviour is needed because clock ticks can now be quite
        irregular, and span several seconds.
        """
        last_ts = self.last_ts or self.next_ts
        ts = self.time()
        if ts - last_ts > 0.2:
            return ts
        return last_ts

    def _get_soft_next_ts(self, last_ts, interval):
        def taken(ts, e):
            """Check if `ts` has already got an item scheduled nearby."""
            # TODO this function is slow and called very often.
            # Optimise it, maybe?
            for item in self._schedule_interval_items:
                if abs(item.next_ts - ts) <= e:
                    return True
                elif item.next_ts > ts + e:
                    return False

            return False

        # sorted list is required required to produce expected results
        # taken() will iterate through the heap, expecting it to be sorted
        # and will not always catch smallest value, so sort here.
        # do not remove the sort key...it is faster than relaying comparisons
        # NOTE: do not rewrite as popping from heap, as that is super slow!
        self._schedule_interval_items.sort(key=attrgetter('next_ts'))

        # Binary division over interval:
        #
        # 0                          interval
        # |--------------------------|
        #   5  3   6   2   7  4  8   1          Order of search
        #
        # i.e., first scheduled at interval,
        #       then at            interval/2
        #       then at            interval/4
        #       then at            interval*3/4
        #       then at            ...
        #
        # Schedule is hopefully then evenly distributed for any interval,
        # and any number of scheduled functions.

        next_ts = last_ts + interval
        if not taken(next_ts, interval / 4):
            return next_ts

        dt = interval
        divs = 1
        while True:
            next_ts = last_ts
            for i in range(divs - 1):
                next_ts += dt
                if not taken(next_ts, dt / 4):
                    return next_ts
            dt /= 2
            divs *= 2

            # Avoid infinite loop in pathological case
            if divs > 16:
                return next_ts

    def schedule(self, func, *args, **kwargs):
        """Schedule a function to be called every frame.

        The function should have a prototype that includes ``dt`` as the
        first argument, which gives the elapsed time, in seconds, since the
        last clock tick.  Any additional arguments given to this function
        are passed on to the callback::

            def callback(dt, *args, **kwargs):
                pass

        :Parameters:
            `func` : callable
                The function to call each frame.
        """
        item = _ScheduledItem(func, args, kwargs)
        self._schedule_items.append(item)

    def schedule_once(self, func, delay, *args, **kwargs):
        """Schedule a function to be called once after `delay` seconds.

        The callback function prototype is the same as for `schedule`.

        :Parameters:
            `func` : callable
                The function to call when the timer lapses.
            `delay` : float
                The number of seconds to wait before the timer lapses.
        """
        last_ts = self._get_nearest_ts()
        next_ts = last_ts + delay
        item = _ScheduledIntervalItem(func, 0, last_ts, next_ts, args, kwargs)
        heappush(self._schedule_interval_items, item)

    def schedule_interval(self, func, interval, *args, **kwargs):
        """Schedule a function to be called every `interval` seconds.

        Specifying an interval of 0 prevents the function from being
        called again (see `schedule` to call a function as often as possible).

        The callback function prototype is the same as for `schedule`.

        :Parameters:
            `func` : callable
                The function to call when the timer lapses.
            `interval` : float
                The number of seconds to wait between each call.

        """
        last_ts = self._get_nearest_ts()
        next_ts = last_ts + interval
        item = _ScheduledIntervalItem(func, interval, last_ts,
                                      next_ts, args, kwargs)
        heappush(self._schedule_interval_items, item)

    def schedule_interval_soft(self, func, interval, *args, **kwargs):
        """Schedule a function to be called every ``interval`` seconds.

        This method is similar to `schedule_interval`, except that the
        clock will move the interval out of phase with other scheduled
        functions so as to distribute CPU more load evenly over time.

        This is useful for functions that need to be called regularly,
        but not relative to the initial start time.  :py:mod:`pyglet.media`
        does this for scheduling audio buffer updates, which need to occur
        regularly -- if all audio updates are scheduled at the same time
        (for example, mixing several tracks of a music score, or playing
        multiple videos back simultaneously), the resulting load on the
        CPU is excessive for those intervals but idle outside.  Using
        the soft interval scheduling, the load is more evenly distributed.

        Soft interval scheduling can also be used as an easy way to schedule
        graphics animations out of phase; for example, multiple flags
        waving in the wind.

        .. versionadded:: 1.1

        :Parameters:
            `func` : callable
                The function to call when the timer lapses.
            `interval` : float
                The number of seconds to wait between each call.

        """
        next_ts = self._get_soft_next_ts(self._get_nearest_ts(), interval)
        last_ts = next_ts - interval
        item = _ScheduledIntervalItem(func, interval, last_ts,
                                      next_ts, args, kwargs)
        heappush(self._schedule_interval_items, item)

    def unschedule(self, func):
        """Remove a function from the schedule.

        If the function appears in the schedule more than once, all occurrences
        are removed.  If the function was not scheduled, no error is raised.

        :Parameters:
            `func` : callable
                The function to remove from the schedule.

        """
        # clever remove item without disturbing the heap:
        # 1. set function to an empty lambda -- original function is not called
        # 2. set interval to 0               -- item will be removed from heap
        #                                                           eventually
        valid_items = set(item
                          for item in self._schedule_interval_items
                          if item.func == func)

        if self._current_interval_item:
            if self._current_interval_item.func == func:
                valid_items.add(self._current_interval_item)

        for item in valid_items:
            item.interval = 0
            item.func = lambda x, *args, **kwargs: x

        self._schedule_items = [i for i in self._schedule_items
                                if i.func != func]


# Default clock.
_default = Clock()


def set_default(default):
    """Set the default clock to use for all module-level functions.

    By default an instance of :py:class:`~pyglet.clock.Clock` is used.

    :Parameters:
        `default` : `Clock`
            The default clock to use.
    """
    global _default
    _default = default


def get_default():
    """Get the pyglet default Clock.

    Return the :py:class:`~pyglet.clock.Clock` instance that is used by all
    module-level clock functions.

    :rtype: `Clock`
    :return: The default clock.
    """
    return _default


def tick(poll=False):
    """Signify that one frame has passed on the default clock.

    This will call any scheduled functions that have elapsed.

    :Parameters:
        `poll` : bool
            If True, the function will call any scheduled functions
            but will not sleep or busy-wait for any reason.  Recommended
            for advanced applications managing their own sleep timers
            only.

            Since pyglet 1.1.

    :rtype: float
    :return: The number of seconds since the last "tick", or 0 if this was the
        first frame.
    """
    return _default.tick(poll)


def get_sleep_time(sleep_idle):
    """Get the time until the next item is scheduled on the default clock.

    See `Clock.get_sleep_time` for details.

    :Parameters:
        `sleep_idle` : bool
            If True, the application intends to sleep through its idle
            time; otherwise it will continue ticking at the maximum
            frame rate allowed.

    :rtype: float
    :return: Time until the next scheduled event in seconds, or ``None``
        if there is no event scheduled.

    .. versionadded:: 1.1
    """
    return _default.get_sleep_time(sleep_idle)


def get_fps():
    """Return the current measured FPS of the default clock.

    :rtype: float
    """
    return _default.get_fps()


def set_fps_limit(fps_limit):
    """Set the framerate limit for the default clock.

    :Parameters:
        `fps_limit` : float
            Maximum frames per second allowed, or None to disable
            limiting.

    :deprecated: Use `pyglet.app.run` and `schedule_interval` instead.
    """
    _default.set_fps_limit(fps_limit)


def get_fps_limit():
    """Get the framerate limit for the default clock.

    :return: The framerate limit previously set by `set_fps_limit`, or None if
        no limit was set.

    """
    return _default.get_fps_limit()


def schedule(func, *args, **kwargs):
    """Schedule 'func' to be called every frame on the default clock.

    The arguments passed to func are ``dt``, followed by any ``*args`` and
    ``**kwargs`` given here.

    :Parameters:
        `func` : callable
            The function to call each frame.
    """
    _default.schedule(func, *args, **kwargs)


def schedule_interval(func, interval, *args, **kwargs):
    """Schedule ``func`` on the default clock every interval seconds.

    The arguments passed to ``func`` are ``dt`` (time since last function
    call), followed by any ``*args`` and ``**kwargs`` given here.

    :Parameters:
        `func` : callable
            The function to call when the timer lapses.
        `interval` : float
            The number of seconds to wait between each call.
    """
    _default.schedule_interval(func, interval, *args, **kwargs)


def schedule_interval_soft(func, interval, *args, **kwargs):
    """Schedule ``func`` on the default clock every interval seconds.

    The clock will move the interval out of phase with other scheduled
    functions so as to distribute CPU more load evenly over time.

    The arguments passed to ``func`` are ``dt`` (time since last function
    call), followed by any ``*args`` and ``**kwargs`` given here.

    :see: `Clock.schedule_interval_soft`

    .. versionadded:: 1.1

    :Parameters:
        `func` : callable
            The function to call when the timer lapses.
        `interval` : float
            The number of seconds to wait between each call.

    """
    _default.schedule_interval_soft(func, interval, *args, **kwargs)


def schedule_once(func, delay, *args, **kwargs):
    """Schedule ``func`` to be called once after ``delay`` seconds.

    This function uses the fefault clock. ``delay`` can be a float. The
    arguments passed to ``func`` are ``dt`` (time since last function call),
    followed by any ``*args`` and ``**kwargs`` given here.

    If no default clock is set, the func is queued and will be scheduled
    on the default clock as soon as it is created.

    :Parameters:
        `func` : callable
            The function to call when the timer lapses.
        `delay` : float
            The number of seconds to wait before the timer lapses.
    """
    _default.schedule_once(func, delay, *args, **kwargs)


def unschedule(func):
    """Remove ``func`` from the default clock's schedule.

    No error is raised if the ``func`` was never scheduled.

    :Parameters:
        `func` : callable
            The function to remove from the schedule.
    """
    _default.unschedule(func)


class ClockDisplay(object):
    """Display current clock values, such as FPS.

    This is a convenience class for displaying diagnostics such as the
    framerate.  See the module documentation for example usage.

    :Ivariables:
        `label` : `pyglet.font.Text`
            The label which is displayed.

    :deprecated: This class presents values that are often misleading, as
        they reflect the rate of clock ticks, not displayed framerate.  Use
        pyglet.window.FPSDisplay instead.

    """

    def __init__(self,
                 font=None,
                 interval=0.25,
                 format='%(fps).2f',
                 color=(.5, .5, .5, .5),
                 clock=None):
        """Create a ClockDisplay.

        All parameters are optional.  By default, a large translucent
        font will be used to display the FPS to two decimal places.

        :Parameters:
            `font` : `pyglet.font.Font`
                The font to format text in.
            `interval` : float
                The number of seconds between updating the display.
            `format` : str
                A format string describing the format of the text.  This
                string is modulated with the dict ``{'fps' : fps}``.
            `color` : 4-tuple of float
                The color, including alpha, passed to ``glColor4f``.
            `clock` : `Clock`
                The clock which determines the time.  If None, the default
                clock is used.

        """
        if clock is None:
            clock = _default
        self.clock = clock
        self.clock.schedule_interval(self.update_text, interval)

        if not font:
            from pyglet.font import load as load_font
            font = load_font('', 36, bold=True)

        import pyglet.font
        self.label = pyglet.font.Text(font, '', color=color, x=10, y=10)

        self.format = format

    def unschedule(self):
        """Remove the display from its clock's schedule.

        :class:`~pyglet.clock.ClockDisplay` uses
        :class:`~pyglet.clocl.Clock.schedule_interval` to periodically update
        its display label.  Even if the ClockDisplay is not being used any
        more, its update method will still be scheduled, which can be a
        resource drain.  Call this method to unschedule the update method
        and allow the ClockDisplay to be garbage collected.

        .. versionadded:: 1.1
        """
        self.clock.unschedule(self.update_text)

    def update_text(self, dt=0):
        """Scheduled method to update the label text."""
        fps = self.clock.get_fps()
        self.label.text = self.format % {'fps': fps}

    def draw(self):
        """Method called each frame to render the label."""
        self.label.draw()


def test_clock():
    """Test clock implementation."""
    import getopt
    import sys
    test_seconds = 1
    test_fps = 60
    show_fps = False
    options, args = getopt.getopt(sys.argv[1:], 'vht:f:',
                                  ['time=', 'fps=', 'help'])
    for key, value in options:
        if key in ('-t', '--time'):
            test_seconds = float(value)
        elif key in ('-f', '--fps'):
            test_fps = float(value)
        elif key in ('-v', ):
            show_fps = True
        elif key in ('-h', '--help'):
            print('Usage: clock.py <options>\n'
                  '\n'
                  'Options:\n'
                  '  -t   --time       Number of seconds to run for.\n'
                  '  -f   --fps        Target FPS.\n'
                  '\n'
                  'Tests the clock module by measuring how close we can\n'
                  'get to the desired FPS by sleeping and busy-waiting.')
            sys.exit(0)

    set_fps_limit(test_fps)
    start = time.time()

    # Add one because first frame has no update interval.
    n_frames = int(test_seconds * test_fps + 1)

    print('Testing %f FPS for %f seconds...' % (test_fps, test_seconds))
    for i in range(n_frames):
        tick()
        if show_fps:
            print(get_fps())
    total_time = time.time() - start
    total_error = total_time - test_seconds
    print('Total clock error: %f secs' % total_error)
    print('Total clock error / secs: %f secs/secs' %
          (total_error / test_seconds))

    # Not fair to add the extra frame in this calc, since no-one's interested
    # in the startup situation.
    print('Average FPS: %f' % ((n_frames - 1) / total_time))


if __name__ == '__main__':
    test_clock()
