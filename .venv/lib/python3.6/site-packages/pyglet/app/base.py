from __future__ import print_function
from __future__ import division
from future import standard_library
standard_library.install_aliases()
from builtins import next
from builtins import object

import platform
import queue
import sys
import threading

from pyglet import app
from pyglet import compat_platform
from pyglet import clock
from pyglet import event

_is_pyglet_docgen = hasattr(sys, 'is_pyglet_docgen') and sys.is_pyglet_docgen


class PlatformEventLoop(object):
    """ Abstract class, implementation depends on platform.
    
    .. versionadded:: 1.2
    """
    def __init__(self):
        self._event_queue = queue.Queue()
        self._is_running = threading.Event()
        self._is_running.clear()

    def is_running(self):  
        """Return True if the event loop is currently processing, or False
        if it is blocked or not activated.

        :rtype: bool
        """
        return self._is_running.is_set()

    def post_event(self, dispatcher, event, *args):
        """Post an event into the main application thread.

        The event is queued internally until the :py:meth:`run` method's thread
        is able to dispatch the event.  This method can be safely called
        from any thread.

        If the method is called from the :py:meth:`run` method's thread (for 
        example, from within an event handler), the event may be dispatched 
        within the same runloop iteration or the next one; the choice is
        nondeterministic.

        :Parameters:
            `dispatcher` : EventDispatcher
                Dispatcher to process the event.
            `event` : str
                Event name.
            `args` : sequence
                Arguments to pass to the event handlers.

        """
        self._event_queue.put((dispatcher, event, args))
        self.notify()

    def dispatch_posted_events(self):
        """Immediately dispatch all pending events.

        Normally this is called automatically by the runloop iteration.
        """
        while True:
            try:
                dispatcher, event, args = self._event_queue.get(False)
            except queue.Empty:
                break

            dispatcher.dispatch_event(event, *args)

    def notify(self):
        """Notify the event loop that something needs processing.

        If the event loop is blocked, it will unblock and perform an iteration
        immediately.  If the event loop is running, another iteration is
        scheduled for immediate execution afterwards.
        """
        raise NotImplementedError('abstract')

    def start(self):
        pass

    def step(self, timeout=None):
        """:TODO: in mac/linux: return True if didn't time out"""
        raise NotImplementedError('abstract')

    def set_timer(self, func, interval):
        raise NotImplementedError('abstract')

    def stop(self):
        pass


class EventLoop(event.EventDispatcher):
    """The main run loop of the application.

    Calling `run` begins the application event loop, which processes
    operating system events, calls :py:func:`pyglet.clock.tick` to call 
    scheduled functions and calls :py:meth:`pyglet.window.Window.on_draw` and
    :py:meth:`pyglet.window.Window.flip` to update window contents.

    Applications can subclass :py:class:`EventLoop` and override certain methods
    to integrate another framework's run loop, or to customise processing
    in some other way.  You should not in general override :py:meth:`run`, as
    this method contains platform-specific code that ensures the application
    remains responsive to the user while keeping CPU usage to a minimum.
    """

    _has_exit_condition = None
    _has_exit = False

    def __init__(self):
        self._has_exit_condition = threading.Condition()
        self.clock = clock.get_default()
        self.is_running = False

    def run(self):
        """Begin processing events, scheduled functions and window updates.

        This method returns when :py:attr:`has_exit` is set to True.

        Developers are discouraged from overriding this method, as the
        implementation is platform-specific.
        """
        self.has_exit = False
        self._legacy_setup()

        platform_event_loop = app.platform_event_loop
        platform_event_loop.start()
        self.dispatch_event('on_enter')

        self.is_running = True
        legacy_platforms = ('XP', '2000', '2003Server', 'post2003')
        if compat_platform == 'win32' and platform.win32_ver()[0] in legacy_platforms:
            self._run_estimated()
        else:
            self._run()
        self.is_running = False

        self.dispatch_event('on_exit')
        platform_event_loop.stop()

    def _run(self):
        """The simplest standard run loop, using constant timeout.  Suitable
        for well-behaving platforms (Mac, Linux and some Windows).
        """
        platform_event_loop = app.platform_event_loop
        while not self.has_exit:
            timeout = self.idle()
            platform_event_loop.step(timeout)

    def _run_estimated(self):
        """Run-loop that continually estimates function mapping requested
        timeout to measured timeout using a least-squares linear regression.
        Suitable for oddball platforms (Windows).

        XXX: There is no real relation between the timeout given by self.idle(), and used
        to calculate the estimate, and the time actually spent waiting for events. I have
        seen this cause a negative gradient, showing a negative relation. Then CPU use
        runs out of control due to very small estimates.
        """
        platform_event_loop = app.platform_event_loop

        predictor = self._least_squares()
        gradient, offset = next(predictor)

        time = self.clock.time
        while not self.has_exit:
            timeout = self.idle()
            if timeout is None: 
                estimate = None
            else:
                estimate = max(gradient * timeout + offset, 0.0)
            if False:
                print('Gradient = %f, Offset = %f' % (gradient, offset))
                print('Timeout = %f, Estimate = %f' % (timeout, estimate))

            t = time()
            if not platform_event_loop.step(estimate) and estimate != 0.0 and estimate is not None:
                dt = time() - t
                gradient, offset = predictor.send((dt, estimate))

    @staticmethod
    def _least_squares(gradient=1, offset=0):
        X = 0
        Y = 0
        XX = 0
        XY = 0
        n = 0

        while True:
            x, y = yield gradient, offset
            X += x
            Y += y
            XX += x * x
            XY += x * y
            n += 1

            try:
                gradient = (n * XY - X * Y) / (n * XX - X * X)
                offset = (Y - gradient * X) / n
            except ZeroDivisionError:
                # Can happen in pathalogical case; keep current
                # gradient/offset for now.
                pass

    def _legacy_setup(self):
        # Disable event queuing for dispatch_events
        from pyglet.window import Window
        Window._enable_event_queue = False
        
        # Dispatch pending events
        for window in app.windows:
            window.switch_to()
            window.dispatch_pending_events()

    def enter_blocking(self):
        """Called by pyglet internal processes when the operating system
        is about to block due to a user interaction.  For example, this
        is common when the user begins resizing or moving a window.

        This method provides the event loop with an opportunity to set up
        an OS timer on the platform event loop, which will continue to
        be invoked during the blocking operation.

        The default implementation ensures that :py:meth:`idle` continues to be
        called as documented.

        .. versionadded:: 1.2
        """
        timeout = self.idle()
        app.platform_event_loop.set_timer(self._blocking_timer, timeout)

    def exit_blocking(self):
        """Called by pyglet internal processes when the blocking operation
        completes.  See :py:meth:`enter_blocking`.
        """
        app.platform_event_loop.set_timer(None, None)

    def _blocking_timer(self):
        timeout = self.idle()
        app.platform_event_loop.set_timer(self._blocking_timer, timeout)

    def idle(self):
        """Called during each iteration of the event loop.

        The method is called immediately after any window events (i.e., after
        any user input).  The method can return a duration after which
        the idle method will be called again.  The method may be called
        earlier if the user creates more input events.  The method
        can return `None` to only wait for user events.

        For example, return ``1.0`` to have the idle method called every
        second, or immediately after any user events.

        The default implementation dispatches the
        :py:meth:`pyglet.window.Window.on_draw` event for all windows and uses
        :py:func:`pyglet.clock.tick` and :py:func:`pyglet.clock.get_sleep_time`
        on the default clock to determine the return value.

        This method should be overridden by advanced users only.  To have
        code execute at regular intervals, use the
        :py:func:`pyglet.clock.schedule` methods.

        :rtype: float
        :return: The number of seconds before the idle method should
            be called again, or `None` to block for user input.
        """
        dt = self.clock.update_time()
        redraw_all = self.clock.call_scheduled_functions(dt)

        # Redraw all windows
        for window in app.windows:
            if redraw_all or (window._legacy_invalid and window.invalid):
                window.switch_to()
                window.dispatch_event('on_draw')
                window.flip()
                window._legacy_invalid = False

        # Update timout
        return self.clock.get_sleep_time(True)

    def _get_has_exit(self):
        self._has_exit_condition.acquire()
        result = self._has_exit
        self._has_exit_condition.release()
        return result

    def _set_has_exit(self, value):
        self._has_exit_condition.acquire()
        self._has_exit = value
        self._has_exit_condition.notify()
        self._has_exit_condition.release()

    @property
    def has_exit(self):
        """Flag indicating if the event loop will exit in
        the next iteration.  When set, all waiting threads are interrupted (see
        :py:meth:`sleep`).
        
        Thread-safe since pyglet 1.2.
    
        :see: `exit`
        :type: bool
        """
        return self._get_has_exit()

    @has_exit.setter
    def has_exit(self, value):
        self._set_has_exit(value)

    def exit(self):
        """Safely exit the event loop at the end of the current iteration.

        This method is a thread-safe equivalent for for setting 
        :py:attr:`has_exit` to ``True``.  All waiting threads will be
        interrupted (see :py:meth:`sleep`).
        """
        self._set_has_exit(True)
        app.platform_event_loop.notify()

    def sleep(self, timeout):
        """Wait for some amount of time, or until the :py:attr:`has_exit` flag
        is set or :py:meth:`exit` is called.

        This method is thread-safe.

        :Parameters:
            `timeout` : float
                Time to wait, in seconds.

        .. versionadded:: 1.2

        :rtype: bool
        :return: ``True`` if the `has_exit` flag is set, otherwise ``False``.
        """
        self._has_exit_condition.acquire()
        self._has_exit_condition.wait(timeout)
        result = self._has_exit
        self._has_exit_condition.release()
        return result

    def on_window_close(self, window):
        """Default window close handler."""
        if len(app.windows) == 0:
            self.exit()

    if _is_pyglet_docgen:
        def on_window_close(self, window):
            """A window was closed.

            This event is dispatched when a window is closed.  It is not
            dispatched if the window's close button was pressed but the
            window did not close.

            The default handler calls :py:meth:`exit` if no more windows are
            open.  You can override this handler to base your application exit
            on some other policy.

            :event:
            """

        def on_enter(self):
            """The event loop is about to begin.

            This is dispatched when the event loop is prepared to enter
            the main run loop, and represents the last chance for an
            application to initialise itself.

            :event:
            """

        def on_exit(self):
            """The event loop is about to exit.

            After dispatching this event, the :py:meth:`run` method returns (the
            application may not actually exit if you have more code
            following the :py:meth:`run` invocation).

            :event:
            """

EventLoop.register_event_type('on_window_close')
EventLoop.register_event_type('on_enter')
EventLoop.register_event_type('on_exit')
