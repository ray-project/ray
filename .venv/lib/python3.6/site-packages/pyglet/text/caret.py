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
# $Id:$

'''Provides keyboard and mouse editing procedures for text layout.

Example usage::

    from pyglet import window
    from pyglet.text import layout, caret

    my_window = window.Window(...)
    my_layout = layout.IncrementalTextLayout(...)
    my_caret = caret.Caret(my_layout)
    my_window.push_handlers(my_caret)

.. versionadded:: 1.1
'''
from builtins import object

__docformat__ = 'restructuredtext'
__version__ = '$Id: $'

import re
import time

from pyglet import clock
from pyglet import event
from pyglet.window import key

class Caret(object):
    '''Visible text insertion marker for 
    `pyglet.text.layout.IncrementalTextLayout`.

    The caret is drawn as a single vertical bar at the document `position` 
    on a text layout object.  If `mark` is not None, it gives the unmoving
    end of the current text selection.  The visible text selection on the
    layout is updated along with `mark` and `position`.
    
    By default the layout's graphics batch is used, so the caret does not need
    to be drawn explicitly.  Even if a different graphics batch is supplied,
    the caret will be correctly positioned and clipped within the layout.

    Updates to the document (and so the layout) are automatically propagated
    to the caret.  

    The caret object can be pushed onto a window event handler stack with
    `Window.push_handlers`.  The caret will respond correctly to keyboard,
    text, mouse and activation events, including double- and triple-clicks.
    If the text layout is being used alongside other graphical widgets, a
    GUI toolkit will be needed to delegate keyboard and mouse events to the
    appropriate widget.  pyglet does not provide such a toolkit at this stage.
    '''

    _next_word_re = re.compile(r'(?<=\W)\w')
    _previous_word_re = re.compile(r'(?<=\W)\w+\W*$')
    _next_para_re = re.compile(r'\n', flags=re.DOTALL)
    _previous_para_re = re.compile(r'\n', flags=re.DOTALL)

    _position = 0

    _active = True
    _visible = True
    _blink_visible = True
    _click_count = 0
    _click_time = 0

    #: Blink period, in seconds.
    PERIOD = 0.5

    #: Pixels to scroll viewport per mouse scroll wheel movement.  Defaults
    #: to 12pt at 96dpi.
    SCROLL_INCREMENT= 12 * 96 // 72

    def __init__(self, layout, batch=None, color=(0, 0, 0)):
        '''Create a caret for a layout.

        By default the layout's batch is used, so the caret does not need to
        be drawn explicitly.

        :Parameters:
            `layout` : `~pyglet.text.layout.TextLayout`
                Layout to control.
            `batch` : `~pyglet.graphics.Batch`
                Graphics batch to add vertices to.
            `color` : (int, int, int)
                RGB tuple with components in range [0, 255].

        '''
        from pyglet import gl
        self._layout = layout
        if batch is None:
            batch = layout.batch
        r, g, b = color
        colors = (r, g, b, 255, r, g, b, 255)
        self._list = batch.add(2, gl.GL_LINES, layout.background_group, 
            'v2f', ('c4B', colors))

        self._ideal_x = None
        self._ideal_line = None
        self._next_attributes = {}

        self.visible = True

        layout.push_handlers(self)

    def delete(self):
        '''Remove the caret from its batch.

        Also disconnects the caret from further layout events.
        '''
        self._list.delete()
        self._layout.remove_handlers(self)

    def _blink(self, dt):
        if self.PERIOD:
            self._blink_visible = not self._blink_visible
        if self._visible and self._active and self._blink_visible:
            alpha = 255
        else:
            alpha = 0
        self._list.colors[3] = alpha
        self._list.colors[7] = alpha

    def _nudge(self):
        self.visible = True

    def _set_visible(self, visible):
        self._visible = visible
        clock.unschedule(self._blink)
        if visible and self._active and self.PERIOD:
            clock.schedule_interval(self._blink, self.PERIOD)
            self._blink_visible = False # flipped immediately by next blink
        self._blink(0)

    def _get_visible(self):
        return self._visible

    visible = property(_get_visible, _set_visible, 
                       doc='''Caret visibility.
    
    The caret may be hidden despite this property due to the periodic blinking
    or by `on_deactivate` if the event handler is attached to a window.

    :type: bool
    ''')
    
    def _set_color(self, color):
        self._list.colors[:3] = color
        self._list.colors[4:7] = color

    def _get_color(self):
        return self._list.colors[:3]

    color = property(_get_color, _set_color, 
                     doc='''Caret color.

    The default caret color is ``[0, 0, 0]`` (black).  Each RGB color
    component is in the range 0 to 255.

    :type: (int, int, int)
    ''')

    def _set_position(self, index):
        self._position = index
        self._next_attributes.clear()
        self._update()

    def _get_position(self):
        return self._position

    position = property(_get_position, _set_position, 
                        doc='''Position of caret within document.

    :type: int
    ''')

    _mark = None
    def _set_mark(self, mark):
        self._mark = mark
        self._update(line=self._ideal_line)
        if mark is None:
            self._layout.set_selection(0, 0)
    
    def _get_mark(self):
        return self._mark

    mark = property(_get_mark, _set_mark,
                    doc='''Position of immovable end of text selection within
    document.

    An interactive text selection is determined by its immovable end (the
    caret's position when a mouse drag begins) and the caret's position, which
    moves interactively by mouse and keyboard input.

    This property is ``None`` when there is no selection.

    :type: int
    ''')

    def _set_line(self, line):
        if self._ideal_x is None:
            self._ideal_x, _ = \
                self._layout.get_point_from_position(self._position)
        self._position = \
            self._layout.get_position_on_line(line, self._ideal_x)
        self._update(line=line, update_ideal_x=False)

    def _get_line(self):
        if self._ideal_line is not None:
            return self._ideal_line
        else:
            return self._layout.get_line_from_position(self._position)

    line = property(_get_line, _set_line,
                    doc='''Index of line containing the caret's position.

    When set, `position` is modified to place the caret on requested line
    while maintaining the closest possible X offset.
                    
    :type: int
    ''')

    def get_style(self, attribute):
        '''Get the document's named style at the caret's current position.

        If there is a text selection and the style varies over the selection,
        `pyglet.text.document.STYLE_INDETERMINATE` is returned.

        :Parameters:
            `attribute` : str
                Name of style attribute to retrieve.  See
                `pyglet.text.document` for a list of recognised attribute
                names.

        :rtype: object
        '''
        if self._mark is None or self._mark == self._position:
            try:
                return self._next_attributes[attribute]
            except KeyError:
                return self._layout.document.get_style(attribute, 
                                                          self._position)

        start = min(self._position, self._mark)
        end = max(self._position, self._mark)
        return self._layout.document.get_style_range(attribute, start, end)

    def set_style(self, attributes):
        '''Set the document style at the caret's current position.

        If there is a text selection the style is modified immediately.
        Otherwise, the next text that is entered before the position is
        modified will take on the given style.

        :Parameters:
            `attributes` : dict
                Dict mapping attribute names to style values.  See
                `pyglet.text.document` for a list of recognised attribute
                names.

        '''

        if self._mark is None or self._mark == self._position:
            self._next_attributes.update(attributes)
            return

        start = min(self._position, self._mark)
        end = max(self._position, self._mark)
        self._layout.document.set_style(start, end, attributes)

    def _delete_selection(self):
        start = min(self._mark, self._position)
        end = max(self._mark, self._position)
        self._position = start
        self._mark = None
        self._layout.document.delete_text(start, end)
        self._layout.set_selection(0, 0)

    def move_to_point(self, x, y):
        '''Move the caret close to the given window coordinate.

        The `mark` will be reset to ``None``.

        :Parameters:
            `x` : int   
                X coordinate.
            `y` : int
                Y coordinate.

        '''
        line = self._layout.get_line_from_point(x, y)
        self._mark = None
        self._layout.set_selection(0, 0)
        self._position = self._layout.get_position_on_line(line, x)
        self._update(line=line)
        self._next_attributes.clear()

    def select_to_point(self, x, y):
        '''Move the caret close to the given window coordinate while
        maintaining the `mark`.

        :Parameters:
            `x` : int   
                X coordinate.
            `y` : int
                Y coordinate.

        '''
        line = self._layout.get_line_from_point(x, y)
        self._position = self._layout.get_position_on_line(line, x)
        self._update(line=line)
        self._next_attributes.clear()

    def select_word(self, x, y):
        '''Select the word at the given window coordinate.

        :Parameters:
            `x` : int   
                X coordinate.
            `y` : int
                Y coordinate.

        '''
        line = self._layout.get_line_from_point(x, y)
        p = self._layout.get_position_on_line(line, x)
        m1 = self._previous_word_re.search(self._layout.document.text, 
                                           0, p+1)
        if not m1:
            m1 = 0
        else:
            m1 = m1.start()
        self.mark = m1

        m2 = self._next_word_re.search(self._layout.document.text, p)
        if not m2:
            m2 = len(self._layout.document.text)
        else:
            m2 = m2.start()
        self._position = m2
        self._update(line=line)
        self._next_attributes.clear()

    def select_paragraph(self, x, y):
        '''Select the paragraph at the given window coordinate.

        :Parameters:
            `x` : int   
                X coordinate.
            `y` : int
                Y coordinate.

        '''
        line = self._layout.get_line_from_point(x, y)
        p = self._layout.get_position_on_line(line, x)
        self.mark = self._layout.document.get_paragraph_start(p)
        self._position = self._layout.document.get_paragraph_end(p)
        self._update(line=line) 
        self._next_attributes.clear()

    def _update(self, line=None, update_ideal_x=True):
        if line is None:
            line = self._layout.get_line_from_position(self._position)
            self._ideal_line = None
        else:
            self._ideal_line = line
        x, y = self._layout.get_point_from_position(self._position, line)
        if update_ideal_x:
            self._ideal_x = x

        x -= self._layout.top_group.translate_x
        y -= self._layout.top_group.translate_y
        font = self._layout.document.get_font(max(0, self._position - 1))
        self._list.vertices[:] = [x, y + font.descent, x, y + font.ascent]

        if self._mark is not None:
            self._layout.set_selection(min(self._position, self._mark),
                                          max(self._position, self._mark))

        self._layout.ensure_line_visible(line)
        self._layout.ensure_x_visible(x)

    def on_layout_update(self):
        if self.position > len(self._layout.document.text):
            self.position = len(self._layout.document.text)
        self._update()

    def on_text(self, text):
        '''Handler for the `pyglet.window.Window.on_text` event.

        Caret keyboard handlers assume the layout always has keyboard focus.
        GUI toolkits should filter keyboard and text events by widget focus
        before invoking this handler.
        '''
        if self._mark is not None:
            self._delete_selection()

        text = text.replace('\r', '\n')
        pos = self._position
        self._position += len(text)
        self._layout.document.insert_text(pos, text, self._next_attributes)
        self._nudge()
        return event.EVENT_HANDLED

    def on_text_motion(self, motion, select=False):
        '''Handler for the `pyglet.window.Window.on_text_motion` event.

        Caret keyboard handlers assume the layout always has keyboard focus.
        GUI toolkits should filter keyboard and text events by widget focus
        before invoking this handler.
        '''
        if motion == key.MOTION_BACKSPACE:
            if self.mark is not None:
                self._delete_selection()
            elif self._position > 0:
                self._position -= 1
                self._layout.document.delete_text(
                    self._position, self._position + 1)
        elif motion == key.MOTION_DELETE:
            if self.mark is not None:
                self._delete_selection()
            elif self._position < len(self._layout.document.text):
                self._layout.document.delete_text(
                    self._position, self._position + 1)
        elif self._mark is not None and not select:
            self._mark = None
            self._layout.set_selection(0, 0)

        if motion == key.MOTION_LEFT:
            self.position = max(0, self.position - 1)
        elif motion == key.MOTION_RIGHT:
            self.position = min(len(self._layout.document.text), 
                                self.position + 1) 
        elif motion == key.MOTION_UP:
            self.line = max(0, self.line - 1)
        elif motion == key.MOTION_DOWN:
            line = self.line
            if line < self._layout.get_line_count() - 1:
                self.line = line + 1
        elif motion == key.MOTION_BEGINNING_OF_LINE:
            self.position = self._layout.get_position_from_line(self.line)
        elif motion == key.MOTION_END_OF_LINE:
            line = self.line
            if line < self._layout.get_line_count() - 1:
                self._position = \
                    self._layout.get_position_from_line(line + 1) - 1
                self._update(line)
            else:
                self.position = len(self._layout.document.text)
        elif motion == key.MOTION_BEGINNING_OF_FILE:
            self.position = 0
        elif motion == key.MOTION_END_OF_FILE:
            self.position = len(self._layout.document.text)
        elif motion == key.MOTION_NEXT_WORD:
            pos = self._position + 1
            m = self._next_word_re.search(self._layout.document.text, pos)
            if not m:
                self.position = len(self._layout.document.text)
            else:
                self.position = m.start()
        elif motion == key.MOTION_PREVIOUS_WORD:
            pos = self._position
            m = self._previous_word_re.search(self._layout.document.text, 
                                              0, pos)
            if not m:
                self.position = 0
            else:
                self.position = m.start()

        self._next_attributes.clear()
        self._nudge()
        return event.EVENT_HANDLED

    def on_text_motion_select(self, motion):
        '''Handler for the `pyglet.window.Window.on_text_motion_select` event.

        Caret keyboard handlers assume the layout always has keyboard focus.
        GUI toolkits should filter keyboard and text events by widget focus
        before invoking this handler.
        '''
        if self.mark is None:
            self.mark = self.position
        self.on_text_motion(motion, True)
        return event.EVENT_HANDLED

    def on_mouse_scroll(self, x, y, scroll_x, scroll_y):
        '''Handler for the `pyglet.window.Window.on_mouse_scroll` event.

        Mouse handlers do not check the bounds of the coordinates: GUI
        toolkits should filter events that do not intersect the layout
        before invoking this handler.

        The layout viewport is scrolled by `SCROLL_INCREMENT` pixels per
        "click".
        '''
        self._layout.view_x -= scroll_x * self.SCROLL_INCREMENT
        self._layout.view_y += scroll_y * self.SCROLL_INCREMENT 
        return event.EVENT_HANDLED

    def on_mouse_press(self, x, y, button, modifiers):
        '''Handler for the `pyglet.window.Window.on_mouse_press` event.

        Mouse handlers do not check the bounds of the coordinates: GUI
        toolkits should filter events that do not intersect the layout
        before invoking this handler.

        This handler keeps track of the number of mouse presses within
        a short span of time and uses this to reconstruct double- and
        triple-click events for selecting words and paragraphs.  This
        technique is not suitable when a GUI toolkit is in use, as the active
        widget must also be tracked.  Do not use this mouse handler if
        a GUI toolkit is being used.
        '''
        t = time.time()
        if t - self._click_time < 0.25:
            self._click_count += 1
        else:
            self._click_count = 1
        self._click_time = time.time()

        if self._click_count == 1:
            self.move_to_point(x, y)
        elif self._click_count == 2:
            self.select_word(x, y)
        elif self._click_count == 3:
            self.select_paragraph(x, y)
            self._click_count = 0

        self._nudge()
        return event.EVENT_HANDLED

    def on_mouse_drag(self, x, y, dx, dy, buttons, modifiers):
        '''Handler for the `pyglet.window.Window.on_mouse_drag` event.

        Mouse handlers do not check the bounds of the coordinates: GUI
        toolkits should filter events that do not intersect the layout
        before invoking this handler.
        '''
        if self.mark is None:
            self.mark = self.position
        self.select_to_point(x, y)
        self._nudge()
        return event.EVENT_HANDLED

    def on_activate(self):
        '''Handler for the `pyglet.window.Window.on_activate` event.

        The caret is hidden when the window is not active.
        '''
        self._active = True
        self.visible = self._active
        return event.EVENT_HANDLED

    def on_deactivate(self):
        '''Handler for the `pyglet.window.Window.on_deactivate` event.

        The caret is hidden when the window is not active.
        '''
        self._active = False
        self.visible = self._active
        return event.EVENT_HANDLED
