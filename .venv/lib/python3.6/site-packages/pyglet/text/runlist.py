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
'''Run list encoding utilities.

.. versionadded:: 1.1
'''
from builtins import str
from builtins import zip
from builtins import next
from builtins import object

__docformat__ = 'restructuredtext'
__version__ = '$Id: $'

class _Run(object):
    def __init__(self, value, count):
        self.value = value
        self.count = count

    def __repr__(self):
        return 'Run(%r, %d)' % (self.value, self.count)

class RunList(object):
    '''List of contiguous runs of values.

    A `RunList` is an efficient encoding of a sequence of values.  For
    example, the sequence ``aaaabbccccc`` is encoded as ``(4, a), (2, b),
    (5, c)``.  The class provides methods for modifying and querying the
    run list without needing to deal with the tricky cases of splitting and
    merging the run list entries.

    Run lists are used to represent formatted character data in pyglet.  A
    separate run list is maintained for each style attribute, for example,
    bold, italic, font size, and so on.  Unless you are overriding the
    document interfaces, the only interaction with run lists is via
    `RunIterator`.

    The length and ranges of a run list always refer to the character
    positions in the decoded list.  For example, in the above sequence,
    ``set_run(2, 5, 'x')`` would change the sequence to ``aaxxxbccccc``.
    '''
    def __init__(self, size, initial):
        '''Create a run list of the given size and a default value.

        :Parameters:
            `size` : int
                Number of characters to represent initially.
            `initial` : object
                The value of all characters in the run list.

        '''
        self.runs = [_Run(initial, size)]

    def insert(self, pos, length):
        '''Insert characters into the run list.

        The inserted characters will take on the value immediately preceding
        the insertion point (or the value of the first character, if `pos` is
        0).

        :Parameters:
            `pos` : int
                Insertion index
            `length` : int
                Number of characters to insert.

        '''

        i = 0
        for run in self.runs:
            if i <= pos <= i + run.count:
                run.count += length
            i += run.count

    def delete(self, start, end):
        '''Remove characters from the run list.

        :Parameters:
            `start` : int
                Starting index to remove from.
            `end` : int
                End index, exclusive.

        '''
        i = 0
        for run in self.runs:
            if end - start == 0:
                break
            if i <= start <= i + run.count:
                trim = min(end - start, i + run.count - start)
                run.count -= trim
                end -= trim
            i += run.count
        self.runs = [r for r in self.runs if r.count > 0]

        # Don't leave an empty list
        if not self.runs:
            self.runs = [_Run(run.value, 0)]

    def set_run(self, start, end, value):
        '''Set the value of a range of characters.

        :Parameters:
            `start` : int
                Start index of range.
            `end` : int
                End of range, exclusive.
            `value` : object
                Value to set over the range.

        '''
        if end - start <= 0:
            return
        
        # Find runs that need to be split
        i = 0
        start_i = None
        start_trim = 0
        end_i = None
        end_trim = 0
        for run_i, run in enumerate(self.runs):
            count = run.count
            if i < start < i + count:
                start_i = run_i
                start_trim = start - i
            if i < end < i + count:
                end_i = run_i
                end_trim = end - i
            i += count
        
        # Split runs
        if start_i is not None:
            run = self.runs[start_i]
            self.runs.insert(start_i, _Run(run.value, start_trim))
            run.count -= start_trim
            if end_i is not None:
                if end_i == start_i:
                    end_trim -= start_trim
                end_i += 1
        if end_i is not None:
            run = self.runs[end_i]
            self.runs.insert(end_i, _Run(run.value, end_trim))
            run.count -= end_trim
                
        # Set new value on runs
        i = 0
        for run in self.runs:
            if start <= i and i + run.count <= end: 
                run.value = value
            i += run.count 

        # Merge adjacent runs
        last_run = self.runs[0]
        for run in self.runs[1:]:
            if run.value == last_run.value:
                run.count += last_run.count
                last_run.count = 0
            last_run = run

        # Delete collapsed runs
        self.runs = [r for r in self.runs if r.count > 0]

    def __iter__(self):
        i = 0
        for run in self.runs:
            yield i, i + run.count, run.value
            i += run.count

    def get_run_iterator(self):
        '''Get an extended iterator over the run list.

        :rtype: `RunIterator`
        '''
        return RunIterator(self)

    def __getitem__(self, index):
        '''Get the value at a character position.

        :Parameters:
            `index` : int
                Index of character.  Must be within range and non-negative.

        :rtype: object
        '''
        i = 0
        for run in self.runs:
            if i <= index < i + run.count:
                return run.value
            i += run.count

        # Append insertion point
        if index == i:
            return self.runs[-1].value

        raise IndexError

    def __repr__(self):
        return str(list(self))

class AbstractRunIterator(object):
    '''Range iteration over `RunList`.

    `AbstractRunIterator` objects allow any monotonically non-decreasing
    access of the iteration, including repeated iteration over the same index.
    Use the ``[index]`` operator to get the value at a particular index within
    the document.  For example::

        run_iter = iter(run_list)
        value = run_iter[0]
        value = run_iter[0]       # non-decreasing access is OK
        value = run_iter[15]
        value = run_iter[17]
        value = run_iter[16]      # this is illegal, the index decreased.

    Using `AbstractRunIterator` to access increasing indices of the value runs
    is more efficient than calling `RunList.__getitem__` repeatedly.

    You can also iterate over monotonically non-decreasing ranges over the
    iteration.  For example::
        
        run_iter = iter(run_list)
        for start, end, value in run_iter.ranges(0, 20):
            pass
        for start, end, value in run_iter.ranges(25, 30):
            pass
        for start, end, value in run_iter.ranges(30, 40):
            pass

    Both start and end indices of the slice are required and must be positive.
    '''

    def __getitem__(self, index):
        '''Get the value at a given index.

        See the class documentation for examples of valid usage.

        :Parameters:
            `index` : int   
                Document position to query.

        :rtype: object
        '''

    def ranges(self, start, end):
        '''Iterate over a subrange of the run list.

        See the class documentation for examples of valid usage.

        :Parameters:
            `start` : int
                Start index to iterate from.
            `end` : int
                End index, exclusive.

        :rtype: iterator
        :return: Iterator over (start, end, value) tuples.
        '''

class RunIterator(AbstractRunIterator):
    def __init__(self, run_list):
        self._run_list_iter = iter(run_list)
        self.start, self.end, self.value = next(self)
        
    def __next__(self):
        return next(self._run_list_iter)

    def __getitem__(self, index):
        try:
            while index >= self.end and index > self.start:
                # condition has special case for 0-length run (fixes issue 471)
                self.start, self.end, self.value = next(self)
            return self.value
        except StopIteration:
            raise IndexError

    def ranges(self, start, end):
        try:
            while start >= self.end:
                self.start, self.end, self.value = next(self)
            yield start, min(self.end, end), self.value
            while end > self.end:
                self.start, self.end, self.value = next(self)
                yield self.start, min(self.end, end), self.value
        except StopIteration:
            return

class OverriddenRunIterator(AbstractRunIterator):
    '''Iterator over a `RunIterator`, with a value temporarily replacing
    a given range.
    '''
    def __init__(self, base_iterator, start, end, value):
        '''Create a derived iterator.

        :Parameters:
            `start` : int
                Start of range to override
            `end` : int
                End of range to override, exclusive
            `value` : object
                Value to replace over the range

        '''
        self.iter = base_iterator
        self.override_start = start
        self.override_end = end
        self.override_value = value

    def ranges(self, start, end):
        if end <= self.override_start or start >= self.override_end:
            # No overlap
            for r in self.iter.ranges(start, end):
                yield r
        else:
            # Overlap: before, override, after
            if start < self.override_start < end:
                for r in self.iter.ranges(start, self.override_start):
                    yield r
            yield (max(self.override_start, start),
                   min(self.override_end, end),
                   self.override_value)
            if start < self.override_end < end:
                for r in self.iter.ranges(self.override_end, end):
                    yield r
        
    def __getitem__(self, index):
        if self.override_start <= index < self.override_end:
            return self.override_value
        else:
            return self.iter[index]

class FilteredRunIterator(AbstractRunIterator):
    '''Iterate over an `AbstractRunIterator` with filtered values replaced
    by a default value.
    '''
    def __init__(self, base_iterator, filter, default):
        '''Create a filtered run iterator.

        :Parameters:
            `base_iterator` : `AbstractRunIterator`
                Source of runs.
            `filter` : ``lambda object: bool``
                Function taking a value as parameter, and returning ``True``
                if the value is acceptable, and ``False`` if the default value
                should be substituted.
            `default` : object
                Default value to replace filtered values.

        '''
        self.iter = base_iterator
        self.filter = filter
        self.default = default

    def ranges(self, start, end):
        for start, end, value in self.iter.ranges(start, end):
            if self.filter(value):
                yield start, end, value
            else:
                yield start, end, self.default

    def __getitem__(self, index):
        value = self.iter[index]
        if self.filter(value):
            return value
        return self.default

class ZipRunIterator(AbstractRunIterator):
    '''Iterate over multiple run iterators concurrently.'''
    def __init__(self, range_iterators):
        self.range_iterators = range_iterators

    def ranges(self, start, end):
        try:
            iterators = [i.ranges(start, end) for i in self.range_iterators]
            starts, ends, values = zip(*[next(i) for i in iterators])
            starts = list(starts)
            ends = list(ends)
            values = list(values)
            while start < end:
                min_end = min(ends)
                yield start, min_end, values
                start = min_end
                for i, iterator in enumerate(iterators):
                    if ends[i] == min_end:
                        starts[i], ends[i], values[i] = next(iterator)
        except StopIteration:
            return

    def __getitem__(self, index):
        return [i[index] for i in self.range_iterators]

class ConstRunIterator(AbstractRunIterator):
    '''Iterate over a constant value without creating a RunList.'''
    def __init__(self, length, value):
        self.length = length
        self.value = value

    def __next__(self):
        yield 0, self.length, self.value

    def ranges(self, start, end):
        yield start, end, self.value

    def __getitem__(self, index):
        return self.value
