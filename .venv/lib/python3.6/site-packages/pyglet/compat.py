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

'''Compatibility tools

Various tools for simultaneous Python 2.x and Python 3.x support
'''

__docformat__ = 'restructuredtext'
__version__ = '$Id$'

import sys
import itertools

if sys.version_info[0] == 2:
    if sys.version_info[1] < 6:
        #Pure Python implementation from
        #http://docs.python.org/library/itertools.html#itertools.izip_longest
        def izip_longest(*args, **kwds):
            # izip_longest('ABCD', 'xy', fillvalue='-') --> Ax By C- D-
            fillvalue = kwds.get('fillvalue')
            def sentinel(counter = ([fillvalue]*(len(args)-1)).pop):
                yield counter()     # yields the fillvalue, or raises IndexError
            fillers = itertools.repeat(fillvalue)
            iters = [itertools.chain(it, sentinel(), fillers) for it in args]
            try:
                for tup in itertools.izip(*iters):
                    yield tup
            except IndexError:
                pass
    else:
        izip_longest = itertools.izip_longest
else:
    izip_longest = itertools.zip_longest


if sys.version_info[0] >= 3:
    import io
    
    def asbytes(s):
        if isinstance(s, bytes):
            return s
        elif isinstance(s, str):
            return bytes(ord(c) for c in s)
        else:
            return bytes(s)

    def asbytes_filename(s):
        if isinstance(s, bytes):
            return s
        elif isinstance(s, str):
            return s.encode(encoding=sys.getfilesystemencoding())
    
    def asstr(s):
        if s is None:
            return ''
        if isinstance(s, str):
            return s
        return s.decode("utf-8")
    
    bytes_type = bytes
    BytesIO = io.BytesIO
else:
    import StringIO
    
    asbytes = str
    asbytes_filename = str
    asstr = str
    bytes_type = str
    BytesIO = StringIO.StringIO
