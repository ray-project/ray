Installation Troubleshooting
============================

Trouble installing Arrow
-------------------------

Some candidate possibilities.

You have a different version of Flatbuffers installed
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Arrow pulls and builds its own copy of Flatbuffers, but if you already have
Flatbuffers installed, Arrow may find the wrong version. If a directory like
``/usr/local/include/flatbuffers`` shows up in the output, this may be the
problem. To solve it, get rid of the old version of flatbuffers.

There is some problem with Boost
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If a message like ``Unable to find the requested Boost libraries`` appears when
installing Arrow, there may be a problem with Boost. This can happen if you
installed Boost using MacPorts. This is sometimes solved by using Brew instead.

Trouble installing or running Ray
---------------------------------

One of the Ray libraries is compiled against the wrong version of Python
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If there is a segfault or a sigabort immediately upon importing Ray, one of the
components may have been compiled against the wrong Python libraries. CMake
should normally find the right version of Python, but this process is not
completely reliable. In this case, check the CMake output from installation and
make sure that the version of the Python libraries that were found match the
version of Python that you're using.

Note that it's common to have multiple versions of Python on your machine (for
example both Python 2 and Python 3). Ray will be compiled against whichever
version of Python is found when you run the ``python`` command from the
command line, so make sure this is the version you wish to use.
