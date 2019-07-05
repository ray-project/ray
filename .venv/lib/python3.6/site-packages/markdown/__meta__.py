# -*- coding: utf-8 -*-
"""
Python Markdown

A Python implementation of John Gruber's Markdown.

Documentation: https://python-markdown.github.io/
GitHub: https://github.com/Python-Markdown/markdown/
PyPI: https://pypi.org/project/Markdown/

Started by Manfred Stienstra (http://www.dwerg.net/).
Maintained for a few years by Yuri Takhteyev (http://www.freewisdom.org).
Currently maintained by Waylan Limberg (https://github.com/waylan),
Dmitry Shachnev (https://github.com/mitya57) and Isaac Muse (https://github.com/facelessuser).

Copyright 2007-2018 The Python Markdown Project (v. 1.7 and later)
Copyright 2004, 2005, 2006 Yuri Takhteyev (v. 0.2-1.6b)
Copyright 2004 Manfred Stienstra (the original version)

License: BSD (see LICENSE.md for details).
"""

try:
    import packaging.version
except ImportError:
    from pkg_resources.extern import packaging

# __version_info__ format:
# (major, minor, patch, dev/alpha/beta/rc/final, #)
# (1, 1, 2, 'dev', 0) => "1.1.2.dev0"
# (1, 1, 2, 'alpha', 1) => "1.1.2a1"
# (1, 2, 0, 'beta', 2) => "1.2b2"
# (1, 2, 0, 'rc', 4) => "1.2rc4"
# (1, 2, 0, 'final', 0) => "1.2"
__version_info__ = (3, 1, 1, 'final', 0)


def _get_version():  # pragma: no cover
    " Returns a PEP 440-compliant version number from version_info. "
    assert len(__version_info__) == 5
    assert __version_info__[3] in ('dev', 'alpha', 'beta', 'rc', 'final')

    parts = 2 if __version_info__[2] == 0 else 3
    v = '.'.join(map(str, __version_info__[:parts]))

    if __version_info__[3] == 'dev':
        v += '.dev' + str(__version_info__[4])
    elif __version_info__[3] != 'final':
        mapping = {'alpha': 'a', 'beta': 'b', 'rc': 'rc'}
        v += mapping[__version_info__[3]] + str(__version_info__[4])

    # Ensure version is valid and normalized
    return str(packaging.version.Version(v))


__version__ = _get_version()
