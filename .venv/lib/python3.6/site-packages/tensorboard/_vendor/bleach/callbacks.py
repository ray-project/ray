"""A set of basic callbacks for bleach.linkify."""
from __future__ import unicode_literals


def nofollow(attrs, new=False):
    if attrs['href'].startswith('mailto:'):
        return attrs
    rel = [x for x in attrs.get('rel', '').split(' ') if x]
    if 'nofollow' not in [x.lower() for x in rel]:
        rel.append('nofollow')
    attrs['rel'] = ' '.join(rel)

    return attrs


def target_blank(attrs, new=False):
    if attrs['href'].startswith('mailto:'):
        return attrs
    attrs['target'] = '_blank'
    return attrs
