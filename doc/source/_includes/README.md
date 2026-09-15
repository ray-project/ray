# Shared documentation fragments

Files here are spliced into pages with the `include` directive. They aren't pages
themselves, and `exclude_patterns` in `conf.py` keeps Sphinx from building them.

````markdown
```{include} /_includes/_deprecation.md
```
````

An included file renders through the parent page's renderer, so substitutions the
parent defines in its front matter resolve inside the fragment. Front matter in the
fragment itself is stripped and ignored, so a fragment can't carry its own defaults.
Put defaults in the substitution value in `conf.py` instead.

## Deprecation notices

`_deprecation.md` and `_deprecation_planned.md` render a standard deprecation notice
as an admonition. They wrap the `deprecation_notice` and `deprecation_planned`
substitutions defined in `myst_substitutions` in `conf.py`, which you can also
reference inline when the notice belongs inside a sentence or list item.

The wording says "will" because a committed deprecation timeline is a real future
event. Keeping the sentence here rather than in the pages also keeps it outside the
paths Vale lints, so the blanket `Google.Will` rule can keep guarding ordinary prose.

For the values these fragments expect and when to use each one, see the deprecation
notices section of `../ray-contribute/writing-style.md`.
