{#
  It's a known bug (https://github.com/sphinx-doc/sphinx/issues/9884)
  that autosummary will generate warning for inherited instance attributes.
  Those warnings will fail our build.
  For now, we don't autosummary classes with inherited instance attributes.
  To opt out, use `:template: autosummary/class_without_autosummary.rst`
#}

{{ fullname | escape | underline}}

.. currentmodule:: {{ module }}

.. autoclass:: {{ objname }}()
    :members:
