{#
  It's a known bug (https://github.com/sphinx-doc/sphinx/issues/9884)
  that autosummary will generate warning for inherited instance attributes.
  Those warnings will fail our build.
  As a temporary workaround, we don't autosummary classes with inherited instance attributes.
#}
{{ fullname | escape | underline}}

.. currentmodule:: {{ module }}

.. autoclass:: {{ objname }}
    :members:
