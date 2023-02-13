{#
  It's a known bug (https://github.com/sphinx-doc/sphinx/issues/9884)
  that autosummary will generate warning for inherited instance attributes.
  Those warnings will fail our build.
  As a temporary workaround, we don't autosummary classes with inherited instance attributes.
#}
{#
  It also seems that autosummary doesn't work with type alias
  so we don't autosummary those as well.
  See ray.tune.schedulers.ASHAScheduler as an example.
#}
{#
  We also don't autosummary some classes to reduce doc build time.
#}
{{ fullname | escape | underline}}

.. currentmodule:: {{ module }}

.. autoclass:: {{ objname }}
    :members:
