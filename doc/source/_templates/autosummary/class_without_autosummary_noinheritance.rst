{# Short label: fullname is the fully-qualified path (e.g. ray.data.Dataset.map);
   split('.')[-1] keeps just the leaf ("map") so the API-sidebar label (and page H1)
   stay readable rather than repeating the full dotted path. -#}
{{ fullname.split('.')[-1] | escape | underline}}

.. currentmodule:: {{ module }}

.. autoclass:: {{ objname }}
    :members:
