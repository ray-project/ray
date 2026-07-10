{# Short label: fullname is the fully-qualified path (e.g. ray.data.Dataset.map);
   split('.')[-1] keeps just the leaf ("map") so the API-sidebar label (and page H1)
   stay readable rather than repeating the full dotted path. -#}
{{ fullname.split('.')[-1] | escape | underline}}

.. currentmodule:: {{ module }}

.. autopydantic_model:: {{ fullname }}
    :inherited-members: BaseModel
    :exclude-members: Config
    :model-show-config-summary: False
    :model-show-validator-summary: False
    :model-show-field-summary: False
    :field-list-validators: False
    :model-show-json: True
    :model-summary-list-order: bysource
    :undoc-members: 