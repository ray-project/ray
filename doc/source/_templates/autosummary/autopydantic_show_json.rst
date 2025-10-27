{{ fullname | escape | underline}}

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