{{ name | escape | underline}}

.. currentmodule:: {{ module }}

.. autoclass:: {{ objname }}

{% block methods %}
{% if methods %}
{% set api_groups = methods | get_api_groups(name, module) %}
{% for api_group in api_groups %}

{% if api_groups | length > 1 %}
.. rubric:: {{ api_group | capitalize }}
{% endif %}

.. autosummary::
   :nosignatures:
   :toctree: doc

   {% for method in methods | select_api_group(name, module, api_group) %}
      {{ name }}.{{ method }}
   {% endfor %}

{% endfor %}
{% endif %}
{% endblock %}
