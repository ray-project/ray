{{ name | escape | underline}}

.. currentmodule:: {{ module }}

.. autoclass:: {{ objname }}

{% block methods %}
{% if methods %}
{% for api_group in methods | get_api_groups(name, module) %}

.. rubric:: {{ api_group | capitalize }}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   {% for method in methods | select_api_group(name, module, api_group) %}
      {{ name }}.{{ method }}
   {% endfor %}

{% endfor %}
{% endif %}
{% endblock %}
