{{ fullname | escape | underline}}

.. currentmodule:: {{ module }}

.. autoclass:: {{ objname }}

   {% block methods %}
   {% if methods %}
   .. rubric:: {{ _('Methods') }}

   .. autosummary::
      :toctree:

   {% for item in methods %}
      {{ name }}.{{ item }}
   {%- endfor %}

   {% endif %}
   {% endblock %}

   {% block attributes %}
   {% if attributes %}
   .. rubric:: {{ _('Attributes') }}

   .. autosummary::
      :toctree:

   {% for item in attributes %}
   {#
     It's a known bug (https://github.com/sphinx-doc/sphinx/issues/9884)
     that autosummary will generate warning for inherited instance attribute.
     We remove them for now.
   #}
   {%- if item not in inherited_members %}
      {{ name }}.{{ item }}
   {%- endif -%}
   {%- endfor %}

   {% endif %}
   {% endblock %}
