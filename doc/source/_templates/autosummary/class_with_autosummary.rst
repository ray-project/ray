{#
  Generating pages for class methods and attributes
  significantly increases the doc build time and causes timeouts.
  For now, opt in explicitly via `:template: autosummary/class_with_autosummary.rst`
#}
{#
  It's a known bug (https://github.com/sphinx-doc/sphinx/issues/9884)
  that autosummary will generate warning for inherited instance attributes.
  Those warnings will fail our build.
  For now, we don't autosummary classes with inherited instance attributes.
#}

{{ fullname | escape | underline}}

.. currentmodule:: {{ module }}

.. autoclass:: {{ objname }}
   :show-inheritance:

   {% block methods %}
   {% if methods %}
   .. rubric:: {{ _('Methods') }}

   .. autosummary::
      :toctree:

   {% for item in methods %}
      {{ item | filter_out_undoc_class_members(name, module) }}
   {%- endfor %}

   {% endif %}
   {% endblock %}


   {% block attributes %}
   {% if attributes %}
   .. rubric:: {{ _('Attributes') }}

   .. autosummary::
      :toctree:

   {% for item in attributes %}
      ~{{ name }}.{{ item }}
   {%- endfor %}

   {% endif %}
   {% endblock %}
