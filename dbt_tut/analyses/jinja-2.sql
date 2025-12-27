{%- set apples = ["Gala", "Red Delicious", "Fuji", "Granny Smith"] -%}

{% for i in apples %}
  {% if i != "Gala" %}
    select '{{ i }}' as apple_name
    {% if not loop.last %}
      union all
    {% endif %}
  {% endif %}
{% endfor %}
