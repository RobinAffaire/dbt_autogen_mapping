{% macro sap_to_boolean(input_sap_value) -%}
    case {{ input_sap_value }}
      when 'X' then true
      else false
    end 

{%- endmacro %}