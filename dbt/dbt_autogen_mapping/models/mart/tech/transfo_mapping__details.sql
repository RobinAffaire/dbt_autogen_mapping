with mapping_fields as (
    select * from {{ref('int_transfo_mapping__get_mapping_fields')}}
),

mapping_rules as (
    select * from {{ ref('int_transfo_mapping__get_mapping_rules')}}
),

target_dso as (
    select * from {{ref('int_transfo_mapping__get_target_struct__dso')}}
),

conso as (
    select 
      target_dso.transformation_id,
      target_dso.target_type,
      target_dso.dso_name,
      target_dso.dso_field_number,
      target_dso.field_is_key,
      target_dso.field_name,

      mapping_fields.rule_id as transformation_rule_id,
      mapping_fields.source_field_name,
      mapping_fields.source_is_key,
      mapping_fields.target_is_key,

      mapping_rules.rule_type,
      mapping_rules.constant_detail__value,
      mapping_rules.constant_detail__type,

    from target_dso
    left outer join mapping_fields 
      on target_dso.transformation_id = mapping_fields.transformation_id
      and target_dso.field_name = mapping_fields.target_field_name
    left outer join mapping_rules
      on mapping_fields.transformation_id = mapping_rules.transformation_id
      and mapping_fields.rule_id = mapping_rules.rule_id
)

select * from conso