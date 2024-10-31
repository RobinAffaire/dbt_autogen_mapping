with mapping_fields as (
    select * from {{ref('int_transfo_mapping__get_mapping_fields')}}
),

mapping_rules as (
    select * from {{ ref('int_transfo_mapping__get_mapping_rules')}}
),

target as (
    select * from {{ref('int_transfo_mapping__get_target_struct')}}
),

source as (
    select * from {{ref('int_transfo_mapping__get_source_struct')}}
),

conso as (
    select 
      source.source_system  as source_system,
      source.source_name    as source_name,
      source.field_number   as source_field_number,
      source.original_sap_field_name as source_original_field,
      source.final_field_name        as source_field,

      target.target_type         as target_type,
      target.target_name         as target_name,
      target.target_field_number as target_field_number,
      target.target_field_is_key as target_field_is_key,
      target.target_field        as target_field,

      mapping_fields.transformation_id as transformation_id,
      mapping_fields.rule_id           as transformation_rule_id,
      mapping_fields.source_field      as transformation_source_field,
      mapping_fields.source_is_key     as transformation_source_is_key,
      mapping_fields.target_field      as transformation_target_field,
      mapping_fields.target_is_key     as transformation_target_is_key,

      mapping_rules.rule_type          as transformation_rule_type,
      mapping_rules.constant_detail__value as transformation_rule__constant_value,
      mapping_rules.constant_detail__type  as transformation_rule__constant_type,

    from target
    left outer join mapping_fields 
      on  target.transformation_id = mapping_fields.transformation_id
      and target.target_field      = mapping_fields.target_field
    left outer join mapping_rules
      on  mapping_fields.transformation_id = mapping_rules.transformation_id
      and mapping_fields.rule_id           = mapping_rules.rule_id
    left outer join source
      on  mapping_fields.transformation_id = source.transformation_id
      and mapping_fields.source_field      = source.original_sap_field_name
      
    order by 
      target_type asc,
      target_name asc, 
      target_field_number asc
)

select * from conso