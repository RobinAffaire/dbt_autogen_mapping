with field as (
    select * from {{ref('stg_flat_file__rstranfield_pw1_2lis')}}
    where object_version = 'A'
      and segment_id = 1
      and step_id = 0
),

worked as (
    select 
      -- ids
      source.transformation_id as transformation_id,
      source.rule_id           as rule_id,

      -- Lookup fields
      source.field_name        as source_field_name,
      target.field_name        as target_field_name,
      source.key_flag          as source_is_key,
      target.key_flag          as target_is_key,

    from field as source
    left outer join field as target
      on  source.transformation_id = target.transformation_id
      and source.rule_id           = target.rule_id
    where source.parameter_type = 'source'
      and target.parameter_type = 'target'
      
     -- sort result
    order by transformation_id asc, rule_id asc
)

select * from worked