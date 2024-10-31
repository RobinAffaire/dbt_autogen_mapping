with source as (
    select * from {{ref('int_transfo_mapping__get_source_struct')}}
),

target as (
    select * from {{ref('int_transfo_mapping__get_target_struct')}}
),

worked as (
    select distinct
      -- technical key
      source.transformation_id,

      -- semantic key
      source.ref_model_name,
      target.target_name,

      -- source fields
      source.source_system,
      source.source_name,
    
      -- Computed fields
      'with source as (
          select * from ' || ref__statement || '
), 

transformed as (
  select 
'                          as prefix,
'
    from source
)

select * from transformed' as suffix,
    from source
    left outer join target
      on source.transformation_id = target.transformation_id
)

select * from worked