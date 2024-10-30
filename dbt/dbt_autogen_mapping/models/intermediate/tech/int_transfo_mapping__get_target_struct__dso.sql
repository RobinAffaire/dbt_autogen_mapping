with header as (
    select transformation_id,
           source_type,
           source_name,
           target_type,
           target_name,
           start_routine_id,
           end_routine_id,
           expert_routine_id,
           transformation_code_id
    from {{ref('stg_flat_file__rstran_pw1_2lis')}}
    where object_version = 'A'
),

dso as (
    select * from {{ref('stg_flat_file__rsdodsoiobj_pw1_2lis')}}
    where object_version = 'A'
),

worked as (
    select 
      -- ids
      header.transformation_id as transformation_id,

      -- transformation details
      header.target_type       as target_type,
      dso.dso_name             as dso_name,
      dso.field_number         as dso_field_number,
      dso.is_key               as field_is_key,
      dso.infoobject_name      as field_name,
      
    from header
    left outer join dso
      on  header.target_type = 'ODSO'
      and header.target_name = dso.dso_name
)

select * from worked