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

sf_datasource as (
  select * from {{source('tech_mart', 'datasource_attributes__staging_statement__field_detailed')}}
),

worked as (
    select 
      -- ids
      header.transformation_id as transformation_id,

      -- source details
      header.source_type       as source_type,
      header.source_name       as original_source_name,

      -- computed fields
      regexp_replace( original_source_name, ' {2,}', ';') as work_source_name,
      split_part(work_source_name, ';', 2) as source_system,
      split_part(work_source_name, ';', 1) as source_name,

    from header
)

select * from worked