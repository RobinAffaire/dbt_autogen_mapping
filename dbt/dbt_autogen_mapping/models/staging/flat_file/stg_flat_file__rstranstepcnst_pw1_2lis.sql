with source as (
    select * exclude (a) from {{source('flat_file_seed', 'rstranstepcnst_pw1_2lis')}}
),

renamed as (
    select
      -- ids
      tranid  as transformation_id,
      objvers as object_version,
      ruleid  as rule_id,
      stepid  as step_id,

      -- strings
      value   as value,
      inttype as type,
      
      -- numerics
      length  as length,
      decimals as decimals,

    from source
    where object_version = 'A'
)

select * from renamed