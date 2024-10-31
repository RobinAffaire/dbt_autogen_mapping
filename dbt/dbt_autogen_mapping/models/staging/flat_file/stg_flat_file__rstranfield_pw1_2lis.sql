with source as (
    select * exclude (a) from {{source('flat_file_seed', 'rstranfield_pw1_2lis')}}
),

renamed as (
    select 
      -- ids
      tranid  as transformation_id,
      objvers as object_version,
      segid   as segment_id,
      ruleid  as rule_id,
      stepid  as step_id,
      case paramtype when 0 then 'source'
                     when 1 then 'target'
                     else 'err_unknown' end as parameter_type,
                     
      lower(paramnm) as parameter_name,

      -- strings
      lower(fieldnm)   as field_name,
      lower(fieldtype) as field_type,
      lower(aggr)      as aggregation_type,

      -- numerics
      ruleposit as rule_position,

      -- booleans
      {{ sap_to_boolean('keyflag') }} as key_flag,
      
    from source
    where object_version = 'A'
)

select * from renamed