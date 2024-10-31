with source as (
    select * exclude (a) from {{source('flat_file_seed', 'rstranrule_pw1_2lis')}}
),

renamed as (
    select
      -- ids
      tranid  as transformation_id,
      objvers as object_version,
      ruleid  as rule_id,

      -- strings
      lower(grouptype)   as group_type,
      lower(ruletype)    as rule_type,
      lower(field_usage) as field_usage,
      lower(aggr)        as aggregation_type,
      
      -- numerics
      seqnr    as sequence_number,
      groupid  as group_id,
      ref_rule as reference_rule,
      
      -- booleans
      {{ sap_to_boolean('no_conv') }} as no_conversion_flag,
      {{ sap_to_boolean('amdp') }}    as amdp_flag,
      {{ sap_to_boolean('abap') }}    as abap_flag,

    from source
    where object_version = 'A'
)

select * from renamed