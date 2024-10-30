with source as (
    select * exclude ("a") from {{source('flat_file_seed', 'rsdodsoiobj_pw1_2lis')}}
),

renamed as (
    select
      -- ids
      odsobject as dso_name,
      objvers   as object_version,
      odstable  as dso_table,
      posit     as field_number,

      -- strings
      iobjnm as infoobject_name,

      -- booleans
      {{ sap_to_boolean('keyflag')}} as is_key,
      
    from source
    where object_version = 'A'
)

select * from renamed