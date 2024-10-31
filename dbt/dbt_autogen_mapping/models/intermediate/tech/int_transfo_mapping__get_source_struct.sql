with header as (
    select * from {{ref('int_transfo_mapping__get_source_struct__prepare')}}
),

fields as (
    select 
      -- key
      lower(sap_source_system) as sap_source_system,
      lower(sap_datasource_name) as sap_datasource_name,
      field_number,

      -- fields
      original_sap_field_name,
      final_field_name,
      dbt__file_name,
    from {{source('tech_mart', 'datasource_attributes__staging_statement__field_detailed')}}
),

worked as (
    select
      -- Source fields from header
      header.transformation_id    as transformation_id,

      -- Source fields from field details
      fields.sap_source_system       as source_system,
      fields.sap_datasource_name     as source_name,
      fields.field_number            as field_number,
      fields.original_sap_field_name as original_sap_field_name,
      fields.final_field_name        as final_field_name,

      -- Computed fields
      split_part(split_part(dbt__file_name, '/', 2), '.', 1) as ref_model_name,
      {%raw%}
      '{{ref(\'' || ref_model_name || '\')}}' 
      {%endraw%} as ref__statement,

    from fields
    left outer join header 
      on fields.sap_source_system = header.source_system
      and fields.sap_datasource_name = header.source_name
    where transformation_id is not null
)

select * from worked