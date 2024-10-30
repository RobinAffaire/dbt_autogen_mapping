with source as (
    select * from {{source('tech_mart', 'datasource_attributes__staging_statement__field_detailed')}}
)

select * from source