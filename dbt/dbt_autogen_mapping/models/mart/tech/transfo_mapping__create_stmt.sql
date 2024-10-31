with header as (
  select * from {{ref('int_transfo_mapping__build_statement__header')}}
),

body as (
  select * from {{ref('int_transfo_mapping__build_statement__body')}}
),

worked as (
  select distinct
    -- Origin key fields
    header.ref_model_name       as ref_model_name,
    header.target_name          as target_name,

    -- Computed fields
    header.prefix      
      || listagg( '    ' || body.source_field || ' as ' || body.target_field || ',
' ) 
          within group ( order by body.transformation_id, 
                                  body.source_field_number )
          over ( partition by body.transformation_id ) 
     || header.suffix as model_stmt,

  from header
  left outer join body
    on  header.transformation_id = body.transformation_id
)

select * from worked