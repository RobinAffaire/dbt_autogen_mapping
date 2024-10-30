with rule as (
    select * from {{ref('stg_flat_file__rstranrule_pw1_2lis')}}
    where object_version = 'A'
),

constant_detail as (
    select * from {{ref('stg_flat_file__rstranstepcnst_pw1_2lis')}}
    where object_version = 'A'
      and step_id        = 1 -- only value '1' exists in SAP rule table
),

-- extend current view with other sap step tables for master data read etc.
-- it is not useful for sap 2lis l1 because no such rule exists

worked as (
    select 
      rule.transformation_id as transformation_id,
      rule.rule_id           as rule_id,

      -- rule fields
      rule.rule_type as rule_type,

      -- Lookup fields Rule detail
      constant_detail.value as constant_detail__value,
      constant_detail.type  as constant_detail__type,

    from rule
    left outer join constant_detail
      on  rule.transformation_id = constant_detail.transformation_id
      and rule.rule_id           = constant_detail.rule_id
)

select * from worked