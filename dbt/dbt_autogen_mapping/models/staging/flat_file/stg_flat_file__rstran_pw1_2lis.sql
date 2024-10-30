with source as (
    select * exclude (a) from {{source('flat_file_seed', 'rstran_pw1_2lis')}}
),

renamed as (
    select
      -- ids
      tranid  as transformation_id,
      objvers as object_version,

      -- strings
      objstat as object_status,
      contrel,
      conttimestmp,
      owner,
      bwappl,
      activfl,
      tstpnm,
      timestmp,
      sourcetype as source_type,
      sourcesubtype,
      sourcename as source_name,
      targettype as target_type,
      targetsubtype,
      targetname as target_name,
      startroutine as start_routine_id,
      endroutine   as end_routine_id,
      expert       as expert_routine_id,
      glbcode,
      tranprog     as transformation_code_id,
      version_cur,
      target_tab_type,
      is_shadow,
      shadow_tranid,
      glbcode2,
      tlogo_owned_by,
      objnm_owned_by,
      currunit_allowed,
      grouping,
      all_fields,
      orgtranid,
      tunnel,
      structure_type,
      double_records,
      unit_check,
      haap_hint,
      txtsh,
      txtmd,
      txtlg,

    from source
    where object_version = 'A'
)

select * from renamed