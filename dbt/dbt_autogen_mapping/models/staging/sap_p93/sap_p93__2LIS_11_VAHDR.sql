with source as (
    select * from {{source('sap_p93', '2LIS_11_VAHDR')}}
)

select * from source