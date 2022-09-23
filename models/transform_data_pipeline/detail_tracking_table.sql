with source_detail_tracking_data as (
    select * from {{ source('complete_data_source', 'detail_tracking_table' )}}
),

final as (
    select * from source_detail_tracking_data
)

select * from final
