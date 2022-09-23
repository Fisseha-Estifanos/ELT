with source_tracking_data as (
    select * from {{ source('complete_data_source', 'tracking_table' )}}
),

final as (
    select * from source_tracking_data
)

select * from final
