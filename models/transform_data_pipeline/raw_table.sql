with source_raw_data as (
    select * from {{ source('complete_data_source', 'raw_table' )}}
),

final as (
    select * from source_raw_data
)

select * from final
