with source_base_data as (
    select * from {{ source('complete_data_source', 'base_table' )}}
),

final as (
    select * from source_base_data
)

select * from final