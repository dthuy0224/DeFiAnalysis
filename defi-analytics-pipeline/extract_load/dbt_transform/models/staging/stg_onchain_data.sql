{{ config(materialized='view') }}

with source as (
    select * from {{ source('defi_raw', 'raw_onchain_data') }}
),

renamed as (
    select
        -- We extract just the date from the timestamp for daily aggregation later
        cast(substr(cast(timestamp as string), 1, 10) as date) as block_date,
        
        cast(block_number as int64) as block_number,
        cast(transaction_count as int64) as transaction_count,
        cast(base_fee_per_gas_gwei as numeric) as base_fee_gwei
    from source
)

select * from renamed
