{{ config(materialized='view') }}

with source as (
    select * from {{ source('defi_raw', 'raw_market_data') }}
),

renamed as (
    select
        -- Ensure pure date format
        cast(date as date) as market_date,
        -- Round prices to 4 decimal places for cleanliness
        round(cast(price as numeric), 4) as eth_price_usd,
        -- Cast volume to float/numeric
        cast(volume as numeric) as eth_volume_usd
    from source
)

select * from renamed
