{{ config(materialized='table') }}

with market_data as (
    select * from {{ ref('stg_market_data') }}
),

onchain_data as (
    select * from {{ ref('stg_onchain_data') }}
),

daily_onchain_agg as (
    -- Aggregate block-level data into daily metrics
    select
        block_date as metric_date,
        count(distinct block_number) as blocks_mined,
        sum(transaction_count) as total_transactions,
        avg(base_fee_gwei) as avg_base_fee_gwei
    from onchain_data
    group by 1
),

final_mart as (
    -- Join Market Data with On-chain Aggregations
    select
        m.market_date as report_date,
        m.eth_price_usd,
        m.eth_volume_usd,
        coalesce(o.blocks_mined, 0) as blocks_mined,
        coalesce(o.total_transactions, 0) as total_transactions,
        coalesce(o.avg_base_fee_gwei, 0) as avg_base_fee_gwei
    from market_data m
    left join daily_onchain_agg o
        on m.market_date = o.metric_date
)

select * from final_mart
order by report_date desc
