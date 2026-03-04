{{
    config(
        materialized='table',
        schema='marts'
    )
}}

with daily as (
    select * from {{ ref('int_meta_ads_daily') }}
),

-- Etapa 1: apenas agregação — sem window functions
weekly_agg as (
    select
        date_trunc(date, week(monday))              as week_start,
        campaign_id,
        campaign_name,
        objective,
        currency,

        sum(impressions)                            as total_impressions,
        sum(clicks)                                 as total_clicks,
        sum(conversions)                            as total_conversions,
        sum(revenue)                                as total_revenue,
        sum(budget_daily)                           as total_spend,

        safe_divide(sum(clicks), sum(impressions))  as ctr,
        safe_divide(sum(conversions), sum(clicks))  as cvr,
        safe_divide(sum(revenue), nullif(sum(budget_daily), 0)) as roas

    from daily
    group by 1, 2, 3, 4, 5
),

-- Etapa 2: window functions sobre a CTE já agregada (week_start já é coluna)
weekly_with_lag as (
    select
        *,
        lag(total_revenue) over (
            partition by campaign_id
            order by week_start
        )                                           as prev_week_revenue,

        lag(total_conversions) over (
            partition by campaign_id
            order by week_start
        )                                           as prev_week_conversions

    from weekly_agg
),

-- Etapa 3: métricas de crescimento WoW
with_growth as (
    select
        *,
        safe_divide(total_revenue - prev_week_revenue, nullif(prev_week_revenue, 0))
            as revenue_wow_growth,
        safe_divide(total_conversions - prev_week_conversions, nullif(prev_week_conversions, 0))
            as conversions_wow_growth
    from weekly_with_lag
)

select * from with_growth