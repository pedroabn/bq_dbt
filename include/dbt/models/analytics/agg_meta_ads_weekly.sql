{{
    config(
        materialized='table',
        schema='marts'
    )
}}

/*
  agg_meta_ads_weekly
  ─────────────────────────────────────────────────────────────────
  Tendências semanais por campanha.
  Facilita análise de evolução e sazonalidade.
*/

with daily as (

    select * from {{ ref('int_meta_ads_daily') }}

),

weekly as (

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
        safe_divide(sum(revenue), nullif(sum(budget_daily),0)) as roas,

        -- WoW (week-over-week) via window function
        lag(sum(revenue)) over (
            partition by campaign_id
            order by date_trunc(date, week(monday))
        )                                           as prev_week_revenue,

        lag(sum(conversions)) over (
            partition by campaign_id
            order by date_trunc(date, week(monday))
        )                                           as prev_week_conversions

    from daily
    group by 1,2,3,4,5

),

with_growth as (

    select
        *,
        safe_divide(total_revenue - prev_week_revenue, nullif(prev_week_revenue,0))
            as revenue_wow_growth,
        safe_divide(total_conversions - prev_week_conversions, nullif(prev_week_conversions,0))
            as conversions_wow_growth
    from weekly

)

select * from with_growth