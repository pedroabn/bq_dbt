{{
    config(
        materialized='table',
        schema='marts'
    )
}}

/*
  agg_meta_ads_region
  ─────────────────────────────────────────────────────────────────
  Performance agregada por estado (região) e campanha.
  Útil para análises geográficas e otimização de targeting.
*/

with daily as (

    select * from {{ ref('int_meta_ads_daily') }}

),

region_totals as (

    select
        region,
        campaign_id,
        campaign_name,
        objective,
        currency,

        sum(impressions)                                            as total_impressions,
        sum(clicks)                                                 as total_clicks,
        sum(conversions)                                            as total_conversions,
        sum(revenue)                                                as total_revenue,
        sum(budget_daily)                                           as total_spend,

        safe_divide(sum(clicks), sum(impressions))                  as ctr,
        safe_divide(sum(conversions), sum(clicks))                  as cvr,
        safe_divide(sum(budget_daily), nullif(sum(conversions),0))  as cpa,
        safe_divide(sum(revenue), nullif(sum(budget_daily),0))      as roas,

        -- Share de receita por região dentro da campanha
        safe_divide(
            sum(revenue),
            sum(sum(revenue)) over (partition by campaign_id)
        )                                                           as revenue_share_in_campaign

    from daily
    group by 1,2,3,4,5

)

select * from region_totals
