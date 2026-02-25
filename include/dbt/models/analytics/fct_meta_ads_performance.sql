{{
    config(
        materialized='table',
        schema='marts'
    )
}}

/*
  agg_meta_ads_campaign
  ─────────────────────────────────────────────────────────────────
  Visão resumida por campanha no período completo.
  Ideal para dashboards de performance de campanha.
*/

with daily as (

    select * from {{ ref('int_meta_ads_daily') }}

),

campaign_totals as (

    select
        campaign_id,
        campaign_name,
        objective,
        currency,

        -- Período ativo
        min(date)                               as first_date,
        max(date)                               as last_date,
        count(distinct date)                    as active_days,

        -- Totais
        sum(budget_daily)                       as total_spend,
        sum(impressions)                        as total_impressions,
        sum(clicks)                             as total_clicks,
        sum(conversions)                        as total_conversions,
        sum(revenue)                            as total_revenue,

        -- Médias ponderadas / KPIs globais
        safe_divide(sum(clicks), sum(impressions))                  as overall_ctr,
        safe_divide(sum(conversions), sum(clicks))                  as overall_cvr,
        safe_divide(sum(budget_daily), nullif(sum(clicks),0))       as overall_cpc,
        safe_divide(sum(budget_daily), sum(impressions)) * 1000     as overall_cpm,
        safe_divide(sum(budget_daily), nullif(sum(conversions),0))  as overall_cpa,
        safe_divide(sum(revenue), nullif(sum(budget_daily),0))      as overall_roas,

        -- Breakdown de dispositivo
        sum(case when device = 'mobile'  then impressions end)      as impressions_mobile,
        sum(case when device = 'desktop' then impressions end)      as impressions_desktop,

        -- Distribuição por objetivo já vem na dimensão, mas reforça top ad_group
        count(distinct ad_group)                                    as n_ad_groups,
        count(distinct region)                                      as n_regions

    from daily
    group by 1,2,3,4

)

select * from campaign_totals