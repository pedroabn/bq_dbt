{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

/*
  metrics
  ─────────────────────────────────────────────────────────────────
  Agrega os dados de criativo para o nível:
    date × campaign × ad_group × device × region
  
  Também calcula métricas derivadas que serão reutilizadas
  nos modelos de mart downstream.
*/

with base as (

    select * from {{ ref('stg_meta_ads') }}

),

aggregated as (

    select
        -- Chaves de agrupamento
        date,
        channel,
        platform,
        campaign_id,
        campaign_name,
        objective,
        ad_group,
        device,
        region,
        currency,

        -- Métricas brutas agregadas
        sum(budget_daily)               as budget_daily,
        sum(impressions)                as impressions,
        sum(clicks)                     as clicks,
        sum(conversions)                as conversions,
        sum(revenue)                    as revenue,

        -- Contagem de criativos distintos no grupo
        count(distinct creative_id)     as n_creatives

    from base

    group by 1,2,3,4,5,6,7,8,9,10

),

with_kpis as (

    select
        *,

        -- CTR  (Click-Through Rate)
        safe_divide(clicks, impressions)                as ctr,

        -- CVR  (Conversion Rate sobre cliques)
        safe_divide(conversions, clicks)                as cvr,

        -- CPC  (Custo por Clique)  —  usa budget como proxy de gasto
        safe_divide(budget_daily, clicks)               as cpc,

        -- CPM  (Custo por Mil Impressões)
        safe_divide(budget_daily, impressions) * 1000   as cpm,

        -- CPA  (Custo por Aquisição)
        safe_divide(budget_daily, conversions)          as cpa,

        -- ROAS (Return on Ad Spend)
        safe_divide(revenue, budget_daily)              as roas

    from aggregated

)

select * from with_kpis