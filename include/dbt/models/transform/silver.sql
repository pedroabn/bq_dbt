{{
    config(
        materialized='view',
        schema='staging'
    )
}}

/*
  silver
  ─────────────────────────────────────────────────────────────────
  Camada de staging do Meta Ads.
  Responsabilidades:
    - Cast de tipos
    - Renomear colunas para snake_case padronizado
    - Filtrar registros inválidos (impressões nulas / negativas)
    - Nenhuma lógica de negócio ainda
*/

with source as (

    select * from {{ source('meta', 'meta_ads_raw') }}

),

cleaned as (

    select
        -- Dimensões temporais
        parse_date('%Y-%m-%d', date)    as date,

        -- Identificadores
        cast(campaign_id as int64)      as campaign_id,
        trim(creative_id)               as creative_id,

        -- Dimensões descritivas
        lower(trim(channel))            as channel,
        lower(trim(platform))           as platform,
        trim(campaign)                  as campaign_name,
        lower(trim(objective))          as objective,
        trim(ad_group)                  as ad_group,
        lower(trim(device))             as device,
        upper(trim(region))             as region,
        lower(trim(status))             as status,
        upper(trim(currency))           as currency,

        -- Métricas financeiras
        cast(budget_daily as float64)   as budget_daily,
        cast(revenue as float64)        as revenue,

        -- Métricas de performance
        cast(impressions as int64)      as impressions,
        cast(clicks as int64)           as clicks,
        cast(conversions as int64)      as conversions

    from source

    where impressions > 0   -- remove linhas sem entrega

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

    from cleaned

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
