{{
    config(
        materialized='table',
        schema='silver'
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

)

select * from cleaned
