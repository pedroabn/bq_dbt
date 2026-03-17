"""
GERAÇÃO DE DADOS BRUTOS - SIMULAÇÃO META ADS API
Empresa: vendas de hardware (B2C)
Camada: Bronze / Raw — sem métricas derivadas
Os campos simulam a estrutura real de resposta da API do Meta
"""

import numpy as np
import pandas as pd
import random
from datetime import datetime, timedelta

np.random.seed(42)
random.seed(42)

# ─────────────────────────────────────────────────────────────────────────────
# CATÁLOGO DA EMPRESA DE HARDWARE
# ─────────────────────────────────────────────────────────────────────────────

PRODUTOS = {
    "GPU":        {"ticket_min": 1200, "ticket_max": 6500, "categoria": "Componentes"},
    "CPU":        {"ticket_min": 800,  "ticket_max": 4000, "categoria": "Componentes"},
    "SSD_NVMe":   {"ticket_min": 200,  "ticket_max": 900,  "categoria": "Armazenamento"},
    "RAM_DDR5":   {"ticket_min": 300,  "ticket_max": 1200, "categoria": "Memoria"},
    "Monitor":    {"ticket_min": 900,  "ticket_max": 4500, "categoria": "Perifericos"},
    "Placa_Mae":  {"ticket_min": 500,  "ticket_max": 2500, "categoria": "Componentes"},
    "Fonte_ATX":  {"ticket_min": 300,  "ticket_max": 1200, "categoria": "Componentes"},
    "Gabinete":   {"ticket_min": 250,  "ticket_max": 1500, "categoria": "Gabinetes"},
    "Headset":    {"ticket_min": 150,  "ticket_max": 800,  "categoria": "Perifericos"},
    "Teclado_Mec":{"ticket_min": 200,  "ticket_max": 1200, "categoria": "Perifericos"},
    "Mouse_Gamer":{"ticket_min": 100,  "ticket_max": 600,  "categoria": "Perifericos"},
    "Webcam":     {"ticket_min": 150,  "ticket_max": 700,  "categoria": "Perifericos"},
    "Kit_Upgrade":{"ticket_min": 1500, "ticket_max": 8000, "categoria": "Kits"},
    "PC_Gamer":   {"ticket_min": 4000, "ticket_max": 15000,"categoria": "Completos"},
    "Cooler_CPU": {"ticket_min": 150,  "ticket_max": 900,  "categoria": "Componentes"},
}

MARCAS = ["Intel", "AMD", "NVIDIA", "Samsung", "Kingston", "Corsair",
          "LG", "ASUS", "Gigabyte", "MSI", "Logitech", "HyperX", "WD", "Seagate"]

OBJETIVOS_CAMPANHA = {
    "CONVERSIONS":   {"cpm_base": 45.0,  "ctr_base": 0.018, "cvr_base": 0.028},
    "LINK_CLICKS":   {"cpm_base": 28.0,  "ctr_base": 0.025, "cvr_base": 0.012},
    "REACH":         {"cpm_base": 15.0,  "ctr_base": 0.008, "cvr_base": 0.004},
    "BRAND_AWARENESS":{"cpm_base": 12.0, "ctr_base": 0.006, "cvr_base": 0.002},
    "VIDEO_VIEWS":   {"cpm_base": 18.0,  "ctr_base": 0.010, "cvr_base": 0.005},
}

FORMATOS_ANUNCIO = ["IMAGE", "VIDEO", "CAROUSEL", "COLLECTION", "STORY", "REELS"]

POSICIONAMENTOS = [
    "facebook_feed", "instagram_feed", "instagram_stories",
    "instagram_reels", "facebook_reels", "facebook_stories",
    "audience_network", "messenger_inbox"
]

SEGMENTACOES = [
    "interesse_tecnologia", "lookalike_compradores", "remarketing_site",
    "remarketing_carrinho", "interesse_games", "interesse_hardware_pc",
    "audiencia_ampla_18_35", "lookalike_engajados"
]

STATUS_ANUNCIO = ["ACTIVE", "ACTIVE", "ACTIVE", "PAUSED", "ACTIVE"]  # 80% active

GENEROS_PUBLICO = ["male", "female", "unknown"]
FAIXAS_ETARIAS  = ["18-24", "25-34", "35-44", "45-54", "55-64", "65+"]
REGIOES_BR      = ["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "GO", "PE", "CE"]

# ─────────────────────────────────────────────────────────────────────────────
# PARÂMETROS DE GERAÇÃO
# ─────────────────────────────────────────────────────────────────────────────

N_CAMPAIGNS    = 25
N_ADSETS_RANGE = (2, 5)    # adsets por campanha
N_ADS_RANGE    = (3, 8)    # anúncios por adset
N_DAYS         = 90        # janela histórica em dias

DATA_FIM   = datetime(2025, 3, 31)
DATA_INICIO = DATA_FIM - timedelta(days=N_DAYS)

# ─────────────────────────────────────────────────────────────────────────────
# GERAÇÃO
# ─────────────────────────────────────────────────────────────────────────────

records = []
account_id = "ACT_487291034"

for camp_idx in range(N_CAMPAIGNS):

    produto_nome   = random.choice(list(PRODUTOS.keys()))
    produto_info   = PRODUTOS[produto_nome]
    marca          = random.choice(MARCAS)
    objetivo_nome  = random.choice(list(OBJETIVOS_CAMPANHA.keys()))
    objetivo_info  = OBJETIVOS_CAMPANHA[objetivo_nome]

    campaign_id    = f"camp_{23400000 + camp_idx}"
    campaign_name  = f"{produto_nome}_{marca}_{objetivo_nome[:4]}_{camp_idx:02d}"

    budget_diario  = round(np.random.uniform(50, 800), 2)

    camp_start = DATA_INICIO + timedelta(days=random.randint(0, 30))
    camp_end   = camp_start + timedelta(days=random.randint(14, N_DAYS))
    camp_end   = min(camp_end, DATA_FIM)

    n_adsets = random.randint(*N_ADSETS_RANGE)

    for adset_idx in range(n_adsets):

        adset_id      = f"adset_{78900000 + camp_idx * 10 + adset_idx}"
        segmentacao   = random.choice(SEGMENTACOES)
        posicionamento = random.choice(POSICIONAMENTOS)
        genero_alvo   = random.choice(GENEROS_PUBLICO)
        faixa_etaria  = random.choice(FAIXAS_ETARIAS)
        regiao        = random.choice(REGIOES_BR)

        adset_name = f"{produto_nome}_{segmentacao}_{adset_idx:02d}"
        budget_adset = round(budget_diario / n_adsets, 2)

        n_ads = random.randint(*N_ADS_RANGE)

        for ad_idx in range(n_ads):

            ad_id     = f"ad_{56700000 + camp_idx * 100 + adset_idx * 10 + ad_idx}"
            formato   = random.choice(FORMATOS_ANUNCIO)
            status    = random.choice(STATUS_ANUNCIO)
            ad_name   = f"{produto_nome}_{marca}_{formato}_{ad_idx:02d}"

            # Dias ativos do anúncio
            ad_start  = camp_start + timedelta(days=random.randint(0, 7))
            ad_end    = camp_end
            dias_ativos = max((ad_end - ad_start).days, 1)

            # Ruído individual do anúncio
            ruido_ctr  = np.random.normal(1.0, 0.25)
            ruido_cpm  = np.random.normal(1.0, 0.20)
            ruido_cvr  = np.random.normal(1.0, 0.30)

            # Por dia: gera uma linha de insights (como a API retorna)
            current_date = ad_start
            while current_date <= ad_end:

                # CPM e alcance diário
                cpm_dia    = max(objetivo_info["cpm_base"] * ruido_cpm * np.random.uniform(0.8, 1.2), 1.0)
                spend_dia  = round(budget_adset / n_ads * np.random.uniform(0.5, 1.3), 4)
                impressoes = int((spend_dia / cpm_dia) * 1000)
                reach      = int(impressoes * np.random.uniform(0.6, 0.95))
                frequency  = round(impressoes / reach if reach > 0 else 1.0, 4)

                # Cliques
                ctr_dia      = max(objetivo_info["ctr_base"] * ruido_ctr * np.random.uniform(0.7, 1.3), 0.001)
                link_clicks  = int(impressoes * ctr_dia)
                all_clicks   = int(link_clicks * np.random.uniform(1.1, 1.8))   # inclui curtidas/shares
                unique_clicks = int(link_clicks * np.random.uniform(0.75, 0.95))

                # Engajamento de post
                post_reactions  = int(impressoes * np.random.uniform(0.002, 0.025))
                post_comments   = int(impressoes * np.random.uniform(0.0005, 0.006))
                post_shares     = int(impressoes * np.random.uniform(0.0003, 0.004))
                post_saves      = int(impressoes * np.random.uniform(0.001, 0.012))

                # Métricas de vídeo (só para formatos video)
                video_views            = 0
                video_avg_watch_time   = 0.0
                video_p25_watched      = 0
                video_p50_watched      = 0
                video_p75_watched      = 0
                video_p100_watched     = 0

                if formato in ["VIDEO", "REELS", "STORY"]:
                    video_views          = int(impressoes * np.random.uniform(0.30, 0.75))
                    video_avg_watch_time = round(np.random.uniform(3.0, 28.0), 2)   # segundos
                    video_p25_watched    = int(video_views * np.random.uniform(0.60, 0.85))
                    video_p50_watched    = int(video_p25_watched * np.random.uniform(0.50, 0.75))
                    video_p75_watched    = int(video_p50_watched * np.random.uniform(0.35, 0.60))
                    video_p100_watched   = int(video_p75_watched * np.random.uniform(0.20, 0.45))

                # Eventos de conversão (actions — como vêm da API do Meta)
                cvr_dia          = max(objetivo_info["cvr_base"] * ruido_cvr * np.random.uniform(0.5, 1.5), 0.001)
                purchases        = int(link_clicks * cvr_dia)
                add_to_cart      = int(link_clicks * cvr_dia * np.random.uniform(3.0, 8.0))
                view_content     = int(link_clicks * np.random.uniform(0.30, 0.70))
                initiate_checkout= int(add_to_cart * np.random.uniform(0.20, 0.50))
                search           = int(link_clicks * np.random.uniform(0.05, 0.20))

                # Valor das compras
                ticket_medio     = np.random.uniform(produto_info["ticket_min"], produto_info["ticket_max"])
                purchase_value   = round(purchases * ticket_medio, 2)

                # Custo por resultado (campo bruto da API)
                cost_per_result  = round(spend_dia / purchases if purchases > 0 else spend_dia, 4)

                records.append({
                    # IDs e metadados
                    "account_id":              account_id,
                    "campaign_id":             campaign_id,
                    "campaign_name":           campaign_name,
                    "adset_id":                adset_id,
                    "adset_name":              adset_name,
                    "ad_id":                   ad_id,
                    "ad_name":                 ad_name,
                    "date":                    current_date.strftime("%Y-%m-%d"),

                    # Configuração da campanha
                    "campaign_objective":      objetivo_nome,
                    "campaign_status":         status,
                    "campaign_budget_daily":   budget_diario,
                    "campaign_start_date":     camp_start.strftime("%Y-%m-%d"),
                    "campaign_end_date":       camp_end.strftime("%Y-%m-%d"),

                    # Configuração do adset
                    "adset_targeting":         segmentacao,
                    "adset_placement":         posicionamento,
                    "adset_gender":            genero_alvo,
                    "adset_age_range":         faixa_etaria,
                    "adset_region":            regiao,
                    "adset_budget":            budget_adset,

                    # Configuração do anúncio
                    "ad_format":               formato,
                    "ad_status":               status,

                    # Produto anunciado
                    "produto":                 produto_nome,
                    "produto_categoria":       produto_info["categoria"],
                    "marca":                   marca,
                    "ticket_medio_estimado":   round(ticket_medio, 2),

                    # ── Métricas brutas (sem derivações) ──────────────────────
                    # Entrega
                    "spend":                   spend_dia,
                    "impressions":             impressoes,
                    "reach":                   reach,
                    "frequency":               frequency,

                    # Cliques brutos
                    "clicks":                  all_clicks,
                    "link_clicks":             link_clicks,
                    "unique_link_clicks":      unique_clicks,

                    # Engajamento orgânico
                    "post_reactions":          post_reactions,
                    "post_comments":           post_comments,
                    "post_shares":             post_shares,
                    "post_saves":              post_saves,

                    # Vídeo
                    "video_views":             video_views,
                    "video_avg_watch_time_sec":video_avg_watch_time,
                    "video_p25_watched":       video_p25_watched,
                    "video_p50_watched":       video_p50_watched,
                    "video_p75_watched":       video_p75_watched,
                    "video_p100_watched":      video_p100_watched,

                    # Ações de conversão (actions da API)
                    "action_purchase":         purchases,
                    "action_add_to_cart":      add_to_cart,
                    "action_view_content":     view_content,
                    "action_initiate_checkout":initiate_checkout,
                    "action_search":           search,

                    # Valor monetário bruto
                    "action_purchase_value":   purchase_value,
                    "cost_per_result":         cost_per_result,
                })

                current_date += timedelta(days=1)

df = pd.DataFrame(records)

# ─────────────────────────────────────────────────────────────────────────────
# VALIDAÇÕES DE INTEGRIDADE
# ─────────────────────────────────────────────────────────────────────────────

assert (df["impressions"] >= df["link_clicks"]).all(),        "impressions < link_clicks"
assert (df["link_clicks"] >= df["action_purchase"]).all(),    "link_clicks < purchases"
assert (df["add_to_cart"] if "add_to_cart" in df else True),  "campo ausente"
assert df["spend"].min() >= 0,                                "spend negativo"
assert (df["action_add_to_cart"] >= df["action_purchase"]).all(), "cart < purchase"

# ─────────────────────────────────────────────────────────────────────────────
# RELATÓRIO
# ─────────────────────────────────────────────────────────────────────────────

print("=" * 55)
print(" DADOS BRUTOS — META ADS API — HARDWARE COMPANY")
print("=" * 55)
print(f"Total de linhas (ad x dia)  : {len(df):,}")
print(f"Campanhas                   : {df['campaign_id'].nunique()}")
print(f"Adsets                      : {df['adset_id'].nunique()}")
print(f"Anuncios                    : {df['ad_id'].nunique()}")
print(f"Janela temporal             : {df['date'].min()} → {df['date'].max()}")
print(f"Spend total                 : R$ {df['spend'].sum():,.2f}")
print(f"Impressions total           : {df['impressions'].sum():,}")
print(f"Purchases total             : {df['action_purchase'].sum():,}")
print(f"Purchase value total        : R$ {df['action_purchase_value'].sum():,.2f}")
print()
print("Distribuicao por produto:")
prod_summary = df.groupby("produto").agg(
    linhas       = ("ad_id", "count"),
    spend        = ("spend", "sum"),
    impressions  = ("impressions", "sum"),
    purchases    = ("action_purchase", "sum"),
    receita      = ("action_purchase_value", "sum"),
).sort_values("receita", ascending=False)
prod_summary["spend"]       = prod_summary["spend"].map("R$ {:,.0f}".format)
prod_summary["receita"]     = prod_summary["receita"].map("R$ {:,.0f}".format)
print(prod_summary.to_string())
print()
print("Colunas do dataset:")
print(df.dtypes.to_string())

df.to_csv("meta_ads_raw.csv", index=False)
print(f"\nArquivo salvo: meta_ads_raw_hardware.csv ({len(df):,} linhas x {len(df.columns)} colunas)")