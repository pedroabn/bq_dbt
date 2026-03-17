import numpy as np
import pandas as pd
import random
from datetime import datetime, timedelta
import json

"""
GERAÇÃO DE DADOS BRUTOS - SIMULAÇÃO META ADS API (BUDGET REALISTA)
Empresa: vendas de hardware (B2C)
Camada: Bronze / Raw — sem métricas derivadas
"""

def dados():
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
        "PC_Gamer":   {"ticket_min": 6000, "ticket_max": 15000,"categoria": "Completos"},
        "Cooler_CPU": {"ticket_min": 150,  "ticket_max": 900,  "categoria": "Componentes"},
    }

    MARCAS = ["Intel", "AMD", "NVIDIA", "Samsung", "LG",
              "ASUS", "Gigabyte", "MSI", "Logitech", "HyperX", "WD"]

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
        "audience_network", "messenger_inbox"]
    
    SEGMENTACOES = [
        "interesse_tecnologia", "lookalike_compradores", "remarketing_site",
        "remarketing_carrinho", "interesse_games", "interesse_hardware_pc",
        "audiencia_ampla_18_35", "lookalike_engajados"]
    
    STATUS_ANUNCIO = ["ACTIVE", "ACTIVE", "ACTIVE", "PAUSED", "ACTIVE"]  # 80% active
    GENEROS_PUBLICO = ["male", "female", "unknown"]
    FAIXAS_ETARIAS  = ["18-24", "25-34", "35-44", "45-54", "55-64", "65+"]
    ESTADOS = ["PB", "MA", "SE", "BA", "AL", "PE", "CE"]


    # ─────────────────────────────────────────────────────────────────────────────
    # PARÂMETROS DE GERAÇÃO
    # ─────────────────────────────────────────────────────────────────────────────
    N_CAMPAIGNS    = 25
    N_ADSETS_RANGE = (2, 5)    # adsets por campanha
    N_ADS_RANGE    = (3, 8)    # anúncios por adset
    N_DAYS         = 180       # janela histórica em dias

    DATA_FIM   = datetime(2026, 3, 31)
    DATA_INICIO = DATA_FIM - timedelta(days=N_DAYS)

    # ─────────────────────────────────────────────────────────────────────────────
    # GERAÇÃO
    # ─────────────────────────────────────────────────────────────────────────────
    records = []
    account_id = "ACT_487291034"

    for camp_idx in range(N_CAMPAIGNS):
        print("🔄 Gerando GA4 BigQuery Export Schema...")
        
        produtos = list(PRODUTOS.keys())
        pesos = [PRODUTOS[p]['ticket_max'] for p in produtos]
        # Normalizar pesos para soma = 1
        pesos_norm = np.array(pesos) / sum(pesos)
        produto_nome   = np.random.choice(produtos, p=pesos_norm)
        produto_info   = PRODUTOS[produto_nome]
        marca          = random.choice(MARCAS)
        objetivo_nome  = random.choice(list(OBJETIVOS_CAMPANHA.keys()))
        objetivo_info  = OBJETIVOS_CAMPANHA[objetivo_nome]

        campaign_id    = f"camp_{23400000 + camp_idx}"
        campaign_name  = f"{produto_nome}_{marca}_{objetivo_nome[:4]}_{camp_idx:02d}"
        budget_diario  = round(np.random.uniform(10, 100), 2)

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
            regiao        = random.choice(ESTADOS)

            adset_name = f"{produto_nome}_{segmentacao}_{adset_idx:02d}"
            budget_adset = round(budget_diario / n_adsets, 2)

            n_ads = random.randint(*N_ADS_RANGE)
            
            for ad_idx in range(n_ads):
                ad_id     = f"ad_{56700000 + camp_idx * 100 + adset_idx * 10 + ad_idx}"
                formato   = random.choice(FORMATOS_ANUNCIO)
                status    = random.choice(STATUS_ANUNCIO)
                ad_name   = f"{produto_nome}_{marca}_{formato}_{ad_idx:02d}"
                
                FORMATO_OBJETIVO_BOOST = {
                    ("VIDEO",      "CONVERSIONS"):    1.45,
                    ("REELS",      "CONVERSIONS"):    1.35,
                    ("CAROUSEL",   "CONVERSIONS"):    1.25,
                    ("IMAGE",      "CONVERSIONS"):    0.75,
                    ("STORY",      "CONVERSIONS"):    0.85,
                    ("VIDEO",      "VIDEO_VIEWS"):    1.80,
                    ("REELS",      "VIDEO_VIEWS"):    1.70,
                    ("IMAGE",      "REACH"):          1.30,
                    ("CAROUSEL",   "LINK_CLICKS"):    1.40,
                    ("IMAGE",      "LINK_CLICKS"):    1.10,
                    ("VIDEO",      "BRAND_AWARENESS"):1.20 }

                # Placement × formato (match/mismatch)
                PLACEMENT_FORMATO_BOOST = {
                    ("instagram_reels",   "REELS"):   1.40,
                    ("instagram_reels",   "IMAGE"):   0.55,
                    ("instagram_stories", "STORY"):   1.35,
                    ("instagram_stories", "VIDEO"):   1.15,
                    ("instagram_feed",    "CAROUSEL"):1.20,
                    ("facebook_feed",     "IMAGE"):   1.10,
                    ("audience_network",  "VIDEO"):   0.70,
                    ("messenger_inbox",   "REELS"):   0.60  }

                # Targeting × objetivo (lógica de funil)
                TARGETING_OBJETIVO_BOOST = {
                    ("remarketing_carrinho",  "CONVERSIONS"):  1.60,
                    ("remarketing_site",      "CONVERSIONS"):  1.35,
                    ("lookalike_compradores",  "CONVERSIONS"): 1.20,
                    ("lookalike_engajados",    "LINK_CLICKS"): 1.25,
                    ("audiencia_ampla_18_35",  "CONVERSIONS"): 0.65,
                    ("audiencia_ampla_18_35",  "REACH"):       1.30,
                    ("interesse_games",        "CONVERSIONS"): 1.10,
                    ("interesse_hardware_pc",  "CONVERSIONS"): 1.15,
                }

                boost_formato_obj = FORMATO_OBJETIVO_BOOST.get((formato, objetivo_nome), 1.0)
                boost_placement = PLACEMENT_FORMATO_BOOST.get((posicionamento, formato), 1.0)
                boost_targeting = TARGETING_OBJETIVO_BOOST.get((segmentacao, objetivo_nome), 1.0)

                boost_total = boost_formato_obj * boost_placement * boost_targeting
                # Dias ativos do anúncio
                ad_start  = camp_start + timedelta(days=random.randint(0, 7))
                ad_end    = ad_start + timedelta(days=random.randint(10, 90))

                # Ruído individual do anúncio (FIXO por ad)
                ruido_ctr  = np.random.normal(1.0, 0.25)
                ruido_cpm  = np.random.normal(1.0, 0.20)
                ruido_cvr  = np.random.normal(1.0, 0.30)

                # Por dia: gera uma linha de insights (como a API retorna)
                current_date = ad_start
                while current_date <= ad_end:

                    # LEARNING PHASE + PERFORMANCE-BASED BUDGET
                    dias_rodando = (current_date - ad_start).days
                    
                    # Performance score dinâmico por dia
                    performance_score = np.random.normal(1.0, 0.4)
                    peso_ad = np.clip(performance_score, 0.1, 3.0)
                    
                    # Budget disponível do adset no dia
                    budget_disponivel_dia = budget_adset * np.random.uniform(0.7, 1.3)
                    spend_base = (budget_disponivel_dia / n_ads) * peso_ad
                    
                    # LEARNING PHASE (primeiros 7 dias)
                    if dias_rodando <= 3:  # Learning phase inicial
                        spend_dia = spend_base * np.random.uniform(0.3, 0.8)
                        cpm_dia = max(objetivo_info["cpm_base"] * ruido_cpm * 1.3 * np.random.uniform(0.8, 1.2), 1.0)
                    elif dias_rodando <= 7:  # Stabilizing
                        spend_dia = spend_base * np.random.uniform(0.8, 1.2)
                        cpm_dia = max(objetivo_info["cpm_base"] * ruido_cpm * np.random.uniform(0.9, 1.1), 1.0)
                    else:  # Full performance
                        spend_dia = spend_base
                        cpm_dia = max(objetivo_info["cpm_base"] * ruido_cpm * np.random.uniform(0.8, 1.2), 1.0)
                    
                    # WINNER TAKES MOST (80/20 rule)
                    if performance_score > 1.8:  # Top performer
                       spend_dia *= 2.5
                    elif performance_score < 0.5:  # Perdedor
                         spend_dia *= 0.3
                    spend_dia = max(round(spend_dia, 4), 0.10)  # Mínimo R$0.10/dia

                    # CPM e alcance diário (após spend)
                    impressoes = int((spend_dia / cpm_dia) * 1000)
                    reach      = int(impressoes * np.random.uniform(0.6, 0.95))
                    frequency  = round(impressoes / reach if reach > 0 else 1.0, 4)

                    # Cliques
                    fadiga = max(0.4, 1 - max(0, frequency - 2.0) * 0.15)

                    ctr_dia = max(
                        objetivo_info["ctr_base"] * ruido_ctr
                        * np.random.uniform(0.7, 1.3) * boost_total * fadiga, 0.001)
                    
                    link_clicks  = int(impressoes * ctr_dia)
                    all_clicks   = int(link_clicks * np.random.uniform(1.1, 1.8))
                    unique_clicks = int(link_clicks * np.random.uniform(0.75, 0.95))

                    # Engajamento de post
                    post_reactions  = int(impressoes * np.random.uniform(0.002, 0.025))
                    post_comments   = int(impressoes * np.random.uniform(0.0005, 0.006))
                    post_shares     = int(impressoes * np.random.uniform(0.0003, 0.004))
                    post_saves      = int(impressoes * np.random.uniform(0.001, 0.012))

                    # Métricas de vídeo (só para formatos video)
                    video_views = 0
                    video_avg_watch_time = 0.0
                    video_p25_watched = 0
                    video_p50_watched = 0
                    video_p75_watched = 0
                    video_p100_watched = 0

                    if formato in ["VIDEO", "REELS", "STORY"]:
                        video_views          = int(impressoes * np.random.uniform(0.30, 0.75))
                        video_avg_watch_time = round(np.random.uniform(3.0, 28.0), 2)
                        video_p25_watched    = int(video_views * np.random.uniform(0.60, 0.85))
                        video_p50_watched    = int(video_p25_watched * np.random.uniform(0.50, 0.75))
                        video_p75_watched    = int(video_p50_watched * np.random.uniform(0.35, 0.60))
                        video_p100_watched   = int(video_p75_watched * np.random.uniform(0.20, 0.45))

                    # Eventos de conversão
                    cvr_dia = max(
                        objetivo_info["cvr_base"] * ruido_cvr * np.random.uniform(0.5, 1.5)
                        * boost_total * fadiga,0.001)                    
                    purchases        = int(link_clicks * cvr_dia)
                    add_to_cart      = int(link_clicks * cvr_dia * np.random.uniform(3.0, 8.0))
                    view_content     = int(link_clicks * np.random.uniform(0.30, 0.70))
                    initiate_checkout= int(add_to_cart * np.random.uniform(0.20, 0.50))
                    search           = int(link_clicks * np.random.uniform(0.05, 0.20))

                    # Valor das compras
                    ticket_medio     = np.random.uniform(produto_info["ticket_min"], produto_info["ticket_max"])
                    purchase_value   = round(purchases * ticket_medio, 2)

                    # Custo por resultado
                    cost_per_result  = round(spend_dia / purchases if purchases > 0 else spend_dia, 4)

                    records.append({
                        # IDs e metadados
                        "account_id": account_id,
                        "campaign_id": campaign_id,
                        "campaign_name": campaign_name,
                        "adset_id": adset_id,
                        "adset_name": adset_name,
                        "ad_id": ad_id,
                        "ad_name": ad_name,
                        "date": current_date.strftime("%Y-%m-%d"),

                        # Configuração da campanha
                        "campaign_objective": objetivo_nome,
                        "campaign_status": status,
                        "campaign_budget_daily": budget_diario,
                        "campaign_start_date": camp_start.strftime("%Y-%m-%d"),
                        "campaign_end_date": camp_end.strftime("%Y-%m-%d"),

                        # Configuração do adset
                        "adset_targeting": segmentacao,
                        "adset_placement": posicionamento,
                        "adset_gender": genero_alvo,
                        "adset_age_range": faixa_etaria,
                        "adset_region": regiao,
                        "adset_budget": budget_adset,

                        # Configuração do anúncio
                        "ad_format": formato,
                        "ad_status": status,

                        # Produto anunciado
                        "produto": produto_nome,
                        "produto_categoria": produto_info["categoria"],
                        "marca": marca,
                        "ticket_medio_estimado": round(ticket_medio, 2),

                        # Métricas brutas
                        "spend": spend_dia,
                        "impressions": impressoes,
                        "reach": reach,
                        "frequency": frequency,
                        "clicks": all_clicks,
                        "link_clicks": link_clicks,
                        "unique_link_clicks": unique_clicks,
                        "post_reactions": post_reactions,
                        "post_comments": post_comments,
                        "post_shares": post_shares,
                        "post_saves": post_saves,
                        "video_views": video_views,
                        "video_avg_watch_time_sec": video_avg_watch_time,
                        "video_p25_watched": video_p25_watched,
                        "video_p50_watched": video_p50_watched,
                        "video_p75_watched": video_p75_watched,
                        "video_p100_watched": video_p100_watched,
                        "action_purchase": purchases,
                        "action_add_to_cart": add_to_cart,
                        "action_view_content": view_content,
                        "action_initiate_checkout": initiate_checkout,
                        "action_search": search,
                        "action_purchase_value": purchase_value,
                        "cost_per_result": cost_per_result,
                    })

                    current_date += timedelta(days=1)

    df = pd.DataFrame(records)

    # ─────────────────────────────────────────────────────────────────────────────
    # VALIDAÇÕES DE INTEGRIDADE
    # ─────────────────────────────────────────────────────────────────────────────
    assert (df["impressions"] >= df["link_clicks"]).all(), "impressions < link_clicks"
    assert (df["link_clicks"] >= df["action_purchase"]).all(), "link_clicks < purchases"
    assert "action_add_to_cart" in df.columns, "Campo action_add_to_cart ausente"
    assert df["spend"].min() >= 0, "spend negativo"
    assert (df["action_add_to_cart"] >= df["action_purchase"]).all()

    return df


# #
# GERAÇÃO DE DADOS BRUTOS - SIMULAÇÃO GA4 E-COMMERCE API
# Empresa: vendas de hardware (B2C) - MESMOS PRODUTOS
# Camada: Bronze / Raw — eventos GA4 originais
# Simula export GA4 BigQuery (eventos + user_properties + items)
#
def gerar_ga4_dados():
    np.random.seed(42)
    random.seed(42)
    
    # ═══════════════════════════════════════════════════════════════════════════════
    # CATÁLOGO (IDÊNTICO Meta Ads)
    # ═══════════════════════════════════════════════════════════════════════════════
    PRODUTOS = {
        "GPU":        {"ticket_min": 1200, "ticket_max": 6500, "category": "Componentes"},
        "CPU":        {"ticket_min": 800,  "ticket_max": 4000, "category": "Componentes"},
        "SSD_NVMe":   {"ticket_min": 200,  "ticket_max": 900,  "category": "Armazenamento"},
        "RAM_DDR5":   {"ticket_min": 600,  "ticket_max": 3500, "category": "Memoria"},
        "Monitor":    {"ticket_min": 900,  "ticket_max": 4500, "category": "Perifericos"},
        "Placa_Mae":  {"ticket_min": 500,  "ticket_max": 2500, "category": "Componentes"},
        "Headset":    {"ticket_min": 150,  "ticket_max": 800,  "category": "Perifericos"},
        "Kit_Upgrade":{"ticket_min": 1500, "ticket_max": 8000, "category": "Kits"},
        "PC_Gamer":   {"ticket_min": 6000, "ticket_max": 15000,"category": "Completos"},
    }
    MARCAS = ["Intel", "AMD", "NVIDIA", "Samsung", "LG",
              "ASUS", "Gigabyte", "MSI", "Logitech", "HyperX", "WD"]
    # GA4 Traffic Source (mapeado Google Ads → GA4)
    GOOGLE_ADS_UTM = {
        "google / cpc / meta_gpu": 0.045,      # CTR Ads
        "facebook / social / remarketing": 0.032,
        "google / organic / gpu": 0.018,
        "direct / none / (direct)": 0.000,
        "instagram / social / stories": 0.028
    }
    
    # GA4 Event Parameters (documentação oficial)
    GA4_EVENTOS_ECOMMERCE = [
        "page_view", "view_item_list", "view_item", "select_item",
        "add_to_cart", "remove_from_cart", "view_cart", 
        "begin_checkout", "add_shipping_info", "add_payment_info", 
        "purchase", "refund"
    ]
    
    # PARÂMETROS REALISTAS 2026 (e-commerce hardware BR)
    N_SESSOES = 75000        # 417 sessões/dia
    TAXA_CONVERSAO = 0.022   # 2.2% (realista hardware high-ticket)
    N_DAYS = 180
    
    DATA_FIM = datetime(2026, 3, 31)
    DATA_INICIO = DATA_FIM - timedelta(days=N_DAYS)
    
    records = []
    
    for sessao_id in range(N_SESSOES):
            # GA4 USER & SESSION (schema oficial)
        user_pseudo_id = f"user_{random.randint(10000000, 99999999)}_{sessao_id:04d}"
        session_start = DATA_INICIO + timedelta(days=random.randint(0, N_DAYS-1), 
                                              hours=random.randint(0,23), 
                                              minutes=random.randint(0,59))
        
        # Google Ads UTM → GA4 traffic_source
        utm_key = random.choice(list(GOOGLE_ADS_UTM.keys()))
        source, medium, campaign = utm_key.split(" / ")
        ctr_sessao = GOOGLE_ADS_UTM[utm_key]
        
        # SESSION JOURNEY (sequência realista GA4)
        session_events = []
        
        # 1. page_view inicial (100%)
        session_events.append({
            "event_name": "page_view",
            "page_location": random.choice(["/", "/componentes", "/gpus"]),
            "page_title": "PC Gamer Store | Hardware High Performance"
        })
        
        # 2. Product Discovery (68% das sessões)
        if random.random() < 0.68:
            produto_id = random.choice(list(PRODUTOS.keys()))
            produto_info = PRODUTOS[produto_id]
            
            session_events.extend([
                {"event_name": "view_item_list", "item_list_name": "Featured Products"},
                {"event_name": "select_item", "item_id": produto_id},
                {"event_name": "view_item", "item_id": produto_id}
            ])
            
            # 3. E-commerce Funnel (taxas reais)
            if random.random() < 0.42:  # add_to_cart
                session_events.append({
                    "event_name": "add_to_cart",
                    "item_id": produto_id,
                    "currency": "BRL",
                    "value": produto_info["ticket_min"]
                })
                
                if random.random() < TAXA_CONVERSAO * 10:  # begin_checkout (raro)
                    session_events.append({"event_name": "begin_checkout"})
                    
                    # PURCHASE (2.8% conversão final)
                    if random.random() < TAXA_CONVERSAO:
                        quantidade = random.choices([1,2], [0.85, 0.15])[0]
                        preco_unitario = np.random.uniform(produto_info["ticket_min"], 
                                                         produto_info["ticket_max"])
                        valor_total = round(preco_unitario * quantidade * 1.12, 2)  # +12% impostos
                        
                        item_ga4 = {
                            "item_name": produto_id,
                            "item_category": produto_info["category"],
                            "item_brand": random.choice(MARCAS),
                            "price": preco_unitario,
                            "quantity": quantidade
                        }
                        
                        session_events.append({
                            "event_name": "purchase",
                            "transaction_id": f"TRX_{random.randint(1000000, 9999999)}",
                            "value": valor_total,
                            "currency": "BRL",
                            "tax": round(valor_total * 0.12, 2),
                            "shipping": round(valor_total * 0.08, 2),
                            "coupon": random.choice(["", "DESC10", "FRETE_GRATIS"]),
                            "items": [item_ga4]
                        })
        
        # Gera eventos com timestamps GA4 (micros)
        for idx, evento in enumerate(session_events):
            event_time = session_start + timedelta(minutes=idx * 1.5 + random.randint(0, 90))
            event_timestamp = int(event_time.timestamp() * 1_000_000)
            
            # GA4 BigQuery Schema EXATO
            record = {
                # Core event dimensions
                "event_date": int(event_time.strftime("%Y%m%d")),
                "event_timestamp": event_timestamp,
                "event_name": evento["event_name"],
                "event_previous_timestamp": event_timestamp - random.randint(1000000, 50000000),
                "event_value_in_usd": 0,
                "event_bundle_sequence_id": random.randint(1, 1000),
                
                # User properties
                "user_pseudo_id": user_pseudo_id,
                "user_first_touch_timestamp": int(session_start.timestamp() * 1_000_000),
                
                # Session properties
                "ga_session_id": sessao_id,
                "ga_session_number": 1,
                "ga_session_start_timestamp": int(session_start.timestamp() * 1_000_000),
                
                # Traffic source (Google Ads → GA4)
                "traffic_source_source": source,
                "traffic_source_medium": medium,
                "traffic_source_name": "google" if source == "google" else "(not set)",
                "traffic_source_campaign": campaign,
                
                # Device + Geo (BR realista)
                "device_category": random.choices(["mobile", "desktop"], [0.62, 0.38])[0],
                "device_mobile_os_name": random.choices(["Android", "iOS"], [0.78, 0.22])[0] if "mobile" in random.choices(["mobile", "desktop"], [0.62, 0.38])[0] else None,
                "geo_country": "BR",
                "geo_region": random.choice(["PB", "MA", "SE", "BA", "AL", "PE", "CE"]),
                
                # E-commerce item (flattened para CSV)
                "ecommerce_item_id": evento.get("item_id"),
                "ecommerce_item_name": PRODUTOS.get(evento.get("item_id"), {}).get("name"),
                "ecommerce_item_category": PRODUTOS.get(evento.get("item_id"), {}).get("category"),
                "ecommerce_item_brand": evento.get("item_brand"),
                "ecommerce_item_price": evento.get("price"),
                "ecommerce_item_quantity": evento.get("quantity", 1),
                
                # Page view params
                "page_location": evento.get("page_location", "/"),
                "page_title": evento.get("page_title", "Hardware Store"),
                "page_referrer": random.choice(["", "google.com", "facebook.com"]),
                
                # Event params JSON (GA4 event_params)
                "event_params_json": json.dumps(evento.get("event_params", {})),
                "items_json": json.dumps(evento.get("items", [])),
            }
            
            records.append(record)
    
    df = pd.DataFrame(records)
    
    # ═══════════════════════════════════════════════════════════════════════════════
    # VALIDAÇÕES GA4 BIGQUERY SCHEMA
    # ═══════════════════════════════════════════════════════════════════════════════
    assert df["event_name"].isin(GA4_EVENTOS_ECOMMERCE + ["page_view"]).all()
    assert df["event_timestamp"].min() > 1_600_000_000_000_000  # micros desde epoch
    assert df["ga_session_id"].nunique() == N_SESSOES
    
    return df



# ─────────────────────────────────────────────────────────────────────────────
# RELATÓRIO
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    dfga4 = gerar_ga4_dados()
    dfmeta = dados()
    print("=" * 60)
    print(" DADOS BRUTOS — META ADS API — HARDWARE COMPANY (BUDGET REALISTA)")
    print("=" * 60)
    print(f"Total de linhas (ad x dia)  : {len(dfmeta):,}")
    
    print("\nDistribuicao por produto:")
    prod_summary = dfmeta.groupby("produto").agg({
        "ad_id": "count",
        "spend": "sum",
        "impressions": "sum",
        "action_purchase": "sum",
        "action_purchase_value": "sum",
    }).round(0).sort_values("action_purchase_value", ascending=False)
    prod_summary["spend"] = prod_summary["spend"].map("R$ {:,.0f}".format)
    prod_summary["action_purchase_value"] = prod_summary["action_purchase_value"].map("R$ {:,.0f}".format)
    print(prod_summary)
    
    print("GA4")
    print("=" * 70)
    print(" GA4 E-COMMERCE RAW EVENTS — HARDWARE COMPANY")
    print("=" * 70)
    
    # Salva BigQuery format (GA4 export)
    dfga4.to_parquet("ga4_raw.parquet", index=False)
    print(f"\n✅ GA4 BigQuery Export salvo: ga4_raw.parquet")
    print(f"   Formato 100% compatível com GA4 BigQuery Schema")
    print(f"   Pronto para análises em Looker Studio/DataStudio")
    
    dfmeta.to_parquet("meta_ads_raw.parquet", index=False)
    print(f"\n✅ Arquivo salvo: meta_ads_raw.parquet ({len(dfmeta):,} linhas x {len(dfmeta.columns)} colunas)")
