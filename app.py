import time
import pandas as pd
import streamlit as st
import pydeck as pdk
import requests
from urllib.parse import quote_plus

st.set_page_config(page_title="Alexandria | ANEEL API (Brasil) v4", layout="wide")

st.title("Alexandria | Prospecção Grupo A — Brasil (API ANEEL) v4")
st.caption("Versão resiliente: retries, mensagens claras de erro, heatmap/cluster, UF automática, links Google Maps, export.")

CKAN_SQL_URL = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search_sql"

RESOURCE_UCAT = "4318d38a-0bcd-421d-afb1-fb88b0c92a87"
RESOURCE_UCMT = "f6671cba-f269-42ef-8eb3-62cb3bfa0b98"

UF_BY_IBGE_UF_CODE = {
    11: "RO", 12: "AC", 13: "AM", 14: "RR", 15: "PA", 16: "AP", 17: "TO",
    21: "MA", 22: "PI", 23: "CE", 24: "RN", 25: "PB", 26: "PE", 27: "AL", 28: "SE", 29: "BA",
    31: "MG", 32: "ES", 33: "RJ", 35: "SP",
    41: "PR", 42: "SC", 43: "RS",
    50: "MS", 51: "MT", 52: "GO", 53: "DF"
}

def ckan_sql(sql: str, timeout: int = 90, retries: int = 4, backoff: float = 1.6) -> pd.DataFrame:
    \"\"\"Consulta CKAN com retries (429/5xx) e erro amigável.\"\"\"
    headers = {"User-Agent": "AlexandriaStreamlit/1.0 (+https://alexandria.energia)"}
    last_err = None

    for i in range(retries + 1):
        try:
            r = requests.get(CKAN_SQL_URL, params={"sql": sql}, timeout=timeout, headers=headers)

            if r.status_code in (429, 500, 502, 503, 504):
                last_err = (r.status_code, (r.text or "")[:300])
                time.sleep(min(8.0, backoff ** i))
                continue

            r.raise_for_status()
            data = r.json()
            if not data.get("success"):
                raise RuntimeError(str(data)[:500])

            return pd.DataFrame(data["result"]["records"])

        except Exception as e:
            last_err = ("EXC", str(e)[:500])
            time.sleep(min(8.0, backoff ** i))

    st.error(f"Falha ao consultar ANEEL/CKAN. Detalhe: {last_err}")
    return pd.DataFrame()

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    ren = {
        "cod_id_encr": "COD_ID_ENCR",
        "cnae": "CNAE",
        "dem_cont": "Demanda_kW",
        "lgrd": "LGRD",
        "brr": "BRR",
        "cep": "CEP",
        "point_y": "Latitude",
        "point_x": "Longitude",
        "mun": "MUN",
        "dist": "DIST",
    }
    for k, v in ren.items():
        if k in df.columns and v not in df.columns:
            df = df.rename(columns={k: v})

    for c in ["Demanda_kW", "Latitude", "Longitude", "MUN", "DIST"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["Latitude", "Longitude"])
    return df

def add_derived(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    if "MUN" in df.columns:
        uf_code = (df["MUN"].fillna(0).astype(int) // 100000).astype(int)
        df["UF"] = uf_code.map(UF_BY_IBGE_UF_CODE).fillna("??")
    else:
        df["UF"] = "??"

    def pot(d):
        try:
            d = float(d)
        except:
            return "Indefinido"
        if d >= 5000: return "AAA"
        if d >= 2000: return "AA"
        if d >= 500:  return "A"
        if d >= 100:  return "B"
        return "C"

    df["Potencial"] = df.get("Demanda_kW", pd.Series([None] * len(df))).apply(pot)

    def setor(cnae):
        if pd.isna(cnae): return "Desconhecido"
        s = str(cnae).strip()
        prefix = s[:2]
        mapa = {
            "10":"Alimentos","17":"Papel e celulose","19":"Combustíveis","20":"Químico",
            "21":"Farmacêutico","22":"Plástico","23":"Minerais não metálicos","24":"Metalurgia",
            "25":"Metal mecânico","26":"Eletrônico","27":"Equipamentos elétricos","28":"Máquinas",
            "29":"Automotivo","30":"Transporte","35":"Energia","46":"Atacado","47":"Varejo",
            "52":"Logística","68":"Imobiliário","84":"Administração pública"
        }
        return mapa.get(prefix, "Outros")

    df["Setor"] = df.get("CNAE", pd.Series([None] * len(df))).apply(setor)

    df["Name"] = df.apply(lambda r: f"{r.get('UF','')} | {r.get('Potencial','')} | {r.get('Setor','')} | {r.get('Demanda_kW','')} kW", axis=1)
    df["Description"] = df.apply(lambda r: f"UF: {r.get('UF','')} | CNAE: {r.get('CNAE','')} | Endereço: {r.get('LGRD','')} | Bairro: {r.get('BRR','')} | CEP: {r.get('CEP','')}", axis=1)

    def gmaps_link(lat, lon, addr):
        if pd.notna(lat) and pd.notna(lon):
            return f"https://www.google.com/maps?q={lat},{lon}"
        a = str(addr or "").strip()
        return f"https://www.google.com/maps/search/?api=1&query={quote_plus(a)}" if a else ""
    df["GoogleMaps"] = df.apply(lambda r: gmaps_link(r.get("Latitude"), r.get("Longitude"), r.get("LGRD")), axis=1)

    return df

@st.cache_data(show_spinner=False, ttl=1800)
def fetch_dataset(resource_id: str, min_kw: float, limit: int, offset: int, order_desc: bool = True) -> pd.DataFrame:
    order = "DESC" if order_desc else "ASC"
    sql = f'''
    SELECT
      cod_id_encr, cnae, dem_cont, lgrd, brr, cep, point_y, point_x, mun, dist
    FROM "{resource_id}"
    WHERE dem_cont IS NOT NULL
      AND dem_cont >= {min_kw}
      AND point_x IS NOT NULL AND point_y IS NOT NULL
    ORDER BY dem_cont {order}
    LIMIT {limit} OFFSET {offset}
    '''
    df = ckan_sql(sql)
    df = normalize_df(df)
    df = add_derived(df)
    return df

def local_filters(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    st.sidebar.subheader("Filtros locais (pós-consulta)")
    uf_list = sorted([u for u in df["UF"].dropna().unique()])
    sel_uf = st.sidebar.multiselect("UF", uf_list, default=[])

    pot_all = ["AAA","AA","A","B","C","Indefinido"]
    sel_pot = st.sidebar.multiselect("Potencial", pot_all, default=["AAA","AA","A"])

    setores = sorted([s for s in df["Setor"].dropna().unique()])
    sel_set = st.sidebar.multiselect("Setor", setores, default=setores)

    q = st.sidebar.text_input("Busca (CNAE/endereço/bairro/CEP)", "")

    f = df.copy()
    if sel_uf:
        f = f[f["UF"].isin(sel_uf)]
    f = f[f["Potencial"].isin(sel_pot)]
    f = f[f["Setor"].isin(sel_set)]

    if q.strip():
        t = q.strip().lower()
        mask = (
            f["CNAE"].astype(str).str.lower().str.contains(t) |
            f["LGRD"].astype(str).str.lower().str.contains(t) |
            f["BRR"].astype(str).str.lower().str.contains(t) |
            f["CEP"].astype(str).str.lower().str.contains(t)
        )
        f = f[mask]
    return f

def render_map(df: pd.DataFrame, mode: str):
    if df is None or df.empty:
        st.info("Nenhum ponto para os filtros atuais (ou a consulta falhou). Tente reduzir LIMIT e/ou aumentar min kW.")
        return

    lat0 = float(df["Latitude"].mean())
    lon0 = float(df["Longitude"].mean())

    tooltip = {
        "html": "<b>{Name}</b><br/>{Description}<br/><a href='{GoogleMaps}' target='_blank'>Abrir no Google Maps</a>",
        "style": {"backgroundColor": "white", "color": "black"}
    }

    if mode == "Pontos":
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=df,
            get_position='[Longitude, Latitude]',
            get_radius=1200,
            radius_min_pixels=2,
            radius_max_pixels=18,
            pickable=True,
            auto_highlight=True,
        )
        deck = pdk.Deck(layers=[layer], initial_view_state=pdk.ViewState(latitude=lat0, longitude=lon0, zoom=4), tooltip=tooltip)
        st.pydeck_chart(deck, use_container_width=True)
        return

    if mode == "Hexagon (heatmap)":
        layer = pdk.Layer(
            "HexagonLayer",
            data=df,
            get_position='[Longitude, Latitude]',
            radius=18000,
            elevation_scale=30,
            elevation_range=[0, 3000],
            pickable=True,
            extruded=True,
        )
        deck = pdk.Deck(layers=[layer], initial_view_state=pdk.ViewState(latitude=lat0, longitude=lon0, zoom=4, pitch=40))
        st.pydeck_chart(deck, use_container_width=True)
        return

    layer = pdk.Layer(
        "ClusterLayer",
        data=df,
        get_position='[Longitude, Latitude]',
        pickable=True,
        auto_highlight=True,
    )
    deck = pdk.Deck(layers=[layer], initial_view_state=pdk.ViewState(latitude=lat0, longitude=lon0, zoom=4), tooltip=tooltip)
    st.pydeck_chart(deck, use_container_width=True)

st.sidebar.header("Consulta na ANEEL (API)")
fonte = st.sidebar.radio("Fonte", ["UCAT (Alta tensão PJ)", "UCMT (Média tensão PJ)"], index=1)
resource_id = RESOURCE_UCAT if fonte.startswith("UCAT") else RESOURCE_UCMT

min_kw = st.sidebar.number_input("Demanda mínima (kW)", min_value=0.0, value=2000.0, step=100.0)
limit = st.sidebar.slider("Linhas por página (LIMIT)", min_value=200, max_value=20000, value=1500, step=200)
page = st.sidebar.number_input("Página (0 = primeira)", min_value=0, value=0, step=1)
offset = int(page) * int(limit)
order_desc = st.sidebar.toggle("Ordenar por maior demanda (DESC)", value=True)

st.sidebar.subheader("Mapa")
map_mode = st.sidebar.selectbox("Modo", ["Hexagon (heatmap)", "Pontos", "Cluster"], index=0)

btn = st.sidebar.button("Carregar / Atualizar", type="primary")

if "df_cache" not in st.session_state:
    st.session_state.df_cache = None
    st.session_state.meta = {}

if btn or st.session_state.df_cache is None:
    with st.spinner("Consultando ANEEL..."):
        df_api = fetch_dataset(resource_id=resource_id, min_kw=min_kw, limit=int(limit), offset=int(offset), order_desc=order_desc)
        st.session_state.df_cache = df_api
        st.session_state.meta = {"fonte": fonte, "min_kw": min_kw, "limit": int(limit), "page": int(page), "offset": int(offset)}

df = st.session_state.df_cache
meta = st.session_state.meta

df_f = local_filters(df)

st.write(f"**Fonte:** {meta.get('fonte')} | **Min kW:** {meta.get('min_kw')} | **LIMIT:** {meta.get('limit')} | **OFFSET:** {meta.get('offset')}")
st.write(f"Registros carregados: **{len(df) if df is not None else 0:,}** | Após filtros: **{len(df_f) if df_f is not None else 0:,}**")

c1, c2 = st.columns([1.2, 1])
with c1:
    st.subheader("Mapa")
    render_map(df_f, mode=map_mode)

with c2:
    st.subheader("Lista priorizada (com UF e link Google Maps)")
    if df_f is None or df_f.empty:
        st.info("Sem dados para listar. Tente: LIMIT=500, min kW=3000, ou mude a página.")
    else:
        show_cols = ["UF","Potencial","Setor","Demanda_kW","CNAE","LGRD","BRR","CEP","GoogleMaps","MUN","DIST","COD_ID_ENCR"]
        show_cols = [c for c in show_cols if c in df_f.columns]
        st.dataframe(df_f[show_cols].sort_values(["UF","Potencial","Demanda_kW"], ascending=[True, True, False]), use_container_width=True, height=560)

st.divider()
st.subheader("Exportar")
if df_f is not None and not df_f.empty:
    st.download_button(
        "Baixar CSV (filtros aplicados)",
        data=df_f.to_csv(index=False).encode("utf-8"),
        file_name="aneel_filtrado_v4.csv",
        mime="text/csv"
    )
else:
    st.caption("Nada para exportar ainda.")
