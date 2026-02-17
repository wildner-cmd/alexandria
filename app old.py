import pandas as pd
import streamlit as st
import pydeck as pdk
import requests

st.set_page_config(page_title="Alexandria | ANEEL API (Brasil)", layout="wide")

st.title("Alexandria | Prospecção Grupo A — Brasil (API ANEEL)")
st.caption("Fonte: Portal de Dados Abertos ANEEL (CKAN DataStore). Consulta em tempo real via SQL + filtros + export.")

CKAN_SQL_URL = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search_sql"

# Resource IDs (BDGD)
RESOURCE_UCAT = "4318d38a-0bcd-421d-afb1-fb88b0c92a87"  # UCAT_PJ.csv
RESOURCE_UCMT = "f6671cba-f269-42ef-8eb3-62cb3bfa0b98"  # UCMT_PJ.csv

def ckan_sql(sql: str) -> pd.DataFrame:
    r = requests.get(CKAN_SQL_URL, params={"sql": sql}, timeout=90)
    r.raise_for_status()
    data = r.json()
    if not data.get("success"):
        raise RuntimeError(str(data))
    return pd.DataFrame(data["result"]["records"])

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    ren = {
        "cod_id_encr": "COD_ID_ENCR",
        "cnae": "CNAE",
        "dem_cont": "Demanda_kW",
        "lgrd": "LGRD",
        "brr": "BRR",
        "cep": "CEP",
        "point_y": "Latitude",
        "point_x": "Longitude",
        "gru_ten": "GRU_TEN",
        "ten_forn": "TEN_FORN",
        "gru_tar": "GRU_TAR",
        "mun": "MUN",
        "dist": "DIST"
    }
    for k, v in ren.items():
        if k in df.columns and v not in df.columns:
            df = df.rename(columns={k: v})

    for c in ["Demanda_kW", "Latitude", "Longitude"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["Latitude", "Longitude"])
    return df

def add_derived(df: pd.DataFrame) -> pd.DataFrame:
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

    if "Potencial" not in df.columns:
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

    if "Setor" not in df.columns:
        df["Setor"] = df.get("CNAE", pd.Series([None] * len(df))).apply(setor)

    if "Name" not in df.columns:
        df["Name"] = df.apply(lambda r: f"{r.get('Potencial','')} | {r.get('Setor','')} | {r.get('Demanda_kW','')} kW", axis=1)
    if "Description" not in df.columns:
        df["Description"] = df.apply(lambda r: f"CNAE: {r.get('CNAE','')} | Endereço: {r.get('LGRD','')} | Bairro: {r.get('BRR','')} | CEP: {r.get('CEP','')}", axis=1)

    return df

@st.cache_data(show_spinner=False, ttl=3600)
def fetch_dataset(resource_id: str, min_kw: float, limit: int, offset: int, order_desc: bool = True) -> pd.DataFrame:
    order = "DESC" if order_desc else "ASC"
    sql = f'''
    SELECT
      cod_id_encr, cnae, dem_cont, lgrd, brr, cep, point_y, point_x, mun, dist, gru_ten, ten_forn, gru_tar
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

def render_map(df: pd.DataFrame):
    if len(df) == 0:
        st.info("Nenhum ponto para os filtros atuais.")
        return

    lat0 = float(df["Latitude"].mean())
    lon0 = float(df["Longitude"].mean())

    tooltip = {"html": "<b>{Name}</b><br/>{Description}", "style": {"backgroundColor": "white", "color": "black"}}

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

    view_state = pdk.ViewState(latitude=lat0, longitude=lon0, zoom=4, pitch=0)
    st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip=tooltip), use_container_width=True)

def filter_local(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.subheader("Filtros locais (pós-consulta)")
    pot_all = ["AAA","AA","A","B","C","Indefinido"]
    sel_pot = st.sidebar.multiselect("Potencial", pot_all, default=["AAA","AA","A"])
    setores = sorted([s for s in df["Setor"].dropna().unique()])
    sel_set = st.sidebar.multiselect("Setor", setores, default=setores)

    q = st.sidebar.text_input("Busca (CNAE/endereço/bairro/CEP/MUN/DIST)", "")
    f = df.copy()
    f = f[f["Potencial"].isin(sel_pot)]
    f = f[f["Setor"].isin(sel_set)]

    if q.strip():
        t = q.strip().lower()
        for col in ["CNAE","LGRD","BRR","CEP","MUN","DIST"]:
            if col not in f.columns:
                f[col] = ""
        mask = (
            f["CNAE"].astype(str).str.lower().str.contains(t) |
            f["LGRD"].astype(str).str.lower().str.contains(t) |
            f["BRR"].astype(str).str.lower().str.contains(t) |
            f["CEP"].astype(str).str.lower().str.contains(t) |
            f["MUN"].astype(str).str.lower().str.contains(t) |
            f["DIST"].astype(str).str.lower().str.contains(t)
        )
        f = f[mask]
    return f

st.sidebar.header("Consulta na ANEEL (API)")
fonte = st.sidebar.radio("Fonte", ["UCAT (Alta tensão PJ)", "UCMT (Média tensão PJ)"], index=1)
resource_id = RESOURCE_UCAT if fonte.startswith("UCAT") else RESOURCE_UCMT

min_kw = st.sidebar.number_input("Demanda mínima (kW)", min_value=0.0, value=2000.0, step=100.0)

limit = st.sidebar.slider("Linhas por página (LIMIT)", min_value=200, max_value=20000, value=5000, step=200)
page = st.sidebar.number_input("Página (0 = primeira)", min_value=0, value=0, step=1)
offset = int(page) * int(limit)

order_desc = st.sidebar.toggle("Ordenar por maior demanda (DESC)", value=True)

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

st.write(f"**Fonte:** {meta.get('fonte')} | **Min kW:** {meta.get('min_kw')} | **LIMIT:** {meta.get('limit')} | **OFFSET:** {meta.get('offset')}")
st.write(f"Registros carregados: **{len(df):,}**")

df_f = filter_local(df)

c1, c2 = st.columns([1.2, 1])
with c1:
    st.subheader("Mapa")
    render_map(df_f)

with c2:
    st.subheader("Lista priorizada")
    show_cols = ["Potencial","Setor","Demanda_kW","CNAE","LGRD","BRR","CEP","MUN","DIST","GRU_TEN","TEN_FORN","GRU_TAR","COD_ID_ENCR"]
    show_cols = [c for c in show_cols if c in df_f.columns]
    st.dataframe(df_f[show_cols].sort_values(["Potencial","Demanda_kW"], ascending=[True, False]), use_container_width=True, height=560)

st.divider()
st.subheader("Exportar")
st.download_button(
    "Baixar CSV (filtros aplicados)",
    data=df_f.to_csv(index=False).encode("utf-8"),
    file_name="aneel_filtrado.csv",
    mime="text/csv"
)

st.caption("Dica: para Brasil inteiro, use paginação (Página) e ajuste LIMIT conforme necessidade. O portal pode impor limites por consulta.")
