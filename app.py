import time
import pandas as pd
import streamlit as st
import requests

st.set_page_config(page_title="Alexandria — Prospecção Grupo A (ANEEL)", layout="wide")

st.title("Alexandria — Prospecção Grupo A (ANEEL)")
st.caption("Detecta automaticamente as colunas do dataset (demanda/lat/lon/mun). Se não achar, você escolhe.")

CKAN_SQL_URL = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search_sql"

# Resource IDs (se estes mudarem no futuro, basta ajustar aqui)
RESOURCE_UCAT = "4318d38a-0bcd-421d-afb1-fb88b0c92a87"
RESOURCE_UCMT = "f6671cba-f269-42ef-8eb3-62cb3bfa0b98"

UF_BY_IBGE_UF_CODE = {
    11:"RO",12:"AC",13:"AM",14:"RR",15:"PA",16:"AP",17:"TO",
    21:"MA",22:"PI",23:"CE",24:"RN",25:"PB",26:"PE",27:"AL",28:"SE",29:"BA",
    31:"MG",32:"ES",33:"RJ",35:"SP",
    41:"PR",42:"SC",43:"RS",
    50:"MS",51:"MT",52:"GO",53:"DF"
}

def ckan_sql(sql: str, timeout=60, retries=4, backoff=1.6) -> pd.DataFrame:
    """Consulta CKAN com retries e retorna DataFrame."""
    last = None
    headers = {"User-Agent": "AlexandriaStreamlit/1.0"}

    for i in range(retries):
        try:
            r = requests.get(CKAN_SQL_URL, params={"sql": sql}, timeout=timeout, headers=headers)

            if r.status_code in (429, 500, 502, 503, 504):
                last = (r.status_code, (r.text or "")[:250])
                time.sleep(min(8, backoff ** i))
                continue

            if r.status_code != 200:
                last = (r.status_code, (r.text or "")[:250])
                time.sleep(min(8, backoff ** i))
                continue

            data = r.json()
            if not data.get("success"):
                last = ("success=false", str(data)[:250])
                time.sleep(min(8, backoff ** i))
                continue

            return pd.DataFrame(data["result"]["records"])

        except Exception as e:
            last = ("EXC", str(e)[:250])
            time.sleep(min(8, backoff ** i))

    st.error(f"Falha ao consultar ANEEL/CKAN após retries. Detalhe: {last}")
    return pd.DataFrame()

@st.cache_data(ttl=3600)
def probe_columns(resource_id: str) -> list[str]:
    """Puxa 1 linha para descobrir colunas reais do dataset."""
    sql = f'SELECT * FROM "{resource_id}" LIMIT 1'
    df = ckan_sql(sql, timeout=60, retries=3)
    return list(df.columns) if df is not None and not df.empty else []

def pick_column(cols: list[str], candidates: list[str]) -> str | None:
    """Escolhe a primeira coluna existente em cols que bater com candidates."""
    cols_lower = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    return None

def normalize_numeric(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_numeric(df[col], errors="coerce")

def carregar(resource_id: str, demand_col: str, lat_col: str, lon_col: str, mun_col: str | None,
             min_kw: float, limit: int, page: int) -> pd.DataFrame:

    offset = int(page) * int(limit)

    # IMPORTANTE: usar >= (ASCII) e não “≥”
    sql = f'''
    SELECT *
    FROM "{resource_id}"
    WHERE "{demand_col}" IS NOT NULL
      AND "{demand_col}" >= {min_kw}
      AND "{lat_col}" IS NOT NULL
      AND "{lon_col}" IS NOT NULL
    ORDER BY "{demand_col}" DESC
    LIMIT {limit} OFFSET {offset}
    '''

    df = ckan_sql(sql, timeout=90, retries=4)
    if df is None or df.empty:
        return pd.DataFrame()

    # padroniza nomes
    df = df.rename(columns={
        demand_col: "Demanda_kW",
        lat_col: "Latitude",
        lon_col: "Longitude"
    })

    df["Demanda_kW"] = normalize_numeric(df, "Demanda_kW")
    df["Latitude"] = normalize_numeric(df, "Latitude")
    df["Longitude"] = normalize_numeric(df, "Longitude")

    df = df.dropna(subset=["Demanda_kW", "Latitude", "Longitude"])

    # UF se tiver MUN
    if mun_col and mun_col in df.columns:
        df[mun_col] = pd.to_numeric(df[mun_col], errors="coerce")
        uf_code = (df[mun_col].fillna(0).astype(int) // 100000).astype(int)
        df["UF"] = uf_code.map(UF_BY_IBGE_UF_CODE).fillna("??")
    else:
        df["UF"] = "??"

    # Potencial
    def potencial(x):
        if x >= 5000: return "AAA"
        if x >= 2000: return "AA"
        if x >= 500:  return "A"
        if x >= 100:  return "B"
        return "C"
    df["Potencial"] = df["Demanda_kW"].apply(potencial)

    # Link maps
    df["GoogleMaps"] = df.apply(lambda r: f"https://www.google.com/maps?q={r['Latitude']},{r['Longitude']}", axis=1)

    return df

# =========================
# UI
# =========================

st.sidebar.header("Parâmetros")

fonte = st.sidebar.selectbox("Fonte", ["UCMT (Média tensão PJ)", "UCAT (Alta tensão PJ)"])
resource_id = RESOURCE_UCMT if fonte.startswith("UCMT") else RESOURCE_UCAT

min_kw = st.sidebar.number_input("Demanda mínima (kW)", min_value=0.0, value=1000.0, step=100.0)
limit = st.sidebar.slider("LIMIT (linhas)", 100, 5000, 1000, step=100)
page = st.sidebar.number_input("Página", min_value=0, value=0, step=1)

cols = probe_columns(resource_id)

if not cols:
    st.error("Não consegui ler as colunas do dataset agora. Tente Reboot e/ou reduzir LIMIT depois.")
    st.stop()

# Candidatos comuns (variam entre datasets)
DEMAND_CANDS = ["dem_cont", "demanda", "dem_kw", "dem", "dem_med", "dem_max", "dem_contr", "dem_contratada", "demanda_kw"]
LAT_CANDS    = ["point_y", "lat", "latitude", "y"]
LON_CANDS    = ["point_x", "lon", "longitude", "x"]
MUN_CANDS    = ["mun", "cod_mun", "ibge_mun", "municipio", "cd_mun"]

auto_demand = pick_column(cols, DEMAND_CANDS)
auto_lat    = pick_column(cols, LAT_CANDS)
auto_lon    = pick_column(cols, LON_CANDS)
auto_mun    = pick_column(cols, MUN_CANDS)

st.sidebar.subheader("Mapeamento de colunas (auto + ajuste)")
demand_col = st.sidebar.selectbox("Coluna de Demanda (kW)", cols, index=cols.index(auto_demand) if auto_demand in cols else 0)
lat_col    = st.sidebar.selectbox("Coluna de Latitude", cols, index=cols.index(auto_lat) if auto_lat in cols else 0)
lon_col    = st.sidebar.selectbox("Coluna de Longitude", cols, index=cols.index(auto_lon) if auto_lon in cols else 0)
mun_col    = st.sidebar.selectbox("Coluna de Município (IBGE) (opcional)", ["(nenhuma)"] + cols,
                                 index=(["(nenhuma)"] + cols).index(auto_mun) if auto_mun in cols else 0)
mun_col = None if mun_col == "(nenhuma)" else mun_col

with st.spinner("Consultando ANEEL..."):
    df = carregar(resource_id, demand_col, lat_col, lon_col, mun_col, min_kw, int(limit), int(page))

st.write(f"**Fonte:** {fonte} | **min_kw:** {min_kw} | **LIMIT:** {limit} | **Página:** {page}")
st.write(f"**Registros retornados:** {len(df):,}")

if df.empty:
    st.warning("Nenhum registro retornado. Tente: min_kw=0, LIMIT=200, Página=0. "
               "Se persistir, revise o mapeamento de colunas no sidebar.")
    st.caption(f"Colunas detectadas no dataset: {cols}")
else:
    cols_show = ["UF","Potencial","Demanda_kW","GoogleMaps","Latitude","Longitude"]
    # Se existirem no DF, adiciona algumas úteis
    for extra in ["cnae","CNAE","lgrd","LGRD","brr","BRR","cep","CEP","cod_id_encr","COD_ID_ENCR"]:
        if extra in df.columns and extra not in cols_show:
            cols_show.append(extra)

    cols_show = [c for c in cols_show if c in df.columns]
    st.dataframe(df[cols_show], use_container_width=True, height=520)

    mapa = df[["Latitude","Longitude"]].rename(columns={"Latitude":"lat","Longitude":"lon"})
    st.map(mapa)

    st.download_button(
        "Baixar CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name="aneel_export.csv",
        mime="text/csv"
    )
