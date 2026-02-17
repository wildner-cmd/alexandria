import time
import pandas as pd
import streamlit as st
import requests

st.set_page_config(page_title="Alexandria — Prospecção Grupo A (ANEEL)", layout="wide")

st.title("Alexandria — Prospecção Grupo A (ANEEL)")
st.caption("Versão sem SQL (datastore_search). Filtra/ordena no Python e gera TOP 500.")

CKAN_SEARCH_URL = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search"

RESOURCE_UCAT = "4318d38a-0bcd-421d-afb1-fb88b0c92a87"
RESOURCE_UCMT = "f6671cba-f269-42ef-8eb3-62cb3bfa0b98"

UF_BY_IBGE_UF_CODE = {
    11:"RO",12:"AC",13:"AM",14:"RR",15:"PA",16:"AP",17:"TO",
    21:"MA",22:"PI",23:"CE",24:"RN",25:"PB",26:"PE",27:"AL",28:"SE",29:"BA",
    31:"MG",32:"ES",33:"RJ",35:"SP",
    41:"PR",42:"SC",43:"RS",
    50:"MS",51:"MT",52:"GO",53:"DF"
}

def ckan_search(resource_id: str, limit: int, offset: int) -> pd.DataFrame:
    """datastore_search (sem SQL)."""
    last = None
    headers = {"User-Agent": "AlexandriaStreamlit/1.0"}

    for i in range(4):
        try:
            r = requests.get(
                CKAN_SEARCH_URL,
                params={"resource_id": resource_id, "limit": int(limit), "offset": int(offset)},
                timeout=90,
                headers=headers
            )

            if r.status_code in (429, 500, 502, 503, 504):
                last = (r.status_code, (r.text or "")[:250])
                time.sleep(2 * (i+1))
                continue

            if r.status_code != 200:
                last = (r.status_code, (r.text or "")[:250])
                time.sleep(2 * (i+1))
                continue

            data = r.json()
            if not data.get("success"):
                last = ("success=false", str(data)[:250])
                time.sleep(2 * (i+1))
                continue

            return pd.DataFrame(data["result"]["records"])

        except Exception as e:
            last = ("EXC", str(e)[:250])
            time.sleep(2 * (i+1))

    st.error(f"Falha ao consultar ANEEL/CKAN. Detalhe: {last}")
    return pd.DataFrame()

def to_num(x):
    """Converte texto de demanda para float (remove lixo)."""
    if x is None:
        return None
    s = str(x)
    s = s.replace(",", ".")
    # mantém só dígitos e ponto
    cleaned = "".join(ch for ch in s if (ch.isdigit() or ch == "."))
    try:
        return float(cleaned) if cleaned else None
    except:
        return None

def enrich(df: pd.DataFrame, demand_col: str, lat_col: str, lon_col: str, mun_col: str | None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    # padroniza
    df["Demanda_kW"] = df[demand_col].apply(to_num) if demand_col in df.columns else None
    df["Latitude"] = pd.to_numeric(df[lat_col], errors="coerce") if lat_col in df.columns else None
    df["Longitude"] = pd.to_numeric(df[lon_col], errors="coerce") if lon_col in df.columns else None

    df = df.dropna(subset=["Demanda_kW", "Latitude", "Longitude"])

    if mun_col and mun_col in df.columns:
        df[mun_col] = pd.to_numeric(df[mun_col], errors="coerce")
        uf_code = (df[mun_col].fillna(0).astype(int) // 100000).astype(int)
        df["UF"] = uf_code.map(UF_BY_IBGE_UF_CODE).fillna("??")
    else:
        df["UF"] = "??"

    def potencial(x):
        if x >= 5000: return "AAA"
        if x >= 2000: return "AA"
        if x >= 500:  return "A"
        if x >= 100:  return "B"
        return "C"
    df["Potencial"] = df["Demanda_kW"].apply(potencial)

    df["GoogleMaps"] = df.apply(lambda r: f"https://www.google.com/maps?q={r['Latitude']},{r['Longitude']}", axis=1)
    return df

@st.cache_data(ttl=3600)
def probe_columns(resource_id: str) -> list[str]:
    df = ckan_search(resource_id, limit=1, offset=0)
    return list(df.columns) if df is not None and not df.empty else []

def pick(cols: list[str], candidates: list[str]) -> str | None:
    low = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in low:
            return low[cand.lower()]
    return None

def fetch_top(resource_id: str, demand_col: str, lat_col: str, lon_col: str, mun_col: str | None,
              min_kw: float, top_n: int, chunk: int = 5000, max_chunks: int = 12) -> pd.DataFrame:
    """
    Busca em blocos no datastore_search, filtra/ordena no Python e devolve Top N.
    max_chunks controla quanto do dataset você varre (evita travar).
    """
    all_parts = []
    offset = 0

    for _ in range(max_chunks):
        df0 = ckan_search(resource_id, limit=chunk, offset=offset)
        if df0.empty:
            break

        df1 = enrich(df0, demand_col, lat_col, lon_col, mun_col)
        df1 = df1[df1["Demanda_kW"] >= float(min_kw)]

        all_parts.append(df1)
        offset += chunk

        # atalho: se já temos bastante, podemos parar cedo
        if sum(len(x) for x in all_parts) >= top_n * 8:
            break

    if not all_parts:
        return pd.DataFrame()

    df = pd.concat(all_parts, ignore_index=True)
    df = df.sort_values("Demanda_kW", ascending=False).head(int(top_n))
    return df

# ============ UI ============
st.sidebar.header("Parâmetros")

fonte = st.sidebar.selectbox("Fonte", ["UCMT (Média tensão PJ)", "UCAT (Alta tensão PJ)"])
resource_id = RESOURCE_UCMT if fonte.startswith("UCMT") else RESOURCE_UCAT

min_kw = st.sidebar.number_input("Demanda mínima (kW)", min_value=0.0, value=1000.0, step=100.0)
ufs = ["(todas)"] + list(UF_BY_IBGE_UF_CODE.values())
uf_sel = st.sidebar.selectbox("Filtro UF (opcional)", ufs, index=0)
uf_filter = None if uf_sel == "(todas)" else uf_sel

cols = probe_columns(resource_id)
if not cols:
    st.error("Não consegui ler colunas do dataset (datastore_search). Tente 'Manage app → Reboot'.")
    st.stop()

DEMAND_CANDS = ["dem_cont", "DEM_CONT", "demanda", "dem_kw", "dem"]
LAT_CANDS    = ["point_y", "POINT_Y", "lat", "latitude", "y"]
LON_CANDS    = ["point_x", "POINT_X", "lon", "longitude", "x"]
MUN_CANDS    = ["mun", "MUN", "cod_mun", "ibge_mun", "municipio", "cd_mun"]

auto_demand = pick(cols, DEMAND_CANDS) or cols[0]
auto_lat    = pick(cols, LAT_CANDS) or cols[0]
auto_lon    = pick(cols, LON_CANDS) or cols[0]
auto_mun    = pick(cols, MUN_CANDS)

st.sidebar.subheader("Mapeamento de colunas")
demand_col = st.sidebar.selectbox("Coluna de Demanda", cols, index=cols.index(auto_demand) if auto_demand in cols else 0)
lat_col    = st.sidebar.selectbox("Coluna de Latitude", cols, index=cols.index(auto_lat) if auto_lat in cols else 0)
lon_col    = st.sidebar.selectbox("Coluna de Longitude", cols, index=cols.index(auto_lon) if auto_lon in cols else 0)
mun_col    = st.sidebar.selectbox("Coluna de Município (opcional)", ["(nenhuma)"] + cols,
                                 index=(["(nenhuma)"] + cols).index(auto_mun) if auto_mun in cols else 0)
mun_col = None if mun_col == "(nenhuma)" else mun_col

st.sidebar.subheader("TOP (atalhos)")
top_n = st.sidebar.selectbox("Tamanho do TOP", [100, 200, 500, 1000], index=2)
btn_top = st.sidebar.button("Gerar TOP agora", type="primary")

# Execução
if btn_top:
    with st.spinner("Buscando blocos e calculando TOP..."):
        df = fetch_top(resource_id, demand_col, lat_col, lon_col, mun_col, min_kw=min_kw, top_n=int(top_n))

    if df.empty:
        st.warning("Nenhum registro retornado. Teste min_kw=0 e confira o mapeamento de colunas.")
    else:
        if uf_filter:
            df = df[df["UF"] == uf_filter]

        st.write(f"**Fonte:** {fonte} | **min_kw:** {min_kw} | **TOP:** {top_n} | **Filtro UF:** {uf_filter or 'todas'}")
        st.write(f"**Registros exibidos:** {len(df):,}")

        cols_show = ["UF","Potencial","Demanda_kW","GoogleMaps","Latitude","Longitude"]
        for extra in ["CNAE","cnae","LGRD","lgrd","BRR","brr","CEP","cep","COD_ID_ENCR","cod_id_encr","MUN","mun"]:
            if extra in df.columns and extra not in cols_show:
                cols_show.append(extra)
        cols_show = [c for c in cols_show if c in df.columns]

        st.dataframe(df[cols_show], use_container_width=True, height=520)

        mapa = df[["Latitude","Longitude"]].rename(columns={"Latitude":"lat","Longitude":"lon"})
        st.map(mapa)

        st.download_button(
            "Baixar CSV (TOP)",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name=f"aneel_top_{top_n}.csv",
            mime="text/csv"
        )
else:
    st.info("Clique em **Gerar TOP agora** no sidebar para buscar dados e montar o TOP.")
    st.caption("Dica: comece com TOP 100 e min_kw=0 para validar. Depois aumente para TOP 500.")
