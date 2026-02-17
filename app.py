import time
import pandas as pd
import streamlit as st
import requests

st.set_page_config(page_title="Alexandria — Prospecção Grupo A", layout="wide")

st.title("Alexandria — Prospecção Grupo A (ANEEL)")
st.caption("Consulta direta na ANEEL (CKAN DataStore) + UF automática + tabela + mapa. Versão resiliente (retries).")

CKAN_SQL_URL = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search_sql"

# Resource IDs (ANEEL / BDGD)
RESOURCE_UCAT = "4318d38a-0bcd-421d-afb1-fb88b0c92a87"  # UCAT PJ
RESOURCE_UCMT = "f6671cba-f269-42ef-8eb3-62cb3bfa0b98"  # UCMT PJ

UF_BY_IBGE_UF_CODE = {
    11: "RO", 12: "AC", 13: "AM", 14: "RR", 15: "PA", 16: "AP", 17: "TO",
    21: "MA", 22: "PI", 23: "CE", 24: "RN", 25: "PB", 26: "PE", 27: "AL", 28: "SE", 29: "BA",
    31: "MG", 32: "ES", 33: "RJ", 35: "SP",
    41: "PR", 42: "SC", 43: "RS",
    50: "MS", 51: "MT", 52: "GO", 53: "DF"
}

def ckan_sql(sql: str, timeout=60, retries=4, backoff=1.7) -> pd.DataFrame:
    """Consulta CKAN com retries para 429/5xx/timeouts. Retorna DataFrame (pode ser vazio)."""
    last = None
    headers = {"User-Agent": "AlexandriaStreamlit/1.0"}

    for i in range(retries):
        try:
            r = requests.get(CKAN_SQL_URL, params={"sql": sql}, timeout=timeout, headers=headers)

            # Se estiver rate limited ou instável, tenta novamente
            if r.status_code in (429, 500, 502, 503, 504):
                last = (r.status_code, (r.text or "")[:200])
                time.sleep(min(8, backoff ** i))
                continue

            if r.status_code != 200:
                last = (r.status_code, (r.text or "")[:200])
                time.sleep(min(8, backoff ** i))
                continue

            data = r.json()
            if not data.get("success"):
                last = ("success=false", str(data)[:200])
                time.sleep(min(8, backoff ** i))
                continue

            return pd.DataFrame(data["result"]["records"])

        except Exception as e:
            last = ("EXC", str(e)[:200])
            time.sleep(min(8, backoff ** i))

    st.error(f"Falha ao consultar ANEEL/CKAN após retries. Detalhe: {last}")
    return pd.DataFrame()

def carregar(resource_id: str, min_kw: float, limit: int, page: int) -> pd.DataFrame:
    offset = int(page) * int(limit)

    sql = f'''
    SELECT
      cod_id_encr, cnae, dem_cont, lgrd, brr, cep, point_y, point_x, mun, dist
    FROM "{resource_id}"
    WHERE dem_cont IS NOT NULL
      AND dem_cont >= {min_kw}
      AND point_x IS NOT NULL AND point_y IS NOT NULL
    ORDER BY dem_cont DESC
    LIMIT {limit} OFFSET {offset}
    '''

    df = ckan_sql(sql)
    if df.empty:
        return df

    # Renomeia colunas
    df = df.rename(columns={
        "cod_id_encr": "COD_ID_ENCR",
        "cnae": "CNAE",
        "dem_cont": "Demanda_kW",
        "lgrd": "Logradouro",
        "brr": "Bairro",
        "cep": "CEP",
        "point_y": "Latitude",
        "point_x": "Longitude",
        "mun": "MUN",
        "dist": "DIST"
    })

    # Tipos numéricos
    df["Latitude"] = pd.to_numeric(df["Latitude"], errors="coerce")
    df["Longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")
    df["Demanda_kW"] = pd.to_numeric(df["Demanda_kW"], errors="coerce")
    df["MUN"] = pd.to_numeric(df["MUN"], errors="coerce")
    df = df.dropna(subset=["Latitude", "Longitude", "Demanda_kW"])

    # UF via MUN (IBGE município 7 dígitos: 2 primeiros = UF)
    uf_code = (df["MUN"].astype(int) // 100000).astype(int)
    df["UF"] = uf_code.map(UF_BY_IBGE_UF_CODE).fillna("??")

    # Potencial simples por demanda
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

# Sidebar
st.sidebar.header("Parâmetros")

fonte = st.sidebar.selectbox("Fonte", ["UCMT (Média tensão PJ)", "UCAT (Alta tensão PJ)"])
resource_id = RESOURCE_UCMT if fonte.startswith("UCMT") else RESOURCE_UCAT

min_kw = st.sidebar.number_input("Demanda mínima (kW)", min_value=0.0, value=1000.0, step=100.0)
limit = st.sidebar.slider("LIMIT (linhas)", 100, 5000, 1000, step=100)
page = st.sidebar.number_input("Página", min_value=0, value=0, step=1)

# Carrega automaticamente
with st.spinner("Consultando ANEEL..."):
    df = carregar(resource_id, min_kw, int(limit), int(page))

st.write(f"**Fonte:** {fonte} | **min_kw:** {min_kw} | **LIMIT:** {limit} | **Página:** {page}")
st.write(f"**Registros retornados:** {len(df):,}")

if df.empty:
    st.warning("Nenhum registro retornado. Teste: min_kw=0, LIMIT=200, Página=0. Se persistir, a ANEEL pode estar instável.")
else:
    # Tabela
    cols = ["UF","Potencial","Demanda_kW","CNAE","Logradouro","Bairro","CEP","GoogleMaps","COD_ID_ENCR"]
    cols = [c for c in cols if c in df.columns]
    st.dataframe(df[cols], use_container_width=True, height=520)

    # Mapa (Streamlit espera colunas lat/lon)
    mapa = df[["Latitude","Longitude"]].rename(columns={"Latitude":"lat","Longitude":"lon"})
    st.map(mapa)
