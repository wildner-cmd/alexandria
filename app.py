import time
import pandas as pd
import streamlit as st
import requests

st.set_page_config(page_title="Alexandria — Prospecção Grupo A", layout="wide")

st.title("Alexandria — Prospecção Grupo A (ANEEL)")

CKAN_SQL_URL = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search_sql"

RESOURCE_UCAT = "4318d38a-0bcd-421d-afb1-fb88b0c92a87"
RESOURCE_UCMT = "f6671cba-f269-42ef-8eb3-62cb3bfa0b98"

UF_BY_IBGE_UF_CODE = {
    11:"RO",12:"AC",13:"AM",14:"RR",15:"PA",16:"AP",17:"TO",
    21:"MA",22:"PI",23:"CE",24:"RN",25:"PB",26:"PE",27:"AL",28:"SE",29:"BA",
    31:"MG",32:"ES",33:"RJ",35:"SP",
    41:"PR",42:"SC",43:"RS",
    50:"MS",51:"MT",52:"GO",53:"DF"
}

def ckan_sql(sql):

    last = None

    for i in range(4):

        try:

            r = requests.get(CKAN_SQL_URL, params={"sql": sql}, timeout=60)

            if r.status_code == 200:

                data = r.json()

                if data["success"]:

                    return pd.DataFrame(data["result"]["records"])

            last = r.text

        except Exception as e:

            last = str(e)

        time.sleep(2)

    st.error(f"Erro ANEEL: {last}")

    return pd.DataFrame()

def carregar(resource, min_kw, limit, page):

    offset = page * limit

    sql = f'''
    SELECT *
    FROM "{resource}"
    WHERE dem_cont IS NOT NULL
    AND dem_cont >= {min_kw}
    LIMIT {limit}
    OFFSET {offset}
    '''

    df = ckan_sql(sql)

    if df.empty:
        return df

    # detectar automaticamente nomes das colunas

    if "point_x" in df.columns:
        df["Longitude"] = pd.to_numeric(df["point_x"], errors="coerce")

    if "point_y" in df.columns:
        df["Latitude"] = pd.to_numeric(df["point_y"], errors="coerce")

    if "dem_cont" in df.columns:
        df["Demanda_kW"] = pd.to_numeric(df["dem_cont"], errors="coerce")

    if "mun" in df.columns:
        df["UF"] = (df["mun"] // 100000).map(UF_BY_IBGE_UF_CODE)

    df = df.dropna(subset=["Latitude","Longitude"])

    return df

# Sidebar

st.sidebar.header("Parâmetros")

fonte = st.sidebar.selectbox(
    "Fonte",
    ["UCMT (Média tensão PJ)", "UCAT (Alta tensão PJ)"]
)

resource = RESOURCE_UCMT if fonte.startswith("UCMT") else RESOURCE_UCAT

min_kw = st.sidebar.number_input("Demanda mínima", value=1000)

limit = st.sidebar.slider("LIMIT",100,5000,1000)

page = st.sidebar.number_input("Página",0)

# carregar

df = carregar(resource,min_kw,limit,page)

st.write("Registros retornados:",len(df))

if df.empty:

    st.warning("Nenhum registro. Teste min_kw=0")

else:

    st.dataframe(df)

    mapa = df.rename(columns={
        "Latitude":"lat",
        "Longitude":"lon"
    })

    st.map(mapa)
