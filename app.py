import time
import pandas as pd
import streamlit as st
import requests

st.set_page_config(page_title="Alexandria", layout="wide")

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
    for i in range(3):
        try:
            r = requests.get(CKAN_SQL_URL, params={"sql": sql}, timeout=60)
            if r.status_code == 200:
                data = r.json()
                if data["success"]:
                    return pd.DataFrame(data["result"]["records"])
        except:
            pass
        time.sleep(2)
    st.error("Erro ao consultar ANEEL")
    return pd.DataFrame()

def carregar(resource, min_kw, limit, offset):
    sql = f'''
    SELECT cod_id_encr, cnae, dem_cont, point_y, point_x, mun
    FROM "{resource}"
    WHERE dem_cont >= {min_kw}
    LIMIT {limit} OFFSET {offset}
    '''
    df = ckan_sql(sql)

    if df.empty:
        return df

    df = df.rename(columns={
        "dem_cont":"Demanda",
        "point_y":"Latitude",
        "point_x":"Longitude",
        "mun":"MUN"
    })

    df["UF"] = (df["MUN"]//100000).map(UF_BY_IBGE_UF_CODE)

    return df

st.title("Alexandria — Prospecção Grupo A")

fonte = st.sidebar.selectbox("Fonte", ["UCMT","UCAT"])
resource = RESOURCE_UCMT if fonte=="UCMT" else RESOURCE_UCAT

min_kw = st.sidebar.number_input("Demanda mínima", value=2000)
limit = st.sidebar.slider("LIMIT",100,5000,1000)
pagina = st.sidebar.number_input("Página",0)
