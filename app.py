import time
import urllib.parse
import pandas as pd
import streamlit as st
import requests

st.set_page_config(page_title="Alexandria — Prospecção Grupo A (ANEEL)", layout="wide")
st.title("Alexandria — Prospecção Grupo A (ANEEL)")
st.caption("Versão comercial (sem Places): TOP maior, filtros (Potencial/CNAE/Município), Score Alexandria e export CRM.")

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

# ---------------- CKAN (sem SQL) ----------------
def ckan_search(resource_id: str, limit: int, offset: int) -> pd.DataFrame:
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

def to_num(x):
    if x is None:
        return None
    s = str(x).replace(",", ".")
    cleaned = "".join(ch for ch in s if (ch.isdigit() or ch == "."))
    try:
        return float(cleaned) if cleaned else None
    except:
        return None

def make_google_search_link(query: str) -> str:
    return "https://www.google.com/search?q=" + urllib.parse.quote(query)

def make_whatsapp_link(msg: str) -> str:
    return "https://wa.me/?text=" + urllib.parse.quote(msg)

def potencia_label(dem_kw: float) -> str:
    if dem_kw >= 5000: return "AAA"
    if dem_kw >= 2000: return "AA"
    if dem_kw >= 500:  return "A"
    if dem_kw >= 100:  return "B"
    return "C"

def calc_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Score 0–100 (priorização):
    - Demanda (principal): 0–70 pontos (log)
    - Potencial: 0–20 pontos (AAA/AA/A/B/C)
    - Urbanidade: 0–10 pontos (endereço mais completo tende a ser mais acionável)
    """
    out = df.copy()

    # 1) Demanda -> 0..70 (log para não explodir)
    d = out["Demanda_kW"].clip(lower=0.0)
    import numpy as np
    dem_score = np.log10(d + 1)  # 0..~6
    # normaliza usando faixa típica 100..500000 (ajuste leve)
    dem_norm = (dem_score - np.log10(100+1)) / (np.log10(500000+1) - np.log10(100+1))
    dem_norm = dem_norm.clip(0, 1)
    out["Score_Demanda"] = (dem_norm * 70).round(1)

    # 2) Potencial -> 0..20
    pot_map = {"AAA": 20, "AA": 16, "A": 12, "B": 6, "C": 2}
    out["Score_Potencial"] = out["Potencial"].map(pot_map).fillna(0)

    # 3) Urbanidade -> 0..10 (endereço com CEP/bairro/rua)
    def urban_score(addr: str) -> int:
        if not addr:
            return 0
        s = str(addr).upper()
        pts = 0
        if "CEP" in s: pts += 4
        if "—" in s or "," in s: pts += 3
        # se parece logradouro (tem número ou "RUA"/"AV"/"BR")
        if any(x in s for x in ["RUA", "AV", "AL", "BR ", "ROD", "KM"]) or any(ch.isdigit() for ch in s):
            pts += 3
        return min(10, pts)

    out["Score_Acionavel"] = out["Endereco"].apply(urban_score)

    out["Score_Alexandria"] = (out["Score_Demanda"] + out["Score_Potencial"] + out["Score_Acionavel"]).round(1)
    return out

def enrich_base(df: pd.DataFrame, demand_col: str, lat_col: str, lon_col: str, mun_col: str | None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    df["Demanda_kW"] = df[demand_col].apply(to_num) if demand_col in df.columns else None
    df["Latitude"] = pd.to_numeric(df[lat_col], errors="coerce") if lat_col in df.columns else None
    df["Longitude"] = pd.to_numeric(df[lon_col], errors="coerce") if lon_col in df.columns else None
    df = df.dropna(subset=["Demanda_kW", "Latitude", "Longitude"])

    # UF
    if mun_col and mun_col in df.columns:
        mun_num = pd.to_numeric(df[mun_col], errors="coerce")
        uf_code = (mun_num.fillna(0).astype("int64") // 100000).astype("int64")
        df["UF"] = uf_code.map(UF_BY_IBGE_UF_CODE).fillna("??")
        df["IBGE_MUN"] = pd.to_numeric(df[mun_col], errors="coerce")
    else:
        df["UF"] = "??"

    # Potencial
    df["Potencial"] = df["Demanda_kW"].apply(potencia_label)

    # endereço (se existir)
    LGRD = next((c for c in df.columns if c.upper() in ("LGRD","LOGRADOURO","ENDERECO")), None)
    BRR  = next((c for c in df.columns if c.upper() in ("BRR","BAIRRO")), None)
    CEP  = next((c for c in df.columns if c.upper() == "CEP"), None)

    CNAE = next((c for c in df.columns if c.upper() == "CNAE"), None)
    DIST = next((c for c in df.columns if c.upper() in ("DIST","DISTRIBUIDORA","SIGLA_DIST")), None)
    IDUC = next((c for c in df.columns if c.upper() in ("COD_ID_ENCR","COD_ID_ENC","COD_ID_ENCRYP")), None)

    def addr_text(r):
        parts = []
        if LGRD and pd.notna(r.get(LGRD)): parts.append(str(r.get(LGRD)).strip())
        if BRR and pd.notna(r.get(BRR)): parts.append(str(r.get(BRR)).strip())
        if CEP and pd.notna(r.get(CEP)): parts.append(f"CEP {str(r.get(CEP)).strip()}")
        parts.append(r.get("UF",""))
        return " — ".join([p for p in parts if p])

    df["Endereco"] = df.apply(addr_text, axis=1)
    df["GoogleMaps"] = df.apply(lambda r: f"https://www.google.com/maps?q={r['Latitude']},{r['Longitude']}", axis=1)
    df["BuscaGoogle"] = df.apply(lambda r: make_google_search_link(r["Endereco"] if r["Endereco"] else f"{r['Latitude']},{r['Longitude']}"), axis=1)

    # WhatsApp texto (sem número)
    msg_base = ("Olá! Tudo bem? Aqui é o Fernando, da Alexandria Energia. "
                "Estou falando com o responsável pela área administrativa/energia? "
                "Fiz um levantamento de grandes consumidores na sua região e posso te mostrar "
                "um diagnóstico rápido para reduzir custos com energia.")
    df["WhatsAppTexto"] = df.apply(lambda r: make_whatsapp_link(msg_base + f" (UF: {r.get('UF','')})"), axis=1)

    if CNAE:
        df["CNAE_Limpo"] = df[CNAE].astype(str).str.replace(r"[^0-9]", "", regex=True)
    if DIST:
        df["Distribuidora"] = df[DIST]
    if IDUC:
        df["ID_UC"] = df[IDUC]

    return df

def fetch_top(resource_id: str, demand_col: str, lat_col: str, lon_col: str, mun_col: str | None,
              min_kw: float, top_n: int, chunk: int, max_chunks: int) -> pd.DataFrame:
    parts = []
    offset = 0
    for _ in range(max_chunks):
        df0 = ckan_search(resource_id, limit=chunk, offset=offset)
        if df0.empty:
            break
        df1 = enrich_base(df0, demand_col, lat_col, lon_col, mun_col)
        df1 = df1[df1["Demanda_kW"] >= float(min_kw)]
        if not df1.empty:
            parts.append(df1)
        offset += chunk

        # para cedo se já tem bastante sobra
        if sum(len(x) for x in parts) >= top_n * 10:
            break

    if not parts:
        return pd.DataFrame()

    df = pd.concat(parts, ignore_index=True)
    df = df.sort_values("Demanda_kW", ascending=False).head(int(top_n))
    return df

# ================= UI =================
st.sidebar.header("Parâmetros")

fonte = st.sidebar.selectbox("Fonte", ["UCMT (Média tensão PJ)", "UCAT (Alta tensão PJ)"])
resource_id = RESOURCE_UCMT if fonte.startswith("UCMT") else RESOURCE_UCAT

min_kw = st.sidebar.number_input("Demanda mínima (kW)", min_value=0.0, value=1000.0, step=100.0)

ufs = ["(todas)"] + list(UF_BY_IBGE_UF_CODE.values())
uf_sel = st.sidebar.selectbox("Filtro UF (opcional)", ufs, index=ufs.index("PR") if "PR" in ufs else 0)
uf_filter = None if uf_sel == "(todas)" else uf_sel

cols = probe_columns(resource_id)
if not cols:
    st.error("Não consegui ler colunas do dataset. Tente 'Manage app → Reboot'.")
    st.stop()

DEMAND_CANDS = ["dem_cont","DEM_CONT","demanda","dem_kw","dem"]
LAT_CANDS    = ["point_y","POINT_Y","lat","latitude","y"]
LON_CANDS    = ["point_x","POINT_X","lon","longitude","x"]
MUN_CANDS    = ["mun","MUN","cod_mun","ibge_mun","municipio","cd_mun"]

auto_demand = pick(cols, DEMAND_CANDS) or cols[0]
auto_lat    = pick(cols, LAT_CANDS) or cols[0]
auto_lon    = pick(cols, LON_CANDS) or cols[0]
auto_mun    = pick(cols, MUN_CANDS)

st.sidebar.subheader("Mapeamento de colunas")
demand_col = st.sidebar.selectbox("Coluna de Demanda", cols, index=cols.index(auto_demand) if auto_demand in cols else 0)
lat_col    = st.sidebar.selectbox("Coluna de Latitude", cols, index=cols.index(auto_lat) if auto_lat in cols else 0)
lon_col    = st.sidebar.selectbox("Coluna de Longitude", cols, index=cols.index(auto_lon) if auto_lon in cols else 0)
mun_col    = st.sidebar.selectbox("Coluna de Município (IBGE) (opcional)", ["(nenhuma)"] + cols,
                                 index=(["(nenhuma)"] + cols).index(auto_mun) if auto_mun in cols else 0)
mun_col = None if mun_col == "(nenhuma)" else mun_col

st.sidebar.subheader("TOP (atalhos)")
top_opt = st.sidebar.selectbox(
    "Tamanho do TOP",
    ["100","200","500","1000","2000","5000","10000","MAX (seguro)"],
    index=2
)

chunk = st.sidebar.selectbox("Tamanho do bloco (chunk)", [2000, 5000, 10000], index=1)
max_chunks = st.sidebar.selectbox("Máx. blocos varridos", [4, 8, 12, 20], index=2)

st.sidebar.subheader("Filtros comerciais")

pot_sel = st.sidebar.multiselect(
    "Potencial (selecionar)",
    ["AAA","AA","A","B","C"],
    default=["AAA","AA","A"]
)

cnae_prefix = st.sidebar.text_input("CNAE começa com (ex: 10, 47, 86) — opcional", "")

mun_mode = st.sidebar.selectbox(
    "Filtro Município (IBGE) — opcional",
    ["(nenhum)", "Curitiba (4106902)", "Informar código IBGE"]
)
mun_custom = None
if mun_mode == "Informar código IBGE":
    mun_custom = st.sidebar.number_input("Código IBGE do município", min_value=0, value=0, step=1)

sort_by = st.sidebar.selectbox("Ordenar por", ["Score Alexandria", "Demanda (kW)"], index=0)

btn_run = st.sidebar.button("Gerar TOP agora", type="primary")

if btn_run:
    if top_opt.startswith("MAX"):
        top_n = int(chunk) * int(max_chunks)
    else:
        top_n = int(top_opt)

    with st.spinner("Buscando dados e montando TOP..."):
        df = fetch_top(resource_id, demand_col, lat_col, lon_col, mun_col,
                       min_kw=min_kw, top_n=top_n, chunk=int(chunk), max_chunks=int(max_chunks))

    if df.empty:
        st.warning("Nenhum registro retornado. Tente min_kw=0, aumente max_chunks ou revise mapeamento.")
        st.stop()

    # UF
    if uf_filter:
        df = df[df["UF"] == uf_filter].copy()

    # Potencial
    if pot_sel:
        df = df[df["Potencial"].isin(pot_sel)].copy()

    # Município (se tiver IBGE_MUN)
    if mun_col and "IBGE_MUN" in df.columns:
        if mun_mode == "Curitiba (4106902)":
            df = df[df["IBGE_MUN"] == 4106902].copy()
        elif mun_mode == "Informar código IBGE" and mun_custom and int(mun_custom) > 0:
            df = df[df["IBGE_MUN"] == int(mun_custom)].copy()

    # CNAE prefix
    if cnae_prefix.strip():
        if "CNAE_Limpo" in df.columns:
            df = df[df["CNAE_Limpo"].astype(str).str.startswith(cnae_prefix.strip())].copy()
        elif "CNAE" in df.columns:
            df = df[df["CNAE"].astype(str).str.replace(r"[^0-9]","",regex=True).str.startswith(cnae_prefix.strip())].copy()

    # Score
    df = calc_score(df)

    # Ordenação
    if sort_by == "Score Alexandria":
        df = df.sort_values("Score_Alexandria", ascending=False).copy()
    else:
        df = df.sort_values("Demanda_kW", ascending=False).copy()

    st.write(f"**Fonte:** {fonte} | **min_kw:** {min_kw} | **TOP:** {top_opt} | **UF:** {uf_filter or 'todas'}")
    st.write(f"**Registros exibidos (pós filtros):** {len(df):,}")

    # Tabela comercial (com score)
    preferred = [
        "Score_Alexandria","Score_Demanda","Score_Potencial","Score_Acionavel",
        "UF","Potencial","Demanda_kW","Endereco","BuscaGoogle","GoogleMaps","WhatsAppTexto",
        "CNAE","CNAE_Limpo","Distribuidora","ID_UC","Latitude","Longitude"
    ]
    cols_show = [c for c in preferred if c in df.columns]

    st.dataframe(df[cols_show], use_container_width=True, height=560)

    mapa = df[["Latitude","Longitude"]].dropna().rename(columns={"Latitude":"lat","Longitude":"lon"})
    if not mapa.empty:
        st.map(mapa)

    # Export CRM
    crm_cols = {
        "Score_Alexandria": "Score",
        "UF": "UF",
        "Potencial": "Potencial",
        "Demanda_kW": "Demanda_kW",
        "Endereco": "Endereco",
        "BuscaGoogle": "BuscaGoogle",
        "GoogleMaps": "GoogleMaps",
        "WhatsAppTexto": "WhatsAppTexto",
        "Distribuidora": "Distribuidora",
        "CNAE_Limpo": "CNAE",
        "ID_UC": "ID_UC",
        "Latitude": "Latitude",
        "Longitude": "Longitude"
    }
    export_df = df.copy()
    export_df = export_df[[c for c in crm_cols.keys() if c in export_df.columns]].rename(columns=crm_cols)

    st.download_button(
        "Baixar CSV (CRM)",
        data=export_df.to_csv(index=False).encode("utf-8"),
        file_name="grupoA_export_crm.csv",
        mime="text/csv"
    )

else:
    st.info("Ajuste parâmetros e clique em **Gerar TOP agora** no sidebar.")
    st.caption("Dica: para TOP grandes, aumente max_chunks e use filtro UF/potencial para manter usável.")
