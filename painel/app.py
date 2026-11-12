"""Painel Calçada Livre (tela do MVP para Big Data).
Consome as views da Ouro exportadas para data/mvp_mapa.csv e data/mvp_indicadores.csv. Nenhum dado pessoal chega aqui."""
from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Calçada Livre - Painel", page_icon="♿", layout="wide")
st.title("Calçada Livre: barreiras de acessibilidade em São Paulo")
st.caption("Pipeline: Airflow -> Databricks (Bronze/Prata/Ouro) -> este painel. Ocorrências simuladas; pedidos SP156 e chuva reais.")

DADOS = Path(__file__).parent / "data"
try:
    mapa = pd.read_csv(DADOS / "mvp_mapa.csv", parse_dates=["data_registro"])
    ind = pd.read_csv(DADOS / "mvp_indicadores.csv")
except FileNotFoundError:
    st.error("Exporte as views ouro_vw_mvp_mapa e ouro_vw_mvp_indicadores para painel/data/ (veja o README).")
    st.stop()

tipos = st.multiselect("Tipo de barreira", sorted(mapa["tipo"].unique()), default=sorted(mapa["tipo"].unique()))
status = st.multiselect("Status", sorted(mapa["status"].unique()), default=["aberta", "em_atendimento"])
f = mapa[mapa["tipo"].isin(tipos) & mapa["status"].isin(status)]

c1, c2, c3 = st.columns(3)
c1.metric("Barreiras exibidas", len(f))
c2.metric("Prioridade alta", int((f["prioridade"] == "alta").sum()))
c3.metric("Tempo médio de atendimento (dias)", float(round(ind["tempo_medio_dias"].mean(), 1)))

st.subheader("Mapa de ocorrências")
st.map(f.rename(columns={"lat": "latitude", "lon": "longitude"})[["latitude", "longitude"]])
st.subheader("Indicadores por local")
st.dataframe(ind.rename(columns={"local": "Local", "ocorrencias": "Ocorrências", "abertas": "Abertas",
                                 "tempo_medio_dias": "Tempo médio (dias)", "pct_resolvidas_30d": "% resolvidas em 30 dias"}),
             hide_index=True, use_container_width=True)
st.bar_chart(f.groupby("tipo").size())
st.info("Ocorrências deste painel são simuladas; o encaminhamento à Prefeitura não é real.")
