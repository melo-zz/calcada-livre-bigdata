"""Configuração compartilhada do pipeline (Calçada Livre). Nada aqui é segredo."""
import os
from pathlib import Path

CHEGADA = Path(os.environ.get("GS_CHEGADA", "/opt/airflow/chegada"))
MUNICIPIO_IBGE_PADRAO = "3550308"  # São Paulo

REGIOES = [
    {"regiao_id": "santo_amaro",    "regiao_nome": "Santo Amaro",       "subprefeitura": "Santo Amaro",       "distrito_ibge": "Santo Amaro",       "lat": -23.6547, "lon": -46.7088},
    {"regiao_id": "capela_socorro", "regiao_nome": "Capela do Socorro", "subprefeitura": "Capela do Socorro", "distrito_ibge": "Socorro", "lat": -23.7100, "lon": -46.7050},
    {"regiao_id": "jabaquara",      "regiao_nome": "Jabaquara",         "subprefeitura": "Jabaquara",         "distrito_ibge": "Jabaquara",         "lat": -23.6459, "lon": -46.6414},
    {"regiao_id": "campo_limpo",    "regiao_nome": "Campo Limpo",       "subprefeitura": "Campo Limpo",       "distrito_ibge": "Campo Limpo",       "lat": -23.6330, "lon": -46.7600},
    {"regiao_id": "pinheiros",      "regiao_nome": "Pinheiros",         "subprefeitura": "Pinheiros",         "distrito_ibge": "Pinheiros",         "lat": -23.5670, "lon": -46.7020},
    {"regiao_id": "se",             "regiao_nome": "Sé (Centro)",       "subprefeitura": "Sé",                "distrito_ibge": "Sé",                "lat": -23.5505, "lon": -46.6333},
    {"regiao_id": "santana",        "regiao_nome": "Santana",           "subprefeitura": "Santana/Tucuruvi",  "distrito_ibge": "Santana",           "lat": -23.5000, "lon": -46.6250},
    {"regiao_id": "itaquera",       "regiao_nome": "Itaquera",          "subprefeitura": "Itaquera",          "distrito_ibge": "Itaquera",          "lat": -23.5400, "lon": -46.4560},
]

FONTES = ["sp156", "chuva", "ibge", "ocorrencias"]  # subpastas de chegada/
