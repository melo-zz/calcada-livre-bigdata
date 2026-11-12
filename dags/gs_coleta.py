"""DAG de coleta - Calçada Livre (Big Data). Airflow 3.x.

Fontes (4, em paralelo):
  1. SP156      - pedidos de calçada da Prefeitura de SP (dados abertos, CSV)   -> chegada/sp156/
  2. Open-Meteo - chuva diária por região (API pública, sem chave)               -> chegada/chuva/
  3. IBGE       - distritos de São Paulo (API de localidades)                    -> chegada/ibge/
  4. Ocorrências do MVP (simuladas; app/carrinho/visão; dados pessoais fictícios) -> chegada/ocorrencias/
Formato: JSON Lines, com a data da coleta no nome do arquivo."""
import json
import re
import logging
from datetime import date, timedelta

import pendulum
import requests
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Variable

from gs_config import CHEGADA, FONTES, MUNICIPIO_IBGE_PADRAO, REGIOES
from gs_logic import filtrar_sp156, gerar_ocorrencias

log = logging.getLogger(__name__)
TIMEOUT = 60


def _alerta_falha(context):
    """Notificação de falha (log). Para e-mail: configurar SMTP e usar EmailOperator."""
    log.error("NOTIFICACAO: tarefa %s falhou (execucao %s)", context["task_instance"].task_id, context.get("ds"))


def _gravar_jsonl(caminho, linhas):
    caminho.parent.mkdir(parents=True, exist_ok=True)
    tmp = caminho.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        for linha in linhas:
            f.write(json.dumps(linha, ensure_ascii=False) + "\n")
    tmp.replace(caminho)  # escrita atômica
    log.info("Gravado %s (%d linhas)", caminho, len(linhas))


def _get_json(url, params=None):
    r = requests.get(url, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def _url_sp156():
    manual = Variable.get("GS_SP156_CSV_URL", default="")
    if manual:
        return manual
    pkg = _get_json("https://dados.prefeitura.sp.gov.br/api/3/action/package_show", {"id": "dados-do-sp156"})
    csvs = [r for r in pkg["result"]["resources"] if str(r.get("format", "")).upper() == "CSV" and r.get("url")]
    if not csvs:
        raise RuntimeError("Nenhum recurso CSV no dataset dados-do-sp156 (defina a Variable GS_SP156_CSV_URL)")
    csvs = [r for r in csvs if "avalia" not in str(r.get("name", "")).lower()]

    def ordem(r):
        m = re.search(r"(\d)\s*[º°o]?\s*tri\w*\s*(?:de\s*)?(\d{2,4})", str(r.get("name", "")) + " " + str(r.get("url", "")), re.I)
        if not m:
            return (0, 0)
        ano = int(m.group(2))
        return (ano + 2000 if ano < 100 else ano, int(m.group(1)))

    csvs.sort(key=ordem)
    return csvs[-1]["url"]


def coletar_sp156(**context):
    ds = context["ds"]
    url = _url_sp156()
    palavras = Variable.get("GS_SP156_PALAVRAS", default="calcada publica,obstaculo na calcada,calcada particular,guia rebaixada,guias para travessia,construcao de calcadas,piso tatil").split(",")
    max_linhas = int(Variable.get("GS_SP156_MAX_LINHAS", default="30000"))
    log.info("SP156: baixando %s", url)

    def decodificar(b):
        try:
            return b.decode("utf-8-sig")
        except UnicodeDecodeError:
            return b.decode("latin-1")

    with requests.get(url, stream=True, timeout=TIMEOUT) as r:
        r.raise_for_status()
        saida, cab, lidas = filtrar_sp156((decodificar(b) for b in r.iter_lines() if b), palavras, max_linhas)
    log.info("SP156: %d linhas lidas, %d de calçada. Colunas: %s", lidas, len(saida), cab)
    if not saida:
        raise RuntimeError("SP156: nenhuma linha casou; ajuste a Variable GS_SP156_PALAVRAS")
    for linha in saida:
        linha["_data_coleta"], linha["_origem_url"] = ds, url
    _gravar_jsonl(CHEGADA / "sp156" / f"sp156_{ds}.json", saida)


def coletar_chuva(**context):
    ds = context["ds"]
    janela = int(Variable.get("GS_CHUVA_JANELA_DIAS", default="150"))
    url = Variable.get("GS_OPENMETEO_ARCHIVE_URL", default="https://archive-api.open-meteo.com/v1/archive")
    fim = date.fromisoformat(ds) - timedelta(days=2)  # o arquivo histórico tem ~2 dias de atraso
    ini = fim - timedelta(days=janela - 1)
    linhas, falhas = [], []
    for reg in REGIOES:
        params = {"latitude": reg["lat"], "longitude": reg["lon"], "start_date": ini.isoformat(),
                  "end_date": fim.isoformat(), "daily": "precipitation_sum", "timezone": "America/Sao_Paulo"}
        try:
            linhas.append({**reg, "data_coleta": ds, "daily": _get_json(url, params).get("daily")})
        except Exception as e:  # noqa: BLE001
            falhas.append(reg["regiao_id"])
            log.warning("Chuva: falha em %s: %s", reg["regiao_id"], e)
    if not linhas:
        raise RuntimeError("Chuva: nenhuma região coletada (API fora do ar?)")
    if falhas:
        log.warning("Chuva: regiões sem dado: %s", falhas)
    _gravar_jsonl(CHEGADA / "chuva" / f"chuva_{ds}.json", linhas)


def coletar_ibge(**context):
    ds = context["ds"]
    mun = Variable.get("GS_IBGE_MUNICIPIO", default=MUNICIPIO_IBGE_PADRAO)
    dados = _get_json(f"https://servicodados.ibge.gov.br/api/v1/localidades/municipios/{mun}/distritos")
    _gravar_jsonl(CHEGADA / "ibge" / f"ibge_{ds}.json", [{**d, "data_coleta": ds} for d in dados])


def gerar_ocorrencias_mvp(**context):
    ds = context["ds"]
    _gravar_jsonl(CHEGADA / "ocorrencias" / f"ocorrencias_{ds}.json", gerar_ocorrencias(date.fromisoformat(ds)))


def conferir_chegada(**context):
    ds = context["ds"]
    faltando = []
    for fonte in FONTES:
        arq = CHEGADA / fonte / f"{fonte}_{ds}.json"
        if not arq.exists() or arq.stat().st_size == 0:
            faltando.append(str(arq))
        else:
            log.info("OK %s (%d bytes)", arq.name, arq.stat().st_size)
    if faltando:
        raise FileNotFoundError(f"Arquivos ausentes/vazios: {faltando}")


with DAG(
    dag_id="gs_coleta",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 8, tz="America/Sao_Paulo"),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=1), "on_failure_callback": _alerta_falha},
    tags=["gs", "citizentech", "calcada-livre"],
) as dag:
    inicio = EmptyOperator(task_id="inicio")
    t_sp156 = PythonOperator(task_id="coletar_sp156", python_callable=coletar_sp156)
    t_chuva = PythonOperator(task_id="coletar_chuva", python_callable=coletar_chuva)
    t_ibge = PythonOperator(task_id="coletar_ibge", python_callable=coletar_ibge)
    t_ocor = PythonOperator(task_id="gerar_ocorrencias_mvp", python_callable=gerar_ocorrencias_mvp)
    conferir = PythonOperator(task_id="conferir_chegada", python_callable=conferir_chegada)
    inicio >> [t_sp156, t_chuva, t_ibge, t_ocor] >> conferir
