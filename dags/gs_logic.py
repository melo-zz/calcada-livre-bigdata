"""Lógica pura (sem Airflow), testável fora do container."""
import csv
import hashlib
import itertools
import random
import re
import unicodedata
from datetime import date, timedelta

from gs_config import REGIOES


def sem_acento(s):
    s = unicodedata.normalize("NFKD", str(s).lower())
    return "".join(c for c in s if not unicodedata.combining(c))


def norm_col(s):
    return re.sub(r"[^a-z0-9]+", "_", sem_acento(s)).strip("_")


def filtrar_sp156(linhas, palavras, max_linhas=30000):
    """Recebe as linhas (str) de um CSV do SP156 e devolve só as relacionadas a calçada.
    Não assume nomes de coluna: procura as palavras nas colunas de texto (serviço/assunto/tema...)."""
    it = iter(linhas)
    cab_linha = next(it)
    delim = ";" if cab_linha.count(";") >= cab_linha.count(",") else ","
    leitor = csv.reader(itertools.chain([cab_linha], it), delimiter=delim)
    cab = [norm_col(c) for c in next(leitor)]
    idx = [i for i, c in enumerate(cab) if any(k in c for k in ("servico", "assunto", "tema", "tipo", "descricao", "categoria"))]
    if not idx:
        idx = list(range(len(cab)))
    alvo = [sem_acento(p).strip() for p in palavras if p.strip()]
    saida, lidas = [], 0
    for row in leitor:
        lidas += 1
        if len(row) != len(cab):
            continue
        txt = sem_acento(" ".join(row[i] for i in idx))
        if any(p in txt for p in alvo):
            saida.append(dict(zip(cab, row)))
            if len(saida) >= max_linhas:
                break
    return saida, cab, lidas


TIPOS = [("buraco", 30), ("obstaculo", 25), ("piso_quebrado", 20), ("degrau_sem_rampa", 15), ("trecho_escuro", 10)]
ORIGENS = [("app", 50), ("carrinho", 30), ("visao", 20)]
PRIMEIROS = ["Ana", "Bruno", "Carla", "Diego", "Elisa", "Fabio", "Gabriela", "Heitor", "Iara", "Joao", "Karen", "Lucas"]
SOBRENOMES = ["Souza", "Lima", "Dias", "Alves", "Rocha", "Nunes", "Melo", "Costa", "Mendes", "Pires", "Lopes", "Barros"]


def gerar_ocorrencias(ds: date, dias=60, por_dia=6):
    """Snapshot das ocorrências dos últimos `dias` dias, com o status conhecido em `ds`.
    Determinístico: os atributos de uma ocorrência são os mesmos em qualquer snapshot; só o status
    evolui (por isso o MERGE da Prata atualiza em vez de duplicar).
    ~5% de registros ruins de propósito (coordenada fora de SP, tipo desconhecido, data futura).
    Dados pessoais FICTÍCIOS: nomes genéricos, e-mail em domínio reservado, telefone em faixa fictícia."""
    linhas = []
    for off in range(dias - 1, -1, -1):
        d = ds - timedelta(days=off)
        for i in range(por_dia):
            chave = f"{d.isoformat()}-{i}"
            rnd = random.Random(chave)
            reg = rnd.choice(REGIOES)
            tipo = rnd.choices([t for t, _ in TIPOS], weights=[w for _, w in TIPOS])[0]
            origem = rnd.choices([o for o, _ in ORIGENS], weights=[w for _, w in ORIGENS])[0]
            dias_res = rnd.choices([None, 3, 7, 12, 20, 28, 35, 45, 60], weights=[25, 8, 12, 12, 12, 10, 8, 8, 5])[0]
            resolvida = dias_res is not None and d + timedelta(days=dias_res) <= ds
            data_res = (d + timedelta(days=dias_res)).isoformat() if resolvida else None
            status = "resolvida" if resolvida else ("em_atendimento" if rnd.random() < 0.5 else "aberta")
            prioridade = "alta" if (tipo in ("degrau_sem_rampa", "buraco") and rnd.random() < 0.4) else rnd.choice(["media", "baixa"])
            lat = round(reg["lat"] + rnd.uniform(-0.008, 0.008), 6)
            lon = round(reg["lon"] + rnd.uniform(-0.008, 0.008), 6)
            nome = f"{rnd.choice(PRIMEIROS)} {rnd.choice(SOBRENOMES)}"
            email = f"{sem_acento(nome).replace(' ', '.')}{rnd.randint(1, 999)}@exemplo.com.br"
            tel = "11" + "90000" + f"{rnd.randint(0, 9999):04d}"
            data_registro = d.isoformat()
            r = rnd.random()
            if r < 0.02:
                lat, lon = 0.0, 0.0
            elif r < 0.04:
                tipo = "desconhecido"
            elif r < 0.05:
                data_registro = (ds + timedelta(days=3)).isoformat()
            linhas.append({
                "id_ocorrencia": hashlib.sha1(chave.encode()).hexdigest()[:12],
                "tipo_barreira": tipo, "origem": origem, "prioridade": prioridade, "status": status,
                "regiao_id": reg["regiao_id"], "lat": lat, "lon": lon,
                "data_registro": data_registro, "data_resolucao": data_res,
                "reporter_nome": nome, "reporter_email": email, "reporter_telefone": tel,
                "data_atualizacao": ds.isoformat(),
            })
    linhas.extend(dict(x) for x in linhas[:3])  # duplicados exatos
    return linhas
