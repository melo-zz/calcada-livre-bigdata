# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Prata (tipos corretos, nulos/inválidos tratados, MERGE pela chave, regras de qualidade com contagem)

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

S = "workspace.gs"
DE, PARA = "áàâãäéèêëíìîïóòôõöúùûüç", "aaaaaeeeeiiiiooooouuuuc"

def merge_prata(df, destino, chaves):
    if not spark.catalog.tableExists(f"{S}.{destino}"):
        df.limit(0).write.saveAsTable(f"{S}.{destino}")
    df.createOrReplaceTempView("lote_prata")
    on = " AND ".join(f"alvo.{c} = novo.{c}" for c in chaves)
    spark.sql(f"""MERGE INTO {S}.{destino} AS alvo USING lote_prata AS novo ON {on}
                  WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
    print(f"{destino}: {spark.table(f'{S}.{destino}').count()} linhas")

def registrar_regra(tabela, regra, barrados):
    spark.sql(f"INSERT INTO {S}.qualidade_prata VALUES (current_timestamp(), '{tabela}', '{regra}', {barrados})")

def aplicar_regras(df, tabela, regras):
    """regras: {nome: condição TRUE quando o registro é VÁLIDO}. Grava quantos cada regra barrou."""
    ok = df
    print(f"== {tabela}: {df.count()} linhas de entrada")
    for nome, cond in regras.items():
        valido = F.coalesce(cond, F.lit(False))
        barrados = ok.filter(~valido).count()
        registrar_regra(tabela, nome, barrados)
        print(f"   regra {nome:<38} barrou {barrados}")
        ok = ok.filter(valido)
    print(f"   restaram {ok.count()}")
    return ok

def dedup_por_chave(df, chave, ordem):
    """Mantém a versão mais recente de cada chave; devolve (df, quantas linhas foram removidas)."""
    total = df.count()
    w = F.row_number().over(Window.partitionBy(chave).orderBy(F.col(ordem).desc()))
    out = df.withColumn("_rn", w).filter("_rn = 1").drop("_rn")
    return out, total - out.count()

def ts_flex(coluna):
    fmts = ["MM/dd/yyyy HH:mm:ss", "MM/dd/yyyy HH:mm", "MM/dd/yyyy", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd"]
    return F.coalesce(*[F.expr(f"try_to_timestamp(`{coluna}`, '{f}')") for f in fmts])

def norm(col):
    return F.translate(F.lower(col), DE, PARA)

# NOTA: o CSV do SP156 não tem coluna de protocolo; a chave é o hash de todas as colunas (linhas 100% idênticas colapsam em uma). Datas vêm em MM/dd/yyyy.

# COMMAND ----------
# MAGIC %md ## 1) SP156 (dados abertos reais). O schema vem do CSV da Prefeitura: o mapeamento das colunas é por nome.

# COMMAND ----------
bz = spark.table(f"{S}.bronze_sp156")
cols = [c for c in bz.columns if not c.startswith("_") and c not in ("arquivo_origem", "carregado_em")]

def achar(*grupos):
    for palavras in grupos:
        for c in cols:
            if all(p in c for p in palavras):
                return c
    return None

C = {
    "protocolo":  achar(["protocolo"], ["numero", "solicitacao"]),
    "abertura":   achar(["data", "abertura"], ["abertura"], ["data", "solicitacao"], ["data", "criacao"]),
    "fechamento": achar(["data", "fechamento"], ["finalizacao"], ["fechamento"], ["data", "conclusao"], ["data", "resolucao"]),
    "status":     achar(["status"], ["situacao"]),
    "servico":    achar(["servico"], ["assunto"], ["tema"]),
    "subpref":    achar(["subprefeitura"], ["prefeitura", "operacional"], ["prefeitura", "regional"], ["regional"], ["distrito"]),
}
print("Colunas do SP156:", cols)
print("Mapeamento:", C)
assert C["abertura"], "Não achei a coluna de data de abertura: ajuste o dicionário C olhando as colunas impressas acima."

chave = F.col(C["protocolo"]) if C["protocolo"] else F.sha2(F.concat_ws("|", *[F.col(c) for c in cols]), 256)
sp = (bz.select(
        chave.cast("string").alias("protocolo"),
        ts_flex(C["abertura"]).alias("data_abertura"),
        (ts_flex(C["fechamento"]) if C["fechamento"] else F.lit(None).cast("timestamp")).alias("data_fechamento"),
        (F.col(C["status"]) if C["status"] else F.lit(None).cast("string")).alias("status"),
        (F.col(C["servico"]) if C["servico"] else F.lit(None).cast("string")).alias("servico"),
        (F.col(C["subpref"]) if C["subpref"] else F.lit(None).cast("string")).alias("subprefeitura"),
        F.col("carregado_em"))
      .withColumn("subprefeitura_norm", norm(F.col("subprefeitura"))))

sp, dup = dedup_por_chave(sp, "protocolo", "carregado_em")
registrar_regra("prata_sp156", "R0_duplicados_entre_lotes_removidos", dup)
print("duplicados entre lotes removidos:", dup)

sp_ok = aplicar_regras(sp.drop("carregado_em"), "prata_sp156", {
    "R1_protocolo_nao_nulo":           F.col("protocolo").isNotNull(),
    "R2_abertura_valida_nao_futura":   F.col("data_abertura").isNotNull() & (F.col("data_abertura") <= F.current_timestamp()),
    "R3_servico_ou_status_preenchido": F.coalesce(F.col("servico"), F.col("status")).isNotNull(),
})
merge_prata(sp_ok, "prata_sp156", ["protocolo"])

# COMMAND ----------
# MAGIC %md ## 2) Chuva diária por região (Open-Meteo devolve listas paralelas: arrays_zip + explode)

# COMMAND ----------
chuva = (spark.table(f"{S}.bronze_chuva")
         .select("regiao_id", "carregado_em",
                 F.explode(F.arrays_zip(F.col("daily.time").alias("t"), F.col("daily.precipitation_sum").alias("p"))).alias("z"))
         .select("regiao_id", F.expr("try_to_date(z.t, 'yyyy-MM-dd')").alias("data"),
                 F.expr("try_cast(z.p as double)").alias("chuva_mm"), "carregado_em")
         .withColumn("k", F.concat_ws("|", "regiao_id", F.col("data").cast("string"))))
chuva, dup = dedup_por_chave(chuva, "k", "carregado_em")
registrar_regra("prata_chuva", "R0_duplicados_entre_lotes_removidos", dup)
chuva_ok = aplicar_regras(chuva.drop("k", "carregado_em"), "prata_chuva", {
    "R1_data_valida":     F.col("data").isNotNull(),
    "R2_chuva_nao_nula":  F.col("chuva_mm").isNotNull(),
    "R3_chuva_0_a_400mm": F.col("chuva_mm").between(0, 400),
})
merge_prata(chuva_ok, "prata_chuva", ["regiao_id", "data"])

# COMMAND ----------
# MAGIC %md ## 3) IBGE - distritos

# COMMAND ----------
ib = (spark.table(f"{S}.bronze_ibge")
      .select(F.col("id").cast("string").alias("distrito_id"), F.col("nome").alias("distrito_nome"),
              F.col("municipio.nome").alias("municipio"), "carregado_em")
      .withColumn("nome_norm", norm(F.col("distrito_nome"))))
ib, dup = dedup_por_chave(ib, "distrito_id", "carregado_em")
registrar_regra("prata_ibge", "R0_duplicados_entre_lotes_removidos", dup)
ib_ok = aplicar_regras(ib.drop("carregado_em"), "prata_ibge", {
    "R1_id_nao_nulo":   F.col("distrito_id").isNotNull(),
    "R2_nome_nao_nulo": F.col("distrito_nome").isNotNull(),
    "R3_municipio_sp":  F.col("municipio") == "São Paulo",
})
merge_prata(ib_ok, "prata_ibge", ["distrito_id"])

# COMMAND ----------
# MAGIC %md ## 4) Ocorrências do MVP (simuladas). Dados pessoais ainda em claro aqui; a proteção acontece na Ouro.

# COMMAND ----------
oc = (spark.table(f"{S}.bronze_ocorrencias")
      .select("id_ocorrencia", "tipo_barreira", "origem", "prioridade", "status", "regiao_id",
              F.expr("try_cast(lat as double)").alias("lat"), F.expr("try_cast(lon as double)").alias("lon"),
              F.expr("try_to_date(data_registro, 'yyyy-MM-dd')").alias("data_registro"),
              F.expr("try_to_date(data_resolucao, 'yyyy-MM-dd')").alias("data_resolucao"),
              "reporter_nome", "reporter_email", F.col("reporter_telefone").cast("string").alias("reporter_telefone"),
              F.expr("try_to_date(data_atualizacao, 'yyyy-MM-dd')").alias("data_atualizacao")))
oc, dup = dedup_por_chave(oc, "id_ocorrencia", "data_atualizacao")
registrar_regra("prata_ocorrencia", "R0_versoes_antigas_e_duplicados_removidos", dup)
print("versões antigas/duplicados removidos:", dup)

TIPOS_OK = ["buraco", "obstaculo", "piso_quebrado", "degrau_sem_rampa", "trecho_escuro"]
oc_ok = aplicar_regras(oc, "prata_ocorrencia", {
    "R1_tipo_barreira_conhecido":            F.col("tipo_barreira").isin(TIPOS_OK),
    "R2_coordenada_dentro_de_sao_paulo":     F.col("lat").between(-24.0, -23.3) & F.col("lon").between(-46.9, -46.3),
    "R3_data_registro_nao_futura":           F.col("data_registro") <= F.current_date(),
    "R4_resolucao_nao_anterior_ao_registro": F.col("data_resolucao").isNull() | (F.col("data_resolucao") >= F.col("data_registro")),
})
merge_prata(oc_ok, "prata_ocorrencia", ["id_ocorrencia"])

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT * FROM workspace.gs.qualidade_prata ORDER BY momento, tabela, regra;
