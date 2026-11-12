# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Ouro (modelo dimensional + LGPD + views do MVP)
# MAGIC **Fatos:** `ouro_fato_ocorrencia` (1 linha por ocorrência), `ouro_fato_sp156_dia`, `ouro_fato_chuva_dia`.
# MAGIC **Dimensões:** `ouro_dim_tempo`, `ouro_dim_local`, `ouro_dim_tipo_barreira`, `ouro_dim_reportante` (dados pessoais mascarados).

# COMMAND ----------
dbutils.widgets.text("salt", "", "Salt do hash (NAO commitar)")
SALT = dbutils.widgets.get("salt")
assert SALT, "Preencha o widget 'salt' (no topo do notebook) com uma frase secreta e rode de novo."
S = "workspace.gs"
TR = "'áàâãäéèêëíìîïóòôõöúùûüç', 'aaaaaeeeeiiiiooooouuuuc'"

# COMMAND ----------
# MAGIC %md ## Dimensões

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {S}.ouro_dim_local AS
SELECT r.regiao_id, r.regiao_nome, r.subprefeitura, r.distrito_ibge, r.lat, r.lon, i.distrito_id AS ibge_distrito_id
FROM (SELECT DISTINCT regiao_id, regiao_nome, subprefeitura, distrito_ibge, lat, lon FROM {S}.bronze_chuva) r
LEFT JOIN {S}.prata_ibge i ON i.nome_norm = translate(lower(r.distrito_ibge), {TR})
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {S}.ouro_dim_tipo_barreira AS
SELECT * FROM VALUES
 ('buraco','Buraco na calçada','mobilidade',3),
 ('obstaculo','Obstáculo no passeio (lixeira, veículo, entulho)','mobilidade',2),
 ('piso_quebrado','Piso quebrado ou irregular','mobilidade',2),
 ('degrau_sem_rampa','Degrau sem rampa','acessibilidade',3),
 ('trecho_escuro','Trecho sem iluminação','seguranca',2)
AS t(tipo_barreira, descricao, categoria, risco_base)
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {S}.ouro_dim_tempo AS
WITH lim AS (
  SELECT least(min(data_registro), (SELECT min(data) FROM {S}.prata_chuva)) AS ini,
         greatest(max(data_registro), (SELECT max(data) FROM {S}.prata_chuva)) AS fim
  FROM {S}.prata_ocorrencia)
SELECT d AS data, year(d) AS ano, month(d) AS mes, day(d) AS dia, date_format(d, 'EEEE') AS dia_semana
FROM lim LATERAL VIEW explode(sequence(ini, fim, interval 1 day)) x AS d
""")

# COMMAND ----------
# MAGIC %md ## LGPD: o reportante (nome, e-mail, telefone) só existe mascarado ou com hash na Ouro

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {S}.ouro_dim_reportante AS
SELECT sha2(concat(lower(reporter_email), '{SALT}'), 256)                                                  AS id_reportante,
       any_value(concat(substr(reporter_nome, 1, 1), repeat('*', greatest(length(reporter_nome) - 1, 0)))) AS nome_mascarado,
       any_value(concat(substr(reporter_email, 1, 1), '***@', split(reporter_email, '@')[1]))              AS email_mascarado,
       any_value(concat('(**) *****-', substr(reporter_telefone, 8, 4)))                                   AS telefone_mascarado,
       any_value(sha2(concat(reporter_telefone, '{SALT}'), 256))                                           AS telefone_hash
FROM {S}.prata_ocorrencia GROUP BY sha2(concat(lower(reporter_email), '{SALT}'), 256)
""")

# COMMAND ----------
# MAGIC %md ## Fatos

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {S}.ouro_fato_ocorrencia AS
SELECT o.id_ocorrencia, o.regiao_id, o.tipo_barreira, o.origem, o.prioridade, o.status,
       sha2(concat(lower(o.reporter_email), '{SALT}'), 256) AS id_reportante,
       o.data_registro, o.data_resolucao, o.lat, o.lon,
       datediff(o.data_resolucao, o.data_registro) AS dias_ate_resolucao
FROM {S}.prata_ocorrencia o
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {S}.ouro_fato_sp156_dia AS
SELECT to_date(data_abertura) AS data, coalesce(subprefeitura, 'NAO INFORMADA') AS subprefeitura,
       count(*) AS qtd_pedidos,
       sum(CASE WHEN data_fechamento IS NOT NULL THEN 1 ELSE 0 END) AS qtd_fechados,
       round(avg(datediff(data_fechamento, data_abertura)), 1) AS tempo_medio_dias
FROM {S}.prata_sp156 GROUP BY to_date(data_abertura), coalesce(subprefeitura, 'NAO INFORMADA')
""")

spark.sql(f"CREATE OR REPLACE TABLE {S}.ouro_fato_chuva_dia AS SELECT regiao_id, data, chuva_mm FROM {S}.prata_chuva")

# COMMAND ----------
# MAGIC %md ## Contrato com o MVP (sem nenhum dado pessoal)

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE VIEW {S}.ouro_vw_mvp_mapa AS
SELECT f.id_ocorrencia, l.regiao_nome AS local, l.subprefeitura, t.descricao AS tipo, f.tipo_barreira, f.origem,
       f.prioridade, f.status, f.data_registro, f.dias_ate_resolucao, f.lat, f.lon
FROM {S}.ouro_fato_ocorrencia f
JOIN {S}.ouro_dim_local l ON l.regiao_id = f.regiao_id
JOIN {S}.ouro_dim_tipo_barreira t ON t.tipo_barreira = f.tipo_barreira
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {S}.ouro_vw_mvp_indicadores AS
WITH ref AS (SELECT max(data_registro) AS max_data FROM {S}.ouro_fato_ocorrencia)
SELECT l.regiao_nome AS local,
       count(*) AS ocorrencias,
       sum(CASE WHEN f.status <> 'resolvida' THEN 1 ELSE 0 END) AS abertas,
       round(avg(f.dias_ate_resolucao), 1) AS tempo_medio_dias,
       round(100 * sum(CASE WHEN f.data_registro <= date_sub(ref.max_data, 30) AND f.dias_ate_resolucao <= 30 THEN 1 ELSE 0 END)
             / nullif(sum(CASE WHEN f.data_registro <= date_sub(ref.max_data, 30) THEN 1 ELSE 0 END), 0), 1) AS pct_resolvidas_30d
FROM {S}.ouro_fato_ocorrencia f JOIN {S}.ouro_dim_local l ON l.regiao_id = f.regiao_id CROSS JOIN ref
GROUP BY l.regiao_nome
""")

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT * FROM workspace.gs.ouro_dim_reportante LIMIT 10;   -- print da proteção LGPD
