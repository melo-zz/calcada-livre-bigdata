-- 5 consultas da Ouro que respondem perguntas do MVP. Rode, tire print e escreva a interpretação.

-- Q1. Quais locais concentram mais barreiras reportadas (e quantas seguem abertas)?
SELECT l.regiao_nome AS local, count(*) AS barreiras,
       sum(CASE WHEN f.status <> 'resolvida' THEN 1 ELSE 0 END) AS ainda_abertas
FROM workspace.gs.ouro_fato_ocorrencia f JOIN workspace.gs.ouro_dim_local l USING (regiao_id)
GROUP BY l.regiao_nome ORDER BY barreiras DESC;

-- Q2. Tempo médio de atendimento (dias) por local, só ocorrências resolvidas.
SELECT l.regiao_nome AS local, count(*) AS resolvidas, round(avg(f.dias_ate_resolucao), 1) AS dias_medios
FROM workspace.gs.ouro_fato_ocorrencia f JOIN workspace.gs.ouro_dim_local l USING (regiao_id)
WHERE f.dias_ate_resolucao IS NOT NULL
GROUP BY l.regiao_nome ORDER BY dias_medios DESC;

-- Q3. Chuva x demanda real: pedidos de calçada no SP156 por faixa de chuva média do dia (dados reais).
WITH chuva AS (SELECT data, avg(chuva_mm) AS chuva_media FROM workspace.gs.ouro_fato_chuva_dia GROUP BY data),
     sp AS (SELECT data, sum(qtd_pedidos) AS pedidos FROM workspace.gs.ouro_fato_sp156_dia GROUP BY data)
SELECT CASE WHEN c.chuva_media >= 10 THEN '3. chuva forte (>= 10 mm)'
            WHEN c.chuva_media >= 1  THEN '2. chuva leve (1 a 10 mm)'
            ELSE '1. sem chuva (< 1 mm)' END AS faixa_chuva,
       count(*) AS dias, round(avg(s.pedidos), 1) AS media_pedidos_por_dia
FROM chuva c JOIN sp s USING (data) GROUP BY 1 ORDER BY 1;

-- Q4. Quais tipos de barreira são mais comuns em cada origem (app, carrinho, visão)?
SELECT f.origem, t.descricao AS tipo, count(*) AS qtd
FROM workspace.gs.ouro_fato_ocorrencia f JOIN workspace.gs.ouro_dim_tipo_barreira t USING (tipo_barreira)
GROUP BY f.origem, t.descricao ORDER BY f.origem, qtd DESC;

-- Q5 (IMPACTO). % de barreiras resolvidas em até 30 dias, por local (só ocorrências com >= 30 dias de idade).
WITH ref AS (SELECT max(data_registro) AS max_data FROM workspace.gs.ouro_fato_ocorrencia)
SELECT l.regiao_nome AS local, count(*) AS elegiveis,
       sum(CASE WHEN f.dias_ate_resolucao <= 30 THEN 1 ELSE 0 END) AS resolvidas_ate_30d,
       round(100 * sum(CASE WHEN f.dias_ate_resolucao <= 30 THEN 1 ELSE 0 END) / count(*), 1) AS pct_resolvidas_30d
FROM workspace.gs.ouro_fato_ocorrencia f
JOIN workspace.gs.ouro_dim_local l USING (regiao_id) CROSS JOIN ref
WHERE f.data_registro <= date_sub(ref.max_data, 30)
GROUP BY l.regiao_nome ORDER BY pct_resolvidas_30d ASC;
