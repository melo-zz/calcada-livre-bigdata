-- Rode uma vez (Databricks Free Edition, catálogo "workspace").
CREATE SCHEMA IF NOT EXISTS workspace.gs;
CREATE VOLUME IF NOT EXISTS workspace.gs.chegada;
-- Depois, em Catalog > workspace > gs > chegada, crie as pastas: sp156, chuva, ibge, ocorrencias

CREATE TABLE IF NOT EXISTS workspace.gs.log_contagens (momento TIMESTAMP, etapa STRING, tabela STRING, qtd BIGINT);
CREATE TABLE IF NOT EXISTS workspace.gs.qualidade_prata (momento TIMESTAMP, tabela STRING, regra STRING, barrados BIGINT);
