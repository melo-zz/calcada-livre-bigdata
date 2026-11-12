# Relatório (PDF) — Big Data Architecture & Data Integration
Cole no Google Docs/Word, insira os prints e exporte em **PDF**. Mantenha **esta ordem**; todo print precisa de legenda e ser legível.

## 01. Integrantes
| Nome completo | RM |
|---|---|
| Carlos Ganzerli | 99840 |
| Paulo Lopes | 551137 |
| Lucas Bandeira | 98640 |
| Gustavo Melo Namura | 98809 |

## 02. O produto e as perguntas do MVP
**Produto:** Calçada Livre, mapa de barreiras de acessibilidade nas calçadas de São Paulo.
**Perguntas:** (1) Onde há mais barreiras abertas? (2) Quanto a prefeitura demora para resolver? (3) A chuva aumenta os pedidos de reparo de calçada? (4) Que tipos de barreira são mais comuns? (5) Que % é resolvido em até 30 dias?

## 03. Diagrama do fluxo de dados
Insira o diagrama do `README.md` (Fontes → Airflow → chegada/ → Volume → Bronze → Prata → Ouro → MVP). Dica: abra o README no GitHub e tire print do diagrama.

## 04. Tabela de fontes
| Origem | Formato | Frequência |
|---|---|---|
| SP156, pedidos de calçada (dados abertos da Prefeitura) | CSV → JSON | diária |
| Open-Meteo, chuva diária (API) | JSON | diária |
| IBGE, distritos (API) | JSON | diária |
| Ocorrências do MVP (simuladas, justificativa no README) | JSON | diária |

## 05. Requisitos 01 a 09 (cada um com print + explicação curta)
| Req. | O que mostrar no print | Explicação (2–3 linhas) |
|---|---|---|
| 01 | As APIs/CSV abertos no navegador ou Postman (SP156, Open-Meteo, IBGE) | ... |
| 02 | Código do DAG + pasta `chegada/` com arquivos com a data no nome | ... |
| 03 | Graph do DAG (4 tarefas em paralelo + `conferir_chegada`) e Admin > Variables | ... |
| 04 | Volume do Databricks com arquivos dos 2 lotes (datas diferentes) | ... |
| 05 | Bronze: Auto Loader e a tabela `log_contagens` | ... |
| 06 | Prata: saída das regras de qualidade com o total barrado + MERGE | ... |
| 07 | Ouro: fato e dimensões + as 5 consultas | ... |
| 08 | `ouro_dim_reportante` mascarada + tela do painel + a view que ela usa (`ouro_vw_mvp_mapa`) | ... |
| 09 | Repositório público no GitHub com o README | ... |

## 06. Prints do Airflow
Visão **Graph** e visão **Grid** com execuções concluídas (verdes).

## 07. Contagens da Bronze
Preencha a partir de `SELECT * FROM workspace.gs.log_contagens ORDER BY momento, tabela`:
| Momento | Tabela | Contagem |
|---|---|---|
| antes do lote 2 | bronze_ocorrencias | ... |
| depois do lote 2 | bronze_ocorrencias | ... (só os novos entraram) |
| depois da 2ª execução (rerun) | bronze_ocorrencias | ... (igual: sem duplicar) |

## 08. As 5 consultas SQL
Para cada Q1–Q5: SQL, print do resultado e **interpretação** (o que o número diz sobre barreiras, tempo ou impacto). Se a Q3 ficar fraca por poucos dias de dados, diga isso.

## 09. Conclusão técnica e link do repositório
O que funcionou, limitações (ocorrências simuladas; upload manual no Free Edition; poucos dias de coleta) e próximos passos. **Repositório:** COLE_AQUI
