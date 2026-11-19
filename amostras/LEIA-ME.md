# Amostras
Um pouco de cada fonte, no mesmo formato dos arquivos que o DAG grava em `chegada/`:
- `sp156_2026-11-08.json`: 3 linhas de pedidos de calçada do portal de dados abertos da Prefeitura (fonte **real**). Removemos `numero` e `cep` da amostra por cautela; a base do SP156 não tem nome nem contato de quem pediu.
- `chuva_2026-11-08.json`: 2 regiões da API Open-Meteo (fonte **real**), com a chuva diária.
- `ibge_2026-11-08.json`: 3 distritos da API de localidades do IBGE (fonte **real**).
- `ocorrencias_amostra.json`: 5 linhas da fonte simulada (ocorrências do MVP). Dados pessoais **fictícios**: nomes genéricos, e-mail em `exemplo.com.br`, telefone de faixa fictícia.
