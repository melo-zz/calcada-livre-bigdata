# Amostras
- `ocorrencias_amostra.json`: 5 linhas da fonte simulada (dados pessoais FICTÍCIOS: nomes genéricos, e-mail em `exemplo.com.br`, telefone de faixa fictícia).
- Depois que o Airflow rodar, copie para cá 2 ou 3 linhas de cada arquivo de `chegada/`: `sp156_AAAA-MM-DD.json`, `chuva_AAAA-MM-DD.json` e `ibge_AAAA-MM-DD.json` (`head -n 3 arquivo > amostras/arquivo`).
- Confira que as linhas do SP156 não têm dados pessoais reais antes de commitar (o portal já os remove, mas confira).
