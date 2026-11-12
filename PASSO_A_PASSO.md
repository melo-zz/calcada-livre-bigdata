# Passo a passo — Big Data (prazo: 19/11 às 23h55)

Hoje é 10/11. O que depende do calendário vem primeiro: **ligar o Airflow hoje**.

## 0. Hoje à noite (30 min)
1. Crie o repositório público no GitHub e suba esta pasta inteira. Troque `SEU_USUARIO/SEU_REPOSITORIO` e `COLE_AQUI` no `README.md`.
2. **Confirme com o grupo os nomes completos** (README pede nome completo; faltar nome/RM custa −0,5 em Big Data e zera o Maker Lab).
3. Instale o Docker Desktop (Windows: WSL2) e o Airflow (passo abaixo). **O computador precisa ficar ligado e o Docker aberto** nos dias seguintes.

## 1. Big Data — Airflow (hoje)
1. Numa pasta `airflow/` fora do repositório: baixe `https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml`, crie as pastas `dags logs plugins config chegada` e o `.env` com `AIRFLOW_UID=50000`.
2. No `docker-compose.yaml`, em `volumes:` do bloco `x-airflow-common`, acrescente `- ${AIRFLOW_PROJ_DIR:-.}/chegada:/opt/airflow/chegada`.
3. Copie `dags/*.py` para `airflow/dags/`.
4. `docker compose up airflow-init`, depois `docker compose up -d`. Abra http://localhost:8080 (`airflow`/`airflow`) e ative o DAG `gs_coleta`.
5. Ele roda 3 dias atrasados (8, 9 e 10/11) e depois todo dia. **Não desligue** até 16/11.
6. Se `coletar_sp156` falhar, abra o log e me mande: o nome do arquivo CSV do SP156 pode precisar da Variable `GS_SP156_CSV_URL` (Admin > Variables; copie o link do CSV mais recente em dados.prefeitura.sp.gov.br, dataset *Dados do SP156*).
7. Print do **Graph** e do **Grid** (execuções verdes) e da pasta `chegada/` (arquivos com data no nome).

## 3. Big Data — Databricks (13 a 15/11)
1. Conta no Databricks Free Edition. Rode `databricks/00_setup.sql` e crie as 4 pastas em `workspace.gs.chegada`.
2. **Lote 1** (13/11): suba os arquivos de `chegada/` até o dia 12 (cada um na pasta da sua fonte). **Lote 2** (15/11 ou depois): os arquivos novos. A regra é "dias de coleta diferentes".
3. `01_bronze.py` com `ETAPA="lote1"`; depois `"lote2"`; depois `"rerun_lote2"` sem subir nada (a contagem não muda). Print do `log_contagens`.
4. `02_prata.py` (print das regras com o total barrado) e `03_ouro.py` (preencha o widget **salt** com uma frase secreta).
5. `04_consultas.sql`: print e interpretação das 5 consultas. Print de `ouro_dim_reportante` (mascarada).
6. Exporte as 2 views em CSV para `painel/data/`, rode `streamlit run painel/app.py` e tire print da tela.
7. Preencha `docs/RELATORIO_TEMPLATE.md`, exporte em **PDF** (com legenda em todo print, na ordem) e inclua o link do repositório.

## 4. Fechamento (18 e 19/11)
1. Rode `python scripts/checar_entrega.py` na raiz (confere nomes/RMs, segredos, arquivos proibidos e notebooks sem saída). Corrija tudo que for ERRO.
2. Abra o repositório numa **janela anônima** (tem que estar público até uma semana depois da entrega).
3. Um integrante envia no Teams de Big Data até **19/11, 23h55** (de preferência até o meio-dia): PDF com o link do repositório.

## O que tira pontos
Nome/RM faltando · chave, senha ou dado pessoal real no repositório (−1,0) · prints sem legenda/fora de ordem (até −1,0) · repositório privado/vazio (zera os itens) · exemplo do enunciado sem adaptar (zera itens 01 e 07 de Big Data).
