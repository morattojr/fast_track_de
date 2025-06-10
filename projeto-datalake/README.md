# Pipeline FastTrack

Pipeline para ingestão, enriquecimento, transformação e validação de dados de filmes, usando Python, Spark e PostgreSQL em contêineres Docker.

## Visão Geral

A pipeline processa dados de filmes para criar tabelas analíticas (`dim_movie` e `fact_financials`) com informações de filmes lançados entre 2015 e 2022. As etapas são:

1. **Ingestão Raw**: Extrai dados de um arquivo ZIP e armazena como CSV.
2. **Enriquecimento TMDB**: Enriquece com dados da API TMDB, aplicando filtros.
3. **Trusted**: Transforma dados brutos em formatos confiáveis (CSV e Parquet).
4. **Refined**: Cria tabelas dimensionais.
5. **Validação**: Verifica integridade e qualidade.

Dados são armazenados em `/app/data-lake/{Camada}/{Fonte}/{Formato}/{Ano}/{Mês}/{Dia}`.

## Etapas da Pipeline

### 1. Ingestão Raw

- **Descrição**: Extrai dados de filmes de um arquivo ZIP e armazena como CSV na camada Raw.
- **Fonte**: `data/input/movies.zip` (CSV com `id`, `tituloPincipal`, `anoLancamento`, etc.).
- **Processamento**: Descompacta o ZIP, copia o CSV para o data lake, registra status no PostgreSQL (`pipeline_status`).
- **Saída**: `/app/data-lake/Raw/Local/CSV/Movies/YYYY/MM/DD/movies.csv` (separado por `|`).
- **Ferramentas**: Python, `zipfile`.

### 2. Enriquecimento TMDB

- **Descrição**: Enriquece os dados do CSV com informações da API TMDB, aplicando filtros.
- **Fontes**:
  - CSV: `/app/data-lake/Raw/Local/CSV/Movies/YYYY/MM/DD/movies.csv`.
  - API TMDB: Endpoints `/find/{imdb_id}` e `/movie/{tmdb_id}`.
- **Processamento**:
  - Lê CSV com pandas (`id`, `anoLancamento`).
  - Validações:
    - Remove IDs duplicados (mantém primeira ocorrência).
    - Rejeita IDs sem prefixo `tt`.
    - Rejeita `anoLancamento` nulo, vazio (`\N`) ou não numérico.
    - Filtra filmes de 2015 a 2022.
  - Consulta TMDB para cada ID, usando cache (`tmdb_cache.json`).
  - Processa paralelamente com `ThreadPoolExecutor` (4 workers).
  - **Filtros**:
    - Ano: Confirma 2015-2022 na `release_date` da TMDB.
    - Financeiros: Com `FILTER_FINANCIAL=True`, rejeita filmes com `revenue` e `budget` iguais a `0.0` ou `None` (exige pelo menos um valor válido).
    - Erros: Exclui IDs com falhas na API (ex.: "Não encontrado").
  - Gera JSONs com até 100 registros cada (`MAX_FILE_SIZE_MB=10MB`).
  - Registra erros em `tmdb_errors.json`, debug em `filter_debug.json`.
- **Saída**:
  - JSONs: `/app/data-lake/Raw/API/JSON/TMDB/YYYY/MM/DD/tmdb_enriched_*.json` (ex.: `tmdb_enriched_001.json`).
  - Auxiliares: `tmdb_cache.json`, `tmdb_errors.json`, `filter_debug.json`.
- **Ferramentas**: Python, pandas, requests, ThreadPoolExecutor, psycopg2.

### 3. Trusted

- **Descrição**: Transforma dados brutos em formatos confiáveis, com limpezas adicionais.
- **Fontes**:
  - CSV: `/app/data-lake/Raw/Local/CSV/Movies/YYYY/MM/DD/`.
  - JSON: `/app/data-lake/Raw/API/JSON/TMDB/YYYY/MM/DD/tmdb_enriched_*.json`.
- **Processamento**:
  - **CSV**: Lê com Spark, converte `anoLancamento` para `integer`, salva como Parquet.
  - **TMDB**: Lê JSONs, remove registros com `erro`, confirma 2015-2022, extrai `year`, `month`, `day`, salva como Parquet.
  - Verifica dependências no PostgreSQL.
- **Saída**:
  - Parquet: `/app/data-lake/Trusted/CSV/Movies/YYYY/MM/DD/`, `/app/data-lake/Trusted/TMDB/YYYY/MM/DD/`.
- **Ferramentas**: Spark, Python, psycopg2.

### 4. Refined (Modelagem Dimensional)

- **Descrição**: Cria tabelas `dim_movie` e `fact_financials` para análise.
- **Fontes**: Parquet da camada Trusted.
- **Processamento**:
  - **dim_movie**: Combina CSV e TMDB (`movie_id`, `title`, `genre_ids`, `vote_average`, etc.).
  - **fact_financials**: Extrai `movie_id`, `revenue`, `budget`, `processing_date`.
  - Salva como Parquet.
- **Saída**:
  - Parquet: `/app/data-lake/Refined/Dim/Movies/YYYY/MM/DD/`, `/app/data-lake/Refined/Fact/Financials/YYYY/MM/DD/`.
- **Ferramentas**: Spark, Python, psycopg2.

### 5. Validação

- **Descrição**: Verifica integridade e qualidade das tabelas.
- **Fontes**: Parquet da camada Refined.
- **Processamento**:
  - Conta registros em `dim_movie` e `fact_financials`.
  - Verifica anos (2015-2022).
  - Confirma integridade referencial (`movie_id` em `fact_financials` existe em `dim_movie`).
- **Saída**: Atualiza `pipeline_status` no PostgreSQL.
- **Ferramentas**: Spark, Python, psycopg2.

## Configuração

- **Requisitos**:
  - Docker e Docker Compose.
  - Arquivos fornecidos no repositório:
    - `docker-compose.yml`
    - `data/input/movies.zip`
    - `.env.example`
- **Preparação**:
  1. Copie `.env.example` para `.env`:
     ```bash
     cp .env.example .env
     ```
  2. Edite `.env`, preenchendo `TMDB_API_KEY` com sua chave da API TMDB.

## Executando

1. **Executar a pipeline**:
   ```bash
   docker-compose up --build
   ```

2. **Monitorar logs**:
   ```bash
   docker logs -f fasttrack_pipeline
   ```

3. **Verificar status**:
   ```bash
   docker exec -it postgres_db psql -U sa -d nprod_db -c "SELECT * FROM pipeline_status;"
   ```

4. **Reiniciar (limpa volumes)**:
   ```bash
   docker-compose down -v
   ```

## Observações

- **Performance**: Para grandes volumes, ajuste a memória do Spark em `docker-compose.yml` (ex.: `--executor-memory 6g`).
- **Dados Financeiros**: `FILTER_FINANCIAL=True` rejeita filmes sem `revenue` ou `budget`. Use `False` em `.env` para incluir todos.
- **Debug**: Consulte `tmdb_errors.json` e `filter_debug.json` para rejeições ou erros.