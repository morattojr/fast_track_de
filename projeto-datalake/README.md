
# Ingestão de Dados para o Data Lake (Camada Raw)

Este diretório contém o script responsável por realizar a **ingestão dos dados da fonte local para a camada Raw do Data Lake**, em modo batch.

## Diretório `data/input`

A pasta `data/input`, localizada na raiz do projeto (`fast_track_de/data/input`), representa a **fonte de dados local**. Nela está o arquivo `movies.csv` compactado em formato .zip `movies.zip`, e contém a base de dados de filmes a ser carregada.

## Objetivo do Script

O script `ingestao_raw.py` tem como propósito:

- Descompactar o arquivo `movies.zip`
- Localizar o arquivo `movies.csv`
- Carregar esse arquivo **sem qualquer transformação** para a camada `Raw` do data lake local, seguindo a seguinte estrutura de diretório:

```

data-lake/Raw/Local/CSV/Movies/YYYY/MM/DD/movies.csv

````

Essa estrutura segue um padrão de versionamento e organização por data de processamento.

## Execução com Docker

O script é executado automaticamente dentro do container Docker assim que o serviço `etl` é iniciado via `docker-compose`. O container está configurado para montar os volumes necessários e executar o script `ingestao_raw.py`.

### Comando para execução:

```bash
docker-compose up --build
````

---

## Estrutura de Diretórios Esperada

```
fast_track_de/
├── data/
│   └── input/
│       └── movies.csv         ← Fonte de dados
├── projeto-datalake/
│   ├── scripts/
│   │   └── ingestao_raw.py      ← Script de ingestão
│   ├── docker/
│   │   └── Dockerfile
│   └── docker-compose.yml     ← Define o serviço ETL
```

---

## Resultado Esperado

Após execução bem-sucedida, o arquivo `movies.csv` será copiado para:

```
fast_track_de/
└── data-lake/
    └── Raw/
        └── Local/
            └── CSV/
                └── Movies/
                    └── 2025/
                        └── 06/
                            └── 07/
                                └── movies.csv
```

---

## Observação

* O script **não altera os dados** nem realiza pré-processamento.
* A separação do diretório `data/input` fora de `projeto-datalake` segue boas práticas de organização, separando claramente a **fonte externa** da **estrutura do data lake**.

```
