# Projeto I – Processo de ETL com Python, PostgreSQL e MongoDB

Programa Fast Track, simulação de pipeline ETL real de extração, transformação e carga de dados.

---

## Visão Geral

O script `etl.py` implementa um pipeline completo de **ETL**:

| Etapa    | O que faz                                                                 |
|----------|---------------------------------------------------------------------------|
| Extract  | Carrega dados de arquivos CSV (tabelas) e do MongoDB (coleção `order_reviews`) |
| Transform| Limpa, une e transforma os dados em um modelo dimensional (estrela)       |
| Load     | Insere os dados transformados no PostgreSQL em tabelas fato e dimensões   |

---

## Ambientes e Bibliotecas Utilizadas

| Biblioteca | Finalidade                                                                 | Paralelo Java/Kotlin                                    |
|------------|----------------------------------------------------------------------------|----------------------------------------------------------|
| `pandas`   | Manipulação de dados tabulares, leitura e transformação de CSVs           | Stream API, Collectors, MapStruct, Apache Spark         |
| `psycopg2` | Conexão e execução de comandos no PostgreSQL                              | JDBC, Spring Data, JPA                                  |
| `pymongo`  | Leitura de documentos no MongoDB                                           | MongoClient, Spring Data MongoDB                        |
| `datetime` | Processamento de datas, como extração de ano/mês/dia                      | java.time.LocalDate                                     |

---

## Arquitetura do Script

Funções principais do `etl.py`:

| Função                    | Responsabilidade                                                          |
|---------------------------|---------------------------------------------------------------------------|
| `create_dimensional_model` | Cria as tabelas no PostgreSQL (dimensões e fato)                         |
| `load_and_transform_data`  | Extrai os dados dos CSVs e do MongoDB                                    |
| `load_dimensions`          | Carrega as tabelas dimensão (`dim_customer`, `dim_product`, etc.)        |
| `load_fact_sales`          | Realiza junções e insere os dados agregados na tabela fato (`fact_sales`)|
| `main()`                   | Orquestra todo o processo, conecta ao banco e fecha as conexões          |

---

## Modelagem Dimensional

Este projeto utiliza a **modelagem estrela**, onde temos:

- **Tabela Fato**: Contém dados quantitativos e chaves estrangeiras
- **Tabelas Dimensão**: Contêm descrições relacionadas às métricas (cliente, produto, data, etc.)

### Esquema utilizado:

```
                  +---------------------+
                  |    dim_customer     |
                  +---------------------+
                             |
                             |
                             |
+------------------+         ▼          +-----------+
| dim_payment_type | --> fact_sales <-- dim_product |
+------------------+         ▲          +-----------+
                             |
                             |
                     +---------------+
                     |   dim_date    |
                     +---------------+
```

**Tabelas criadas:**

- `dim_customer`: dados dos clientes
- `dim_product`: dados dos produtos
- `dim_date`: data da venda (granularidade temporal)
- `dim_payment_type`: forma de pagamento
- `fact_sales`: dados consolidados (price, freight, etc.)

---

## Sequência da Execução

1. `load_and_transform_data()`  
   - Lê os arquivos `.csv`:
     - `olist_customers_dataset.csv`
     - `olist_orders_dataset.csv`
     - `olist_order_items_dataset.csv`
     - `olist_products_dataset.csv`
     - `olist_order_payments_dataset.csv`
   - Lê a coleção `order_reviews` do MongoDB

2. `create_dimensional_model()`  
   - Cria as tabelas no PostgreSQL com chaves primárias e estrangeiras

3. `load_dimensions()`  
   - Remove duplicatas com `drop_duplicates()`
   - Insere os dados com `INSERT ... ON CONFLICT DO NOTHING`

4. `load_fact_sales()`  
   - Realiza merges entre os DataFrames
   - Busca as chaves estrangeiras (IDs de dimensão)
   - Insere os dados finais na `fact_sales`

---

## Arquivos Utilizados

- `etl.py`: script principal
- `Dockerfile`: imagem personalizada para rodar o script Python
- `docker-compose.yml`: orquestração dos serviços (Python, PostgreSQL, MongoDB)
- `requirements.txt`: bibliotecas Python necessárias
- Pasta `input/`: contém os arquivos `.csv` (não incluídos no repositório)

---

## Execução com Docker Compose

Este projeto roda inteiramente com Docker Compose:

```bash
docker-compose up --build
```

O script `etl.py` é executado automaticamente ao subir o container Python. Ele extrai, transforma e carrega os dados nos bancos PostgreSQL e MongoDB.

---

## Visualização dos Dados

Após a execução, você pode visualizar os dados usando:

- **PostgreSQL**:
  - Use **DBeaver**, **pgAdmin** ou qualquer cliente SQL para explorar o schema e as tabelas.
  - URI: localhost
  - porta: 5432
  - banco: pb_dw
  - usuário: postgres
  - senha: postgres

- **MongoDB**:
  - Use **MongoDB Compass**, **NoSQLBooster**, ou qualquer GUI compatível com MongoDB para visualizar os documentos brutos da coleção `order_reviews`.
  - URI: mongodb://localhost:27017
  - banco: ecommerce
  - coleção: order_reviews

---