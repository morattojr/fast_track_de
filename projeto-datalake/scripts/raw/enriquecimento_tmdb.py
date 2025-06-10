import os
import json
import time
import logging
import requests
import psycopg2
from psycopg2 import OperationalError
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from collections import Counter
import pandas as pd
import numpy as np

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Verificar versão do pandas
if pd.__version__ < '1.5.0':
    logging.error("Versão do pandas inferior a 1.5.0. Atualize para 2.2.3.")
    exit(1)

def get_env_var(name):
    value = os.getenv(name)
    if not value:
        logging.error(f"A variável de ambiente {name} não está definida.")
        exit(1)
    return value

# Carrega variáveis do .env
env_path = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(dotenv_path=env_path)

# Variáveis obrigatórias
TMDB_API_KEY = get_env_var("TMDB_API_KEY")
INPUT_CSV_DIR = get_env_var("INPUT_CSV_DIR")
OUTPUT_BASE_DIR = get_env_var("OUTPUT_BASE_DIR")
DB_HOST = get_env_var("DB_HOST")
DB_PORT = get_env_var("DB_PORT")
DB_NAME = get_env_var("DB_NAME")
DB_USER = get_env_var("DB_USER")
DB_PASSWORD = get_env_var("DB_PASSWORD")

# Variáveis opcionais
max_files_env = os.getenv("MAX_JSON_FILES", "ALL")
MAX_JSON_FILES = None if max_files_env.upper() == "ALL" or max_files_env == "0" else int(max_files_env)
FILTER_FINANCIAL = os.getenv("FILTER_FINANCIAL", "TRUE").upper() == "TRUE"

# Data de processamento
hoje = datetime.now()
ano, mes, dia = hoje.strftime("%Y"), hoje.strftime("%m"), hoje.strftime("%d")

INPUT_CSV = os.path.join(INPUT_CSV_DIR, ano, mes, dia, "movies.csv")
OUTPUT_DIR = os.path.join(OUTPUT_BASE_DIR, ano, mes, dia)
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# Arquivos de debug
DEBUG_FILE = os.path.join(OUTPUT_DIR, "filter_debug.json")
CACHE_FILE = os.path.join(OUTPUT_DIR, "tmdb_cache.json")
ERROR_LOG_FILE = os.path.join(OUTPUT_DIR, "tmdb_errors.json")

# API TMDB
TMDB_FIND_URL = "https://api.themoviedb.org/3/find/{}?external_source=imdb_id&api_key={}"
TMDB_MOVIE_URL = "https://api.themoviedb.org/3/movie/{}?api_key={}"
MAX_RECORDS = 100
MAX_FILE_SIZE_MB = 10
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
session = requests.Session()

# Configuração de paralelismo
MAX_WORKERS = 4
REQUESTS_PER_10S = 40
SLEEP_TIME = 0.25
count_lock = Lock()
request_lock = Lock()
last_requests = []
count = 0

# Limites de validação
MIN_IDS = 1000
MAX_IDS = 10000

def conectar_postgres_com_retentativas(tentativas=10, intervalo=3):
    for i in range(tentativas):
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            logging.info("[OK] Conectado ao banco de dados.")
            return conn
        except OperationalError as e:
            logging.warning(f"[{i+1}/{tentativas}] Banco não disponível. Retentando em {intervalo}s... {e}")
            time.sleep(intervalo)
    logging.error("Não foi possível conectar ao banco após múltiplas tentativas.")
    return None

def etapa_ja_executada(conn, etapa, data_processamento):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM pipeline_status
            WHERE etapa = %s AND data_processamento = %s AND status = 'COMPLETED'
        """, (etapa, data_processamento))
        return cur.fetchone() is not None

def registrar_status(conn, etapa, data_processamento, status):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline_status (etapa, data_processamento, status, updated_at)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (etapa, data_processamento)
            DO UPDATE SET status = EXCLUDED.status, updated_at = EXCLUDED.updated_at
        """, (etapa, data_processamento, status))
        conn.commit()

def ingestao_concluida(conn, data_processamento):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM pipeline_status
            WHERE etapa = 'ingestao_raw' AND data_processamento = %s AND status = 'COMPLETED'
        """, (data_processamento,))
        return cur.fetchone() is not None

def aguardar_ingestao_concluida(conn, data_processamento, max_tentativas=30, intervalo=10):
    for tentativa in range(1, max_tentativas + 1):
        logging.info(f"Verificando status da etapa 'ingestao_raw'... [Tentativa {tentativa}/{max_tentativas}]")
        if ingestao_concluida(conn, data_processamento):
            logging.info(f"Ingestão concluída detectada na tentativa {tentativa}. Prosseguindo.")
            return True
        logging.info(f"Ingestão ainda não concluída. Aguardando {intervalo}s...")
        time.sleep(intervalo)
    logging.error("Tempo máximo de espera excedido. Ingestão não foi concluída.")
    return False

def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            logging.info(f"Cache carregado de: {CACHE_FILE}")
            return json.load(f)
    logging.info("Nenhum cache encontrado. Iniciando vazio.")
    return {}

def save_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False)
    logging.info(f"Cache salvo em: {CACHE_FILE}")

def log_error(imdb_id, error_msg):
    error_entry = {"imdb_id": imdb_id, "error": error_msg, "timestamp": str(datetime.now())}
    errors = []
    if os.path.exists(ERROR_LOG_FILE):
        try:
            with open(ERROR_LOG_FILE, "r", encoding="utf-8") as f:
                errors = json.load(f)
        except json.JSONDecodeError:
            logging.warning("Arquivo de log de erros corrompido. Reiniciando.")
    errors.append(error_entry)
    with open(ERROR_LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(errors, f, ensure_ascii=False)
    logging.info(f"Erro registrado para IMDb ID {imdb_id}: {error_msg}")

def save_debug_info(debug_data):
    with open(DEBUG_FILE, "w", encoding="utf-8") as f:
        json.dump(debug_data, f, ensure_ascii=False, indent=2)
    logging.info(f"Informações de debug salvas em: {DEBUG_FILE}")

def respect_rate_limit():
    with request_lock:
        current_time = time.time()
        global last_requests
        last_requests = [t for t in last_requests if current_time - t < 10]
        if len(last_requests) >= REQUESTS_PER_10S:
            sleep_time = 10 - (current_time - last_requests[0])
            if sleep_time > 0:
                logging.debug(f"Aguardando {sleep_time:.2f}s para respeitar limite de taxa.")
                time.sleep(sleep_time)
        last_requests.append(current_time)

def fetch_tmdb_data(imdb_id, cache, retries=3):
    if imdb_id in cache:
        logging.debug(f"IMDb ID {imdb_id} encontrado no cache.")
        return cache[imdb_id]
    
    url_find = TMDB_FIND_URL.format(imdb_id, TMDB_API_KEY)
    for attempt in range(1, retries + 1):
        try:
            respect_rate_limit()
            response = session.get(url_find)
            response.raise_for_status()
            time.sleep(SLEEP_TIME)
            find_data = response.json()
            movie_results = find_data.get("movie_results", [])
            if not movie_results:
                logging.info(f"IMDb ID {imdb_id} não retornou resultados no TMDB.")
                log_error(imdb_id, "Não encontrado na TMDB")
                cache[imdb_id] = None
                return None
            tmdb_id = movie_results[0].get("id")
            respect_rate_limit()
            url_movie = TMDB_MOVIE_URL.format(tmdb_id, TMDB_API_KEY)
            response = session.get(url_movie)
            response.raise_for_status()
            movie_data = response.json()
            time.sleep(SLEEP_TIME)
            cache[imdb_id] = movie_data
            return movie_data
        except requests.RequestException as e:
            logging.warning(f"Tentativa {attempt} falhou para ID {imdb_id}: {e}")
            if attempt == retries:
                logging.error(f"Erro definitivo para ID {imdb_id}")
                log_error(imdb_id, f"Erro na API: {e}")
                cache[imdb_id] = None
                return None
            time.sleep(2)

def extrair_info_relevante(tmdb_response, imdb_id):
    if not tmdb_response:
        return {"imdb_id": imdb_id, "erro": "Não encontrado no TMDB"}

    release_year = tmdb_response.get("release_date", "")[:4]
    if release_year.isdigit() and not (2015 <= int(release_year) <= 2022):
        logging.info(f"IMDb ID {imdb_id} fora do período 2015-2022. Release date: {tmdb_response.get('release_date')}. Ignorando.")
        return None
    enriched = {
        "imdb_id": imdb_id,
        "id": tmdb_response.get("id"),
        "title": tmdb_response.get("title"),
        "original_title": tmdb_response.get("original_title"),
        "overview": tmdb_response.get("overview"),
        "release_date": tmdb_response.get("release_date"),
        "genre_ids": [genre["id"] for genre in tmdb_response.get("genres", [])],
        "popularity": tmdb_response.get("popularity"),
        "vote_average": tmdb_response.get("vote_average"),
        "original_language": tmdb_response.get("original_language"),
        "runtime": tmdb_response.get("runtime"),
        "revenue": tmdb_response.get("revenue"),
        "budget": tmdb_response.get("budget"),
        "erro": None
    }
    if FILTER_FINANCIAL and (enriched.get("revenue") in [None, 0.0] and enriched.get("budget") in [None, 0.0]):
        logging.info(f"IMDb ID {imdb_id} sem dados financeiros válidos. Ignorando.")
        return None
    if enriched.get("revenue") not in [None, 0.0] or enriched.get("budget") not in [None, 0.0]:
        logging.debug(f"IMDb ID {imdb_id} com dados financeiros: revenue={enriched.get('revenue')}, budget={enriched.get('budget')}")
    return enriched

def process_imdb_id(imdb_id, cache):
    raw_data = fetch_tmdb_data(imdb_id, cache)
    enriched = extrair_info_relevante(raw_data, imdb_id)
    return imdb_id, enriched

def save_json_chunk(data_chunk, index):
    file_path = os.path.join(OUTPUT_DIR, f"tmdb_enriched_{index:03}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data_chunk, f, ensure_ascii=False, indent=2)
    logging.info(f"{len(data_chunk)} registros salvos em: {file_path}")

def main():
    etapa = "enriquecimento_tmdb"
    data_processamento = datetime.now().date()

    logging.info("Início do Enriquecimento TMDB")

    conn = conectar_postgres_com_retentativas()
    if not conn:
        logging.error("Erro crítico de conexão com o banco. Encerrando.")
        exit(1)

    if not aguardar_ingestao_concluida(conn, data_processamento):
        conn.close()
        exit(1)

    if etapa_ja_executada(conn, etapa, data_processamento):
        logging.info(f"Etapa '{etapa}' já concluída em {data_processamento}. Encerrando.")
        conn.close()
        return

    registrar_status(conn, etapa, data_processamento, "IN_PROGRESS")

    try:
        # Carregar cache
        cache = load_cache()

        # Ler CSV com pandas
        logging.info(f"Lendo arquivo CSV: {INPUT_CSV}")
        try:
            # Ler apenas colunas necessárias
            df = pd.read_csv(INPUT_CSV, sep='|', usecols=['id', 'anoLancamento'], dtype={'id': str, 'anoLancamento': str}, encoding='utf-8')
            logging.info(f"Total de linhas lidas no CSV: {len(df)}")

            # Logar amostra
            sample_rows = df.head(5).to_dict('records')
            logging.info(f"Sample de linhas do CSV: {sample_rows}")

            # Contagem de anos
            year_counts = Counter(df['anoLancamento'].dropna())
            logging.info(f"Distribuição de anoLancamento (top 10): {year_counts.most_common(10)}")

            # Logar duplicatas por ID
            id_counts = df['id'].value_counts()
            logging.info(f"Total de IDs únicos no CSV: {len(id_counts)}")
            multi_row_ids = id_counts[id_counts > 1].to_dict()
            if multi_row_ids:
                logging.info(f"IDs com múltiplas linhas (top 10): {dict(sorted(multi_row_ids.items(), key=lambda x: x[1], reverse=True)[:10])}")
            else:
                logging.info("Nenhum ID com múltiplas linhas encontrado.")

            # Filtrar IDs com uma única linha
            single_row_ids = id_counts[id_counts == 1].index
            df = df[df['id'].isin(single_row_ids)]
            logging.info(f"Linhas após filtro de IDs com uma única linha: {len(df)}")

            # Validações iniciais
            rejected_counts = Counter()
            # IDs inválidos
            invalid_ids_mask = ~df['id'].str.startswith('tt').fillna(False)
            rejected_counts['id_invalido'] = invalid_ids_mask.sum()
            if rejected_counts['id_invalido'] > 0:
                logging.debug(f"Exemplos de IDs inválidos: {df[invalid_ids_mask]['id'].head(10).tolist()}")

            # Anos vazios ou \N
            empty_year_mask = df['anoLancamento'].isna() | (df['anoLancamento'] == '\\N')
            rejected_counts['ano_vazio_ou_nulo'] = empty_year_mask.sum()
            if rejected_counts['ano_vazio_ou_nulo'] > 0:
                logging.debug(f"Exemplos de linhas com ano vazio: {df[empty_year_mask][['id', 'anoLancamento']].head(20).to_dict('records')}")

            # Anos não numéricos
            non_numeric_mask = (~df['anoLancamento'].str.isdigit().fillna(False)) & (~empty_year_mask)
            rejected_counts['ano_nao_numerico'] = non_numeric_mask.sum()
            if rejected_counts['ano_nao_numerico'] > 0:
                logging.debug(f"Exemplos de anos não numéricos: {df[non_numeric_mask]['anoLancamento'].head(20).tolist()}")

            # Converter anos para inteiros
            df['anoLancamento'] = pd.to_numeric(df['anoLancamento'], errors='coerce').astype('Int64')
            accepted_rows = df[df['anoLancamento'].between(2015, 2022)]
            logging.info(f"Linhas aceitas (2015-2022): {len(accepted_rows)}")

            # Filtrar IDs válidos
            debug_data = {"rejected_ids": []}
            # Rejeitar IDs fora de 2015-2022
            rejected_ids = df[~df['anoLancamento'].between(2015, 2022)][['id', 'anoLancamento']].head(20).to_dict('records')
            if rejected_ids:
                debug_data['rejected_ids'] = rejected_ids
                logging.debug(f"Exemplos de IDs rejeitados: {rejected_ids[:5]}")
            rejected_ids_count = len(df[~df['anoLancamento'].between(2015, 2022)])
            logging.info(f"IDs rejeitados por anos fora de 2015-2022: {rejected_ids_count}")

            # Selecionar IDs válidos
            unique_ids = sorted(accepted_rows['id'].unique().tolist())
            logging.info(f"IMDb IDs únicos encontrados (2015-2022): {len(unique_ids)}")

            # Salvar debug
            save_debug_info(debug_data)

            # Validar número de IDs
            if len(unique_ids) > MAX_IDS:
                logging.error(f"Número de IDs únicos ({len(unique_ids)}) excede o limite máximo ({MAX_IDS}). Verifique o filtro ou CSV.")
                registrar_status(conn, etapa, data_processamento, "FAILED")
                conn.close()
                exit(1)
            if len(unique_ids) < MIN_IDS:
                logging.error(f"Número de IDs únicos ({len(unique_ids)}) abaixo do mínimo esperado ({MIN_IDS}). Verifique o arquivo CSV.")
                registrar_status(conn, etapa, data_processamento, "FAILED")
                conn.close()
                exit(1)

            if not unique_ids:
                logging.warning("Nenhum ID válido encontrado para 2015-2022. Finalizando.")
                registrar_status(conn, etapa, data_processamento, "COMPLETED")
                conn.close()
                return

            logging.info(f"Rejeições por motivo: {dict(rejected_counts)}")

        except FileNotFoundError:
            logging.error(f"Arquivo CSV não encontrado: {INPUT_CSV}")
            registrar_status(conn, etapa, data_processamento, "FAILED")
            conn.close()
            exit(1)
        except Exception as e:
            logging.error(f"Erro ao ler o CSV: {e}")
            registrar_status(conn, etapa, data_processamento, "FAILED")
            conn.close()
            exit(1)

        # Processar IDs em paralelo
        data_chunk, file_index, chunk_size_bytes = [], 1, 0
        global count
        processed_count = 0
        financial_rejected = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_id = {executor.submit(process_imdb_id, imdb_id, cache): imdb_id for imdb_id in unique_ids}
            for future in future_to_id:
                imdb_id = future_to_id[future] 
                if MAX_JSON_FILES and file_index > MAX_JSON_FILES:
                    logging.info(f"Limite de {MAX_JSON_FILES} arquivos atingido. Cancelando tarefas pendentes.")
                    for f in future_to_id:
                        f.cancel()
                    break

                try:
                    result = future.result()
                    with count_lock:
                        count += 1
                        processed_count = count

                    if not result or result[1] is None:
                        financial_rejected += 1
                        logging.debug(f"IMDb ID {imdb_id} ignorado (sem dados válidos ou financeiros).")
                        continue

                    _, enriched = result
                    data_str = json.dumps(enriched, ensure_ascii=False)
                    data_size = len(data_str.encode('utf-8'))

                    if len(data_chunk) >= MAX_RECORDS or (chunk_size_bytes + data_size) > MAX_FILE_SIZE_BYTES:
                        save_json_chunk(data_chunk, file_index)
                        file_index += 1
                        data_chunk, chunk_size_bytes = [], 0

                    data_chunk.append(enriched)
                    chunk_size_bytes += data_size

                    logging.info(f"({processed_count}/{len(unique_ids)}) IMDb ID {imdb_id} processado.")
                except Exception as e:
                    with count_lock:
                        count += 1
                        processed_count = count
                    logging.error(f"Erro ao processar ID {imdb_id}: {str(e)}")

        if data_chunk and (MAX_JSON_FILES is None or file_index <= MAX_JSON_FILES):
            save_json_chunk(data_chunk, file_index)
            logging.info(f"Total de arquivos JSON gerados: {file_index}")
            logging.info(f"Registros rejeitados por falta de dados financeiros ou erros: {financial_rejected}")

        # Salvar cache
        save_cache(cache)

        registrar_status(conn, etapa, data_processamento, "COMPLETED")
        logging.info("Enriquecimento concluído com sucesso.")

    except Exception as e:
        logging.error(f"Erro durante execução: {str(e)}")
        registrar_status(conn, etapa, data_processamento, "FAILED")
        exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()