import os
import zipfile
from datetime import datetime
import shutil
from pathlib import Path
import time
import psycopg2
from psycopg2 import OperationalError
from dotenv import load_dotenv
import logging

# Configuração do logger
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Carrega variáveis do .env
env_path = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(dotenv_path=env_path)

def get_env_var(name):
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(f"A variável de ambiente {name} não está definida. Verifique o arquivo .env.")
    return value

# Variáveis de ambiente
INPUT_ZIP_DIR = Path(get_env_var("INPUT_ZIP_DIR"))
DATA_LAKE_DIR = Path(get_env_var("DATA_LAKE_DIR"))
DB_HOST = get_env_var("DB_HOST")
DB_PORT = get_env_var("DB_PORT")
DB_NAME = get_env_var("DB_NAME")
DB_USER = get_env_var("DB_USER")
DB_PASSWORD = get_env_var("DB_PASSWORD")

ETAPA = "ingestao_raw"
DATA_PROCESSAMENTO = datetime.today().date()

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
            logging.info("Conectado ao banco de dados.")
            return conn
        except OperationalError as e:
            logging.warning(f"Tentativa {i+1}/{tentativas} - Banco não disponível. Erro: {e}")
            time.sleep(intervalo)
    logging.error("Falha ao conectar ao banco após múltiplas tentativas.")
    return None

def registrar_status(conn, etapa, data, status):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline_status (etapa, data_processamento, status)
            VALUES (%s, %s, %s)
            ON CONFLICT (etapa, data_processamento)
            DO UPDATE SET status = EXCLUDED.status, updated_at = CURRENT_TIMESTAMP
        """, (etapa, data, status))
        conn.commit()

def unzip_file(zip_path: Path, extract_to: Path):
    logging.info(f"Descompactando: {zip_path}")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)

def create_datalake_path(data: datetime = None) -> Path:
    if data is None:
        data = datetime.today()
    return DATA_LAKE_DIR / 'Raw' / 'Local' / 'CSV' / 'Movies' / str(data.year) / f"{data.month:02d}" / f"{data.day:02d}"

def main():
    logging.info("Início da ingestão.")
    logging.info(f"Data de processamento: {DATA_PROCESSAMENTO}")
    conn = None
    registrado_status = False

    try:
        conn = conectar_postgres_com_retentativas()
        if not conn:
            logging.error("Execução encerrada por erro de conexão ao banco.")
            return

        registrar_status(conn, ETAPA, DATA_PROCESSAMENTO, "IN_PROGRESS")
        registrado_status = True

        zip_path = INPUT_ZIP_DIR / 'movies.zip'
        if not zip_path.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {zip_path}")
        if not zipfile.is_zipfile(zip_path):
            raise zipfile.BadZipFile(f"O arquivo não é um ZIP válido: {zip_path}")

        temp_extract_dir = INPUT_ZIP_DIR / 'temp'
        if temp_extract_dir.exists():
            shutil.rmtree(temp_extract_dir)

        unzip_file(zip_path, temp_extract_dir)

        # Garante que é o 'movies.csv'
        csv_file_path = next((f for f in temp_extract_dir.iterdir() if f.name == 'movies.csv'), None)
        if not csv_file_path:
            raise FileNotFoundError("Arquivo 'movies.csv' não encontrado no zip.")

        target_dir = create_datalake_path()
        target_dir.mkdir(parents=True, exist_ok=True)

        target_file_path = target_dir / 'movies.csv'
        if target_file_path.exists():
            logging.warning(f"O arquivo de destino já existe e será sobrescrito: {target_file_path}")
        shutil.copy(csv_file_path, target_file_path)
        logging.info(f"Arquivo copiado para: {target_file_path}")

        shutil.rmtree(temp_extract_dir)

        registrar_status(conn, ETAPA, DATA_PROCESSAMENTO, "COMPLETED")
        logging.info("Ingestão finalizada com sucesso.")

    except Exception as e:
        if conn and registrado_status:
            registrar_status(conn, ETAPA, DATA_PROCESSAMENTO, "FAILED")
        logging.error(f"Falha na ingestão: {e}")
        raise
    finally:
        if conn:
            conn.close()
    logging.info("Fim da ingestão.")

if __name__ == '__main__':
    main()
