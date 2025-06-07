import os
import zipfile
from datetime import datetime
import shutil

# Caminhos relativos dentro do container
INPUT_DIR = '/app/data/input'
DATA_LAKE_DIR = '/app/data-lake'

def unzip_file(zip_path, extract_to):
    print(f"Descompactando: {zip_path}")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)

def create_datalake_path():
    today = datetime.today()
    return os.path.join(
        DATA_LAKE_DIR,
        'Raw', 'Local', 'CSV', 'Movies',
        str(today.year),
        f"{today.month:02d}",
        f"{today.day:02d}"
    )

def main():
    zip_path = os.path.join(INPUT_DIR, 'movies.zip')
    temp_extract_dir = os.path.join(INPUT_DIR, 'temp')

    # Descompactar
    unzip_file(zip_path, temp_extract_dir)

    # Localiza o arquivo .csv extraído
    csv_files = [f for f in os.listdir(temp_extract_dir) if f.endswith('.csv')]
    if not csv_files:
        raise FileNotFoundError("Nenhum arquivo CSV encontrado no zip.")
    csv_file_path = os.path.join(temp_extract_dir, csv_files[0])

    # Criar caminho de destino no datalake
    target_dir = create_datalake_path()
    os.makedirs(target_dir, exist_ok=True)

    # Copiar o arquivo para o datalake
    target_file_path = os.path.join(target_dir, 'movies.csv')
    shutil.copy(csv_file_path, target_file_path)
    print(f"Arquivo copiado para: {target_file_path}")

    # Limpeza
    shutil.rmtree(temp_extract_dir)
    print("Ingestão finalizada com sucesso.")

if __name__ == '__main__':
    main()
