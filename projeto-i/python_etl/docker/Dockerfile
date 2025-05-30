FROM python:latest

# Define diretório de trabalho
WORKDIR /app

# Cria o diretório que será usado como volume
RUN mkdir -p /app/input

# Instala dependências de sistema necessárias para psycopg2 e pandas
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copia o arquivo de dependências Python
COPY requirements.txt .

# Instala bibliotecas Python
RUN pip install --no-cache-dir -r requirements.txt

# Copia o script ETL
COPY etl.py .

# Comando padrão do container
CMD ["python", "etl.py"]
