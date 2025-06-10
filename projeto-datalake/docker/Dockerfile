FROM python:3.11-slim

# Variáveis de ambiente iniciais (JAVA_HOME será ajustado depois)
ENV PYTHONUNBUFFERED=1 \
    SPARK_VERSION=3.5.1 \
    HADOOP_VERSION=3 \
    PATH="/opt/spark/bin:${PATH}"

WORKDIR /app

# Instala Java Temurin, ferramentas básicas e dependências do Spark
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    wget \
    gnupg \
    procps \
    ca-certificates \
    && wget -O - https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor -o /etc/apt/trusted.gpg.d/adoptium.gpg \
    && echo "deb https://packages.adoptium.net/artifactory/deb bookworm main" > /etc/apt/sources.list.d/adoptium.list \
    && apt-get update \
    && apt-get install -y temurin-11-jre \
    && rm -rf /var/lib/apt/lists/*

# Descobre onde o java foi instalado e cria link simbólico para JAVA_HOME esperado
RUN JAVA_PATH=$(dirname $(dirname $(readlink -f $(which java)))) && \
    ln -sfn $JAVA_PATH /usr/lib/jvm/temurin-11-jre-amd64

# Define JAVA_HOME e atualiza PATH
ENV JAVA_HOME=/usr/lib/jvm/temurin-11-jre-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Instala Spark
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    -o /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt/ \
    && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
    && rm /tmp/spark.tgz

# Instala dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia código da aplicação
COPY . .

# Verifica versão do Java e Python para debug
RUN java -version && python --version

# Comando padrão (pode ser sobrescrito no docker-compose)
CMD ["python", "--version"]
