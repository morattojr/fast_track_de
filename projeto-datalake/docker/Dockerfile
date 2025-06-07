FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/ scripts/

CMD ["python", "scripts/ingestao_raw.py"]
