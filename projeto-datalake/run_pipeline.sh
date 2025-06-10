#!/bin/bash
set -e

echo "Iniciando pipeline..."

python /app/scripts/raw/ingestao_raw.py
python /app/scripts/raw/enriquecimento_tmdb.py
python /app/scripts/trusted/trusted_ingestao_csv.py
python /app/scripts/trusted/trusted_ingestao_tmdb.py
python /app/scripts/refined/refined_modelagem_dimensional.py
python /app/scripts/refined/validacao_modelagem.py

echo "Pipeline concluída com sucesso!"