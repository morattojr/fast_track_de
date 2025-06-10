import requests
import os
from dotenv import load_dotenv

# Carrega variáveis do .env
load_dotenv(dotenv_path=".env")

API_KEY = os.getenv("TMDB_API_KEY")

def buscar_filmes_populares(ano=2025):
    if not API_KEY:
        raise ValueError("API Key não encontrada. Verifique seu arquivo .env")

    url = (
        f"https://api.themoviedb.org/3/discover/movie?"
        f"include_adult=false&include_video=false&language=pt-BR&"
        f"page=1&sort_by=popularity.desc&year={ano}&api_key={API_KEY}"
    )

    response = requests.get(url)

    if response.status_code == 200:
        return response.json().get("results", [])
    else:
        print(f"Erro ao consultar a API: {response.status_code}")
        return []
