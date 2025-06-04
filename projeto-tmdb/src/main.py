from src.tmdb_client import buscar_filmes_populares

def main():
    ano = 2025
    filmes = buscar_filmes_populares(ano)

    print(f"\n🎬 Filmes populares de {ano}:\n")
    if not filmes:
        print("Nenhum filme encontrado ou erro na consulta.")
        return

    for filme in filmes:
        titulo = filme.get("title", "Sem título")
        popularidade = round(filme.get("popularity", 0), 2)
        print(f"- {titulo} (Popularidade: {popularidade})")

if __name__ == "__main__":
    main()
