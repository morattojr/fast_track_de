
---

# Fast Track TMDB

Este projeto é um script em Python que consome a API do [The Movie Database (TMDb)](https://www.themoviedb.org/) para listar os filmes mais populares do ano de 2025. O retorno da API é exibido diretamente no console.

---

## Estrutura do Projeto

```

fast\_track\_de\_tmdb/
├── README.md
├── requirements.txt
├── .env.tmdb
└── src/
├── **init**.py
├── main.py
└── tmdb\_client.py

````

---

## Pré-requisitos

- Python 3.9 ou superior
- Conta no [TMDb](https://www.themoviedb.org/) para obter a sua chave da API

---

## Como executar

1. **Clone o repositório**

```bash
git clone https://github.com/seu-usuario/fast_track_de_tmdb.git
cd fast_track_de_tmdb
````

2. **Crie e ative um ambiente virtual**

```bash
# Crie o ambiente virtual (nome padrão usado: .venv)
python -m venv .venv

# Ative o ambiente virtual:

# 👉 Linux/macOS:
source .venv/bin/activate

# 👉 Windows:
.venv\Scripts\activate
```

> 💡 Dica: O VS Code reconhece automaticamente o ambiente virtual chamado `.venv`, facilitando o uso com o interpretador Python configurado no editor.

3. **Instale as dependências**

```bash
pip install -r requirements.txt
```

4. **Configure sua chave da API**

Copie o arquivo `.env.tmdb` para `.env`:

```bash
cp .env.tmdb .env
```

Edite o arquivo `.env` com a sua chave da API:

```env
TMDB_API_KEY=sua_chave_aqui
```

5. **Execute o script**

```bash
python -m src.main
```

---

## Exemplo de saída

```bash
🎬 Filmes populares de 2025:

1. Missão: Impossível 8
2. Avatar 3
3. Duna: Parte Dois
4. Deadpool & Wolverine
...
```

---

## Licença

Este projeto é livre para fins de estudo e uso pessoal.

```
---
