import os
import time
import requests
import pandas as pd
from pathlib import Path

BASE_URL = "https://brasil.io/api/v1/dataset/gastos-diretos/gastos/data/"

RAW_DIR = Path("dataset/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

def get_token():
    token = os.getenv("BRASIL_IO_TOKEN")
    if not token:
        raise RuntimeError(
         "Erro: Token n~ao definido"
        )
    return token

def fetch_page(url, params, headers):
    max_retries = 5
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:  # ERRO 429
                print(f"Limite atingido — esperando 4 seg... (tentativa {attempt+1}/{max_retries})")
                time.sleep(4)
            else:
                raise e
    raise RuntimeError("Erro após várias tentativas")

def download_raw_data(max_pages=1000):
    token = get_token()
    headers = {"Authorization": f"Token {token}"}

    page = 1
    while page <= max_pages:
        print(f"Baixando página {page}/{max_pages}...")

        params = {"page": page, "page_size": 1000}
        data = fetch_page(BASE_URL, params, headers)

        results = data.get("results", [])
        if not results:
            print("Sem mais dados. Encerrando.")
            break

        df = pd.DataFrame(results)
        file_path = RAW_DIR / f"page_{page}.csv"
        df.to_csv(file_path, index=False)

        print(f"Página {page} salva — {len(df)} linhas")

        page += 1
        time.sleep(0.5)

    print("Download finalizado")

def main():
    print("Iniciando ETL — Brasil.IO")
    download_raw_data(max_pages=1000)

if __name__ == "__main__":
    main()
