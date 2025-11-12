import pandas as pd
from pathlib import Path
import numpy as np

BRONZE_DIR = Path("dataset/bronze")
SILVER_DIR = Path("dataset/silver")

SILVER_DIR.mkdir(parents=True, exist_ok=True)


def load_bronze_data():
    """Lê todos os arquivos Parquet da camada bronze"""
    files = list(BRONZE_DIR.rglob("data.parquet"))
    if not files:
        raise RuntimeError("Nenhum arquivo encontrado na camada bronze.")
    
    dfs = []
    for f in files:
        print(f"Lendo {f} ...")
        dfs.append(pd.read_parquet(f))
    return pd.concat(dfs, ignore_index=True)


def clean_data(df):
    print("Iniciando limpeza dos dados...")

    
    df.columns = [col.lower().strip() for col in df.columns]

    
    df = df.dropna(axis=1, how='all')

    
    df = df.drop_duplicates()

    
    if 'data_pagamento' in df.columns:
        df['data_pagamento'] = pd.to_datetime(df['data_pagamento'], errors='coerce')

    
    for col in df.columns:
        if df[col].dtype == "string" or df[col].dtype == "object":
            df[col] = df[col].fillna("Não informado")
        elif pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(0)
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].fillna(pd.Timestamp("1900-01-01"))

    
    if 'favorecido' in df.columns:
        df['favorecido'] = df['favorecido'].str.title().str.strip()

    
    for col in df.columns:
        if "valor" in col:
            df[col] = (
                df[col].astype(str)
                .str.replace(",", ".", regex=False)
                .str.replace(" ", "", regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)


    df["ano"] = df["data_pagamento"].dt.year.fillna(0).astype(int)
    df["mes"] = df["data_pagamento"].dt.month.fillna(0).astype(int)

    print("Limpeza concluída.")
    return df


def validate_data(df):
    print("Executando validações básicas...")

    if df["valor_pago"].isna().any():
        print("Existem valores pagos nulos — substituídos por 0.")
        df["valor_pago"] = df["valor_pago"].fillna(0)

    if (df["valor_pago"] < 0).any():
        print("Existem valores negativos em valor_pago.")

    if df["data_pagamento"].isna().any():
        print("Existem registros sem data válida.")

    print("Validações concluídas.")
    return df


def save_silver_partitioned(df):
    """Salva em formato Parquet particionado por ano/mês."""
    for (ano, mes), group in df.groupby(["ano", "mes"]):
        folder = SILVER_DIR / f"ano={ano}" / f"mes={mes:02d}"
        folder.mkdir(parents=True, exist_ok=True)
        file_path = folder / "data.parquet"
        group.to_parquet(file_path, index=False)
        print(f"✅ {len(group)} linhas salvas em {file_path}")


def main():
    print("=== ETL: Bronze → Silver ===")
    df = load_bronze_data()
    df = clean_data(df)
    df = validate_data(df)
    save_silver_partitioned(df)
    print("Camada Silver criada com sucesso")

if __name__ == "__main__":
    main()
