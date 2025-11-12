import pandas as pd
from pathlib import Path

RAW_DIR = Path("dataset/raw")
BRONZE_DIR = Path("dataset/bronze")

BRONZE_DIR.mkdir(parents=True, exist_ok=True)

def load_all_raw():
    files = sorted(RAW_DIR.glob("*.csv"))
    if not files:
        raise RuntimeError("Nenhum arquivo encontrado na pasta raw")
    dfs = []
    for f in files:
        print(f"Lendo {f.name}...")
        dfs.append(pd.read_csv(f))
    print(f"Total de arquivos lidos: {len(dfs)}")
    return pd.concat(dfs, ignore_index=True)

def save_parquet_partitioned(df):

    df["data_pagamento"] = pd.to_datetime(df["data_pagamento"], errors="coerce")

    
    object_cols = df.select_dtypes(include=["object"]).columns
    for col in object_cols:
        df[col] = df[col].astype("string")

    
    df["ano"] = df["data_pagamento"].dt.year
    df["mes"] = df["data_pagamento"].dt.month

    
    missing_mask = df["ano"].isna() | df["mes"].isna()
    if missing_mask.any():
        missing_df = df[missing_mask].copy()
        unk_folder = BRONZE_DIR / "ano=unknown" / "mes=unknown"
        unk_folder.mkdir(parents=True, exist_ok=True)
        unk_path = unk_folder / "data.parquet"
        if unk_path.exists():
            existing = pd.read_parquet(unk_path)
            pd.concat([existing, missing_df], ignore_index=True).to_parquet(unk_path, index=False)
        else:
            missing_df.to_parquet(unk_path, index=False)
        print(f"{len(missing_df)} linhas sem data válidas salvas em {unk_path}")

    
    good_df = df[~missing_mask].copy()
    if good_df.empty:
        print("Não há registros válidos para particionar por ano/mes.")
        return

    good_df["ano"] = good_df["ano"].astype(int)
    good_df["mes"] = good_df["mes"].astype(int)

    
    for (ano, mes), group in good_df.groupby(["ano", "mes"]):
        folder = BRONZE_DIR / f"ano={ano}" / f"mes={mes:02d}"
        folder.mkdir(parents=True, exist_ok=True)
        file_path = folder / "data.parquet"

        if file_path.exists():
            existing = pd.read_parquet(file_path)
            pd.concat([existing, group], ignore_index=True).to_parquet(file_path, index=False)
        else:
            group.to_parquet(file_path, index=False)

        print(f"{len(group)} linhas salvas em {file_path}")


def main():
    print("convertendo de raw -> bronze.")
    df = load_all_raw()
    save_parquet_partitioned(df)
    print(" Finalizado.")

if __name__ == "__main__":
    main()
