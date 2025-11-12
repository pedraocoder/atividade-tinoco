import pandas as pd
from pathlib import Path

SILVER_DIR = Path("dataset/silver")
GOLD_DIR = Path("dataset/gold")

GOLD_DIR.mkdir(parents=True, exist_ok=True)

def load_silver_data():
    """Lê todos os arquivos Parquet da camada Silver."""
    files = list(SILVER_DIR.rglob("data.parquet"))
    if not files:
        raise RuntimeError("Nenhum arquivo encontrado na camada silver.")
    
    dfs = []
    for f in files:
        print(f"Lendo {f} ...")
        dfs.append(pd.read_parquet(f))
    return pd.concat(dfs, ignore_index=True)

def generate_gold_tables(df):
    """Gera agregações de negócio a partir da camada Silver."""
    print("Gerando agregações (Camada Gold)...")

    gastos_mensais = (
        df.groupby(["ano", "mes"], as_index=False)
        .agg(total_gasto=("valor_pago", "sum"))
        .sort_values(["ano", "mes"])
    )

    if "orgao_superior" in df.columns:
        gastos_por_orgao = (
            df.groupby("orgao_superior", as_index=False)
            .agg(total_gasto=("valor_pago", "sum"))
            .sort_values("total_gasto", ascending=False)
        )
    else:
        gastos_por_orgao = pd.DataFrame()

    if "categoria_economica" in df.columns:
        gastos_por_categoria = (
            df.groupby("categoria_economica", as_index=False)
            .agg(total_gasto=("valor_pago", "sum"))
            .sort_values("total_gasto", ascending=False)
        )
    else:
        gastos_por_categoria = pd.DataFrame()

    print("Agregações concluídas.")
    return {
        "gastos_mensais": gastos_mensais,
        "gastos_por_orgao": gastos_por_orgao,
        "gastos_por_categoria": gastos_por_categoria,
    }

def save_gold_data(tables):
    """Salva as tabelas agregadas em formato parquet."""
    for name, df in tables.items():
        if df.empty:
            print(f"Nenhum dado disponível para {name}.")
            continue

        path = GOLD_DIR / f"{name}.parquet"
        df.to_parquet(path, index=False)
        print(f"✅ Tabela '{name}' salva em {path} ({len(df)} linhas).")

def main():
    print("=== ETL: Silver → Gold ===")
    df = load_silver_data()
    tables = generate_gold_tables(df)
    save_gold_data(tables)
    print("Camada Gold criada com sucesso!")

if __name__ == "__main__":
    main()
