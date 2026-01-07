from pathlib import Path
import pandas as pd
import time
from tqdm import tqdm

RAW = Path("data/raw/customers_2m.csv")
OUT = Path("data/bronze/customers_parquet")
OUT.mkdir(parents=True, exist_ok=True)

CHUNK_SIZE = 200_000 
# --- 1) Contar filas totales (para que la barra sea exacta) ---
with RAW.open("rb") as f:
    total_lines = sum(1 for _ in f)  # líneas = filas + header

total_rows = max(0, total_lines - 1)  # restamos la fila del header

start = time.time()
rows = 0
part = 0

print(f"Archivo: {RAW.name}")
print(f"Filas estimadas: {total_rows:,}")
print(f"Chunk size: {CHUNK_SIZE:,}\n")

# --- 2) Leer el CSV en chunks y procesar ---
with tqdm(total=total_rows, unit="rows", desc="Ingesta Bronze") as pbar:
    for df in pd.read_csv(RAW, chunksize=CHUNK_SIZE):
        # Normalizar nombres de columnas a snake_case
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        # Guardar chunk como Parquet
        df.to_parquet(OUT / f"part_{part:04d}.parquet", index=False)

        # Actualizar contadores
        chunk_rows = len(df)
        rows += chunk_rows
        part += 1

        # Actualizar barra de progreso
        pbar.update(chunk_rows)

        # Mostrar info “no se colgó” (cada 5 partes para no spamear)
        if part % 5 == 0:
            elapsed = time.time() - start
            rate = rows / elapsed if elapsed > 0 else 0
            print(f"  → partes: {part:04d} | filas: {rows:,} | {rate:,.0f} filas/seg | {elapsed:.1f}s")

elapsed = time.time() - start
rate = rows / elapsed if elapsed > 0 else 0

print("\n==== BRONZE LISTO ====")
print("Archivo:", RAW.name)
print("Filas:", f"{rows:,}")
print("Partes:", part)
print("Tiempo:", f"{elapsed:.2f}s")
print("Velocidad:", f"{rate:,.0f} filas/seg")
print("Salida:", str(OUT))
