"""
Orquestador local del pipeline.
Ejecuta todos los pasos en orden (alternativa a Airflow para ejecución manual).
"""
import time
import subprocess
import sys


def run_step(name: str, command: list):
    """Ejecuta un paso del pipeline y reporta el resultado."""
    print(f"\n{'='*50}")
    print(f"STEP: {name}")
    print(f"{'='*50}")

    start = time.time()
    result = subprocess.run(command, capture_output=True, text=True)
    duration = time.time() - start

    if result.stdout:
        print(result.stdout)
    if result.returncode != 0:
        print(f"ERROR: {result.stderr}")
        return False

    print(f"✅ {name} completado en {duration:.1f}s")
    return True


def main():
    steps = [
        ("Generar datos", [sys.executable, "jobs/generar_datos.py"]),
        ("Ingestión (simulada local)", [sys.executable, "-c",
            "import pandas as pd; import os; "
            "os.makedirs('data/output', exist_ok=True); "
            "df = pd.read_parquet('data/input/ordenes.parquet'); "
            "df.to_parquet('data/output/ordenes_raw.parquet', index=False); "
            "print(f'[ingest] {len(df)} órdenes copiadas a data/output/')"
        ]),
        ("dbt run", [sys.executable, "-m", "dbt", "run",
            "--project-dir", "dbt", "--profiles-dir", "dbt"]),
        ("dbt test", [sys.executable, "-m", "dbt", "test",
            "--project-dir", "dbt", "--profiles-dir", "dbt"]),
    ]

    print("🚀 PIPELINE E-COMMERCE — Ejecución completa")
    start_total = time.time()

    for name, cmd in steps:
        success = run_step(name, cmd)
        if not success:
            print(f"\n❌ Pipeline falló en: {name}")
            sys.exit(1)

    duration_total = time.time() - start_total
    print(f"\n{'='*50}")
    print(f"✅ PIPELINE COMPLETADO en {duration_total:.1f}s")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
