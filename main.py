import subprocess
import os
import sys
from utils.logger import get_logger

logger = get_logger(__name__)

def run_extract(endpoint, folder, filename, date_column):
    script_path = os.path.abspath("src/extract_fact.py")
    cmd = [
        sys.executable,
        script_path,
        "--endpoint", endpoint,
        "--folder", folder,
        "--filename", filename,
        "--date_column", date_column
    ]
    logger.info(f"Ejecutando extracción: {cmd}")
    try:
        subprocess.run(cmd, check=True, env=os.environ)
        logger.info(f"Extracción {filename} finalizada con éxito.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error ejecutando {filename}: {e}")

if __name__ == "__main__":
    tasks = [
        ("/sales", "fact_sales", "fact_sales", "attributes.createdAt"),
        ("/items", "fact_sales_orders", "fact_sales_orders", "attributes.createdAt")
        ]

    for endpoint, folder, filename, date_column in tasks:
        run_extract(endpoint, folder, filename, date_column)