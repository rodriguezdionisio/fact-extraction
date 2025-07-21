import sys
import os
import pandas as pd
import argparse
import json
import pytz

# Obtiene la ruta absoluta del directorio padre del script actual (src/)
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
# Añade el directorio padre (la raíz del proyecto) al sys.path
sys.path.append(parent_dir)

from utils import gcp, fudo
from utils.env_config import config
from utils.logger import get_logger

logger = get_logger(__name__)

def read_log(bucket_name, folder_prefix, filename):
    """Lee la última página descargada desde un archivo en GCS."""
    fs = gcp.get_gcsfs()
    full_path = f"gs://{bucket_name}/raw/{folder_prefix}/{filename}_log.txt"
    try:
        with fs.open(full_path, 'r') as f:
            content = f.read().strip()
            if content.isdigit():
                return int(content)
            else:
                logger.warning(f"El archivo de estado en GCS contiene un valor inválido: '{content}'. Iniciando desde la página 0.")
                return 0
    except FileNotFoundError:
        logger.info("Archivo de estado no encontrado en GCS. Iniciando desde la página 0.")
        return 0
    except Exception as e:
        logger.error(f"Error al leer el archivo de estado desde GCS: {e}")
        return 0

def write_last_page(bucket_name, folder_prefix, filename, page):
    """Escribe la última página descargada en un archivo en GCS."""
    fs = gcp.get_gcsfs()
    full_path = f"gs://{bucket_name}/raw/{folder_prefix}/{filename}_log.txt"
    try:
        with fs.open(full_path, 'w') as f:
            f.write(str(page))
    except Exception as e:
        logger.error(f"Error al escribir el archivo de estado en GCS: {e}")

def get_from_fudo(token, endpoint, start_page, end_page, page_size=500):
    """
    Descarga datos desde un endpoint de Fudo, paginados por rango.
    """
    all_data = []
    for page in range(start_page, end_page + 1):
        response = fudo.get_fudo_data(token, endpoint, page_size=page_size, page_number=page)
        if response:
            all_data.extend(response)
            # La actualización del estado se hará al final de la descarga exitosa del lote
        else:
            logger.warning(f"No se obtuvieron datos en la página {page}.")
    df = pd.json_normalize(all_data)
    return df

def group_by_day_argentina(df: pd.DataFrame, date_column="attributes.createdAt") -> dict:
    """
    Agrupa un DataFrame por día según la zona horaria de Argentina usando pytz,
    sin modificar la columna de fecha original.
    """
    # 1. Crea una Serie de datetimes a partir de la columna, forzando la lectura como UTC.
    utc_times = pd.to_datetime(df[date_column], utc=True)
    
    # 2. Convierte esa Serie de UTC a la zona horaria de Argentina usando pytz.
    argentina_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
    argentina_times = utc_times.dt.tz_convert(argentina_timezone)
    
    # 3. Extrae la fecha de la Serie ya convertida para usarla como clave.
    grouping_key = argentina_times.dt.date
    
    # 4. Agrupa el DataFrame ORIGINAL usando la clave creada.
    return dict(tuple(df.groupby(grouping_key)))

def save_on_gcs(df_day, date, bucket_name, folder_prefix, filename, id_col="id"):
    """
    Guarda o actualiza un archivo diario de ventas en GCS, identificando duplicados por ID.

    Args:
        df_day (DataFrame): DataFrame con ventas de un día.
        date (date or str): Fecha del archivo (YYYY-MM-DD).
        bucket_name (str): Nombre del bucket de GCS.
        folder_prefix (str): Carpeta en GCS (ej: 'fact_sales/').
        id_col (str): Nombre de la columna ID para eliminar duplicados.
    """
    fs = gcp.get_gcsfs()
    gcs_path = f"raw/{folder_prefix}/date={date}/{filename}.csv"
    full_path = f"gs://{bucket_name}/{gcs_path}"

    try:
        try:
            with fs.open(full_path, "rb") as f:
                df_existente = pd.read_csv(f)
            logger.info(f"Archivo {filename}.csv ya existe. Actualizando...")
            df_comb = pd.concat([df_existente, df_day], ignore_index=True).drop_duplicates(subset=[id_col])
        except FileNotFoundError:
            logger.info(f"Archivo {filename}.csv no existe. Creando nuevo...")
            df_comb = df_day

        # Subir el archivo actualizado a GCS
        gcp.upload_csv_to_gcs(df_comb, bucket_name, gcs_path)

    except Exception as e:
        logger.error(f"Error al guardar ventas del día {date} en GCS: {e}")

def main(endpoint: str, folder: str, filename_base: str, date_column: str = "attributes.createdAt"):

    logger.info(f"🔵 Iniciando extracción y carga a GCS para endpoint '{endpoint}'...")

    max_pages = 5

    last_page = read_log(config.GCS_BUCKET_NAME, folder, filename_base)
    start_page = last_page + 1
    end_page = start_page + max_pages - 1

    logger.info(f"Descargando páginas desde {start_page} hasta {end_page}")
    
    try:
        token = fudo.get_token()
    except Exception as e:
        logger.error(f"❌ Error al obtener token: {e}")
        return

    df = get_from_fudo(token, endpoint, start_page, end_page, page_size=500)

    if not df.empty:
        days = group_by_day_argentina(df, date_column)
        for date, df_day in days.items():
            save_on_gcs(df_day, date, config.GCS_BUCKET_NAME, folder, filename_base)
        # Actualizar el estado solo si la descarga fue exitosa
        write_last_page(config.GCS_BUCKET_NAME, folder, filename_base, end_page)
    elif start_page <= end_page:
        logger.info("❌ No se obtuvieron datos en las páginas solicitadas.")
    else:
        logger.info("❌ No hay páginas nuevas para descargar según el estado en GCS.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract data from Fudo API and upload to GCS")
    parser.add_argument("--endpoint", required=True, help="Endpoint API de Fudo, ej: /sales")
    parser.add_argument("--folder", required=True, help="Carpeta en GCS donde guardar el archivo, ej: fact_sales")
    parser.add_argument("--filename", required=True, help="Nombre base del archivo CSV, ej: fact_sales")
    parser.add_argument("--date_column", default="attributes.createdAt", help="Nombre de la columna de fecha para agrupar")

    args = parser.parse_args()

    main(args.endpoint, args.folder, args.filename, args.date_column)