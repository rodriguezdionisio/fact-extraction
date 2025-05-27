import pandas as pd
from google.cloud import storage, secretmanager
from google.oauth2 import service_account
import gcsfs
from utils.env_config import config
from utils.logger import get_logger

logger = get_logger(__name__)

# Cliente singleton
_storage_client = None

def get_storage_client():
    """Inicializa (una vez) y devuelve el cliente de Google Cloud Storage."""
    global _storage_client
    if _storage_client:
        return _storage_client

    try:
        if config.GOOGLE_APPLICATION_CREDENTIALS:
            credentials = service_account.Credentials.from_service_account_file(config.GOOGLE_APPLICATION_CREDENTIALS)
            _storage_client = storage.Client(credentials=credentials, project=config.GCP_PROJECT_NAME)
            logger.info("Cliente de Storage inicializado con archivo de credenciales.")
        else:
            _storage_client = storage.Client(project=config.GCP_PROJECT_NAME)
            logger.info("Cliente de Storage inicializado con Application Default Credentials (ADC).")
    except Exception as e:
        logger.error(f"Error al inicializar cliente de GCS: {e}")
        _storage_client = None

    return _storage_client


def upload_csv_to_gcs(dataframe: pd.DataFrame, bucket_name: str, gcs_file_path: str, content_type: str = 'text/csv') -> bool:
    """
    Sube un DataFrame como CSV a Google Cloud Storage.

    Args:
        dataframe: DataFrame de Pandas.
        bucket_name: Nombre del bucket en GCS.
        gcs_file_path: Ruta destino (ej: 'carpeta/archivo.csv').
        content_type: Tipo de contenido (por defecto 'text/csv').

    Returns:
        True si la subida fue exitosa, False si hubo error.
    """
    client = get_storage_client()
    if not client:
        logger.error("Cliente de GCS no disponible.")
        return False

    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_file_path)
        csv_data = dataframe.to_csv(index=False, encoding='utf-8')
        blob.upload_from_string(csv_data, content_type=content_type)
        logger.info(f"Archivo subido con éxito a gs://{bucket_name}/{gcs_file_path}")
        return True
    except Exception as e:
        logger.error(f"Error al subir archivo a GCS: {e}")
        return False


def get_secret(secret_id: str) -> str:
    """
    Recupera el valor de un secreto almacenado en Secret Manager.

    Args:
        secret_id: ID del secreto.

    Returns:
        Valor del secreto como string.
    """
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{config.GCP_PROJECT_ID}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        logger.info(f"Secreto '{secret_id}' accedido correctamente.")
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logger.error(f"Error al acceder al secreto '{secret_id}': {e}")
        return ""

def get_gcsfs():
    """
    Retorna una instancia de GCSFileSystem, usando el archivo de credenciales si está definido.
    """
    if config.GOOGLE_APPLICATION_CREDENTIALS:
        return gcsfs.GCSFileSystem(token=config.GOOGLE_APPLICATION_CREDENTIALS)
    return gcsfs.GCSFileSystem()

def list_gcs_files(bucket: str, prefix: str) -> list[str]:
    """
    Lista archivos en GCS con un prefijo determinado usando gcsfs.

    Args:
        bucket (str): Nombre del bucket (sin 'gs://').
        prefix (str): Prefijo de los archivos a listar.

    Returns:
        list[str]: Lista de rutas de archivos en GCS que coinciden con el prefijo.
    """
    fs = get_gcsfs()
    path = f"gs://{bucket}/{prefix}"
    try:
        return fs.ls(path)
    except Exception as e:
        logger.error(f"gcs_utils: Error al listar archivos en {path}: {e}")
        return []