import requests
from utils.gcp import get_secret
from utils.logger import get_logger

logger = get_logger(__name__)

FUDO_AUTH_URL = "https://auth.fu.do/api"
FUDO_API_URL = "https://api.fu.do/v1alpha1"

def get_token():
    """Obtiene el token de autenticación desde la API de Fudo."""
    try:
        api_key = get_secret("fudo-api-key")
        api_secret = get_secret("fudo-api-secret")

        payload = {"apiKey": api_key, "apiSecret": api_secret}
        headers = {"Content-Type": "application/json"}

        response = requests.post(FUDO_AUTH_URL, json=payload, headers=headers)
        response.raise_for_status()

        token = response.json().get("token")
        if not token:
            raise Exception("Token no encontrado en la respuesta.")

        logger.info("Token obtenido correctamente desde Fudo.")
        return token

    except requests.exceptions.RequestException as e:
        logger.error(f"Error de conexión al obtener token: {e}")
        raise
    except Exception as e:
        logger.error(f"Error inesperado al obtener token: {e}")
        raise


def get_fudo_data(token, endpoint, page_size=500, page_number=1, extra_params=None, max_pages=1):
    """
    Descarga datos paginados desde un endpoint de la API de Fudo.

    Args:
        token (str): Token de autenticación.
        endpoint (str): Endpoint de la API.
        page_size (int): Tamaño de página para la paginación.
        page_number (int): Número de página inicial.
        extra_params (dict, optional): Parámetros adicionales.
        max_pages (int or None): Máximo de páginas a recorrer. Si es None, descarga todas.

    Returns:
        list: Lista de resultados.
    """
    url = f"{FUDO_API_URL}{endpoint}"
    headers = {"Authorization": f"Bearer {token}"}
    data = []

    logger.info(f"Descargando datos desde: {url}")
    current_page = page_number
    pages_fetched = 0

    try:
        while True:
            params = {
                "page[size]": page_size,
                "page[number]": current_page
            }

            if extra_params:
                params.update(extra_params)

            logger.info(f"Consultando página: {current_page} con parámetros: {params}")
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()

            page_data = response.json()
            results = page_data.get("data", [])
            data.extend(results)

            logger.info(f"Página {current_page} recibida con {len(results)} registros.")

            if not results or len(results) < page_size:
                logger.info("No hay más datos para descargar.")
                break

            current_page += 1
            pages_fetched += 1

            if max_pages is not None and pages_fetched >= max_pages:
                logger.info(f"Se alcanzó el máximo de páginas permitido: {max_pages}")
                break

    except requests.exceptions.RequestException as e:
        logger.error(f"Error durante la petición a la API: {e}")
        return None
    except Exception as e:
        logger.error(f"Error inesperado durante la descarga: {e}")
        return None

    return data
