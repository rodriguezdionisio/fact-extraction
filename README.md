# fact-extraction-gcs

Este proyecto permite extraer datos de las tablas de hechos del sistema de gestión de restaurantes Fudo a través de su API y cargarlos en Google Cloud Storage (GCS) de forma automatizada.

## Requisitos

- Python 3.10+
- Acceso a Google Cloud Platform (GCP) con permisos para Storage y Secret Manager
- Credenciales de servicio de GCP (archivo JSON)
- API Key y Secret de Fudo

## Instalación

1. Clona el repositorio y entra en la carpeta del proyecto.
2. Instala las dependencias:
   ```sh
   pip install -r requirements.txt
   ```
3. Crea un archivo `.env` en la raíz con las siguientes variables:
   ```
   GCS_BUCKET_NAME=tu-bucket
   GCP_PROJECT_NAME=tu-proyecto
   GCP_PROJECT_ID=tu-id-proyecto
   GOOGLE_APPLICATION_CREDENTIALS=path/a/credenciales.json
   FUDO_AUTH_URL=https://api.fudo.com/auth
   FUDO_API_URL=https://api.fudo.com/v1
   ENV=local
   ```

4. Coloca tus credenciales de servicio en la ruta indicada.

## Uso

### Ejecución manual

Puedes ejecutar la extracción de datos desde la terminal:

```sh
python src/extract_fact.py --endpoint /sales --folder fact_sales --filename fact_sales --date_column attributes.createdAt
```

Parámetros:
- `--endpoint`: Endpoint de la API de Fudo (ej: `/sales`)
- `--folder`: Carpeta destino en GCS (ej: `fact_sales`)
- `--filename`: Nombre base del archivo CSV (ej: `fact_sales`)
- `--date_column`: Columna de fecha para agrupar (por defecto: `attributes.createdAt`)

### Ejecución automática

El archivo [`main.py`](main.py) ejecuta tareas de extracción para varios endpoints definidos en el código.

```sh
python main.py
```

## Estructura del proyecto

- `src/extract_fact.py`: Lógica principal de extracción y carga a GCS.
- `utils/`: Utilidades para conexión con GCP, Fudo y logging.
- `config/credentials.json`: Credenciales de servicio de GCP (no versionar).
- `.env`: Variables de entorno (no versionar).

## Notas

- Los secretos de Fudo (API Key y Secret) deben almacenarse en Secret Manager de GCP.
- El sistema guarda el estado de la última página descargada en GCS para evitar duplicados.
- Los archivos se agrupan y almacenan por día en formato CSV en GCS.

## Licencia

MIT
