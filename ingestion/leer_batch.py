import pandas as pd
import requests
import logging

log = logging.getLogger(__name__)

def leer_datos_batch(subject='cooking'):
    """
    Ingesta batch: obtiene un listado de libros desde la API pública
    de Open Library según el tema indicado.
    Retorna un DataFrame con título, key y año de primera publicación.
    """
    url = f"https://openlibrary.org/subjects/{subject}.json?limit=10"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        df = pd.json_normalize(data['works'])
        df = df[['title', 'key', 'first_publish_year']]
        log.info(f"Batch completado: {len(df)} libros obtenidos (tema: {subject}).")
        return df
    except Exception as e:
        log.error(f"Error al obtener datos batch de Open Library: {e}")
        return pd.DataFrame()
