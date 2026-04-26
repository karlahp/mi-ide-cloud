import pandas as pd
import logging

log = logging.getLogger(__name__)

def leer_datos_csv():
    """
    Ingesta batch: lee el dataset Titanic desde un archivo CSV local.
    Retorna un DataFrame con todos los registros.
    """
    source = "Titanic.csv"
    try:
        df = pd.read_csv(source)
        log.info(f"CSV cargado correctamente. Total líneas importadas: {len(df)}")
        return df
    except FileNotFoundError:
        log.error(f"Archivo '{source}' no encontrado. Verifica que esté en la raíz del proyecto.")
        return pd.DataFrame()
