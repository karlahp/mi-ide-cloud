import pandas as pd
import time
import logging

from ingestion.lectura_csv import leer_datos_csv
from ingestion.leer_batch import leer_datos_batch
from ingestion.fuente_realtime import leer_clima_tiempo_real

# --- CONFIGURACIÓN DE LOGGING (Requisito de la actividad) ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("ingesta.log"), logging.StreamHandler()]
)
# -----------------------------------------------------------

def run_orchestator():
    logging.info("Iniciando orquestador de ingesta...")
    almacen_datos = {}

    print("--- Lectura de csv")
    almacen_datos['Titanic']=leer_datos_csv()
    logging.info("CSV Titanic cargado correctamente.")

    print("--- Lectura de titulos libros")
    almacen_datos['Libros']=leer_datos_batch('scifi')
    logging.info("Batch de libros cargado.")

    print("--- Lectura del clima en tiempo real")
    total_lecturas=[]
    
    # Tomamos 5 instantaneas para simular tiempo real
    for i in range(5):
        print(f"  > instantanea {i+1}...")
        df_snap = leer_clima_tiempo_real()
        if not df_snap.empty:
            total_lecturas.append(df_snap)
        time.sleep(1) # Short delay
    
    if total_lecturas:
        almacen_datos['clima'] = pd.concat(total_lecturas, ignore_index=True)
        logging.info("Datos del clima recolectados exitosamente.")
    else:
        almacen_datos['clima'] = pd.DataFrame()
        logging.error("No se pudo recolectar el clima.")

    print("\n--- Resumen de datos sin transformar ---")
    for elemento, df in almacen_datos.items():
        print(f"\n📍 FUENTE: {elemento}")
        if not df.empty:
            print(f"Rows: {len(df)} | Columns: {list(df.columns)}")
            print(df.head(2))
        else:
            print("Datos vacíos o con error")
            
    return almacen_datos

if __name__ == "__main__":
    run_orchestator()