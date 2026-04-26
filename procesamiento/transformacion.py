import pandas as pd
import logging

# Configuración de logging para trazabilidad, esto hace que el ingesta.log aparezca la info con datos y forma ordenada
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# TRANSFORMACIÓN 1
# Tabla resumen: pasajeros que sobrevivieron vs. no sobrevivieron en TITANIC
# ─────────────────────────────────────────────
def resumen_sobrevivientes(almacen_datos: dict) -> dict:
    """
    Genera una tabla resumen con el conteo de pasajeros que sobrevivieron
    y los que no sobrevivieron en el dataset TITANIC.
    Almacena el resultado como nueva entrada 'resumen_sobrevivientes'
    en almacen_datos.
    """
    log.info("Transformación 1: Calculando resumen de sobrevivientes TITANIC...")

    df = almacen_datos.get("Titanic")
    if df is None or df.empty:
        log.warning("Dataset TITANIC no encontrado o vacío. Saltando transformación 1.")
        return almacen_datos

    # La columna de supervivencia se llama '2urvived' en este CSV
    col_survived = "2urvived"
    if col_survived not in df.columns:
        log.warning(f"Columna '{col_survived}' no encontrada en TITANIC.")
        return almacen_datos

    resumen = (
        df[col_survived]
        .value_counts()
        .reset_index()
    )
    resumen.columns = ["Sobrevivio", "Cantidad"]
    resumen["Sobrevivio"] = resumen["Sobrevivio"].map({1: "Sí sobrevivió", 0: "No sobrevivió"})

    almacen_datos["resumen_sobrevivientes"] = resumen

    log.info(f"Resumen generado: {resumen.to_dict(orient='records')}")
    return almacen_datos


# ─────────────────────────────────────────────
# TRANSFORMACIÓN 2
# Columna UniqueKey en la entrada Librería
# ─────────────────────────────────────────────
def agregar_unique_key(almacen_datos: dict) -> dict:
    """
    Crea una columna 'UniqueKey' en el dataset Libros combinando
    el campo 'key' de Open Library con un índice correlativo,
    asegurando un identificador único por fila.
    Actualiza la entrada 'Libros' en almacen_datos.
    """
    log.info("Transformación 2: Creando columna UniqueKey en Librería...")

    df = almacen_datos.get("Libros")
    if df is None or df.empty:
        log.warning("Dataset Libros no encontrado o vacío. Saltando transformación 2.")
        return almacen_datos

    # UniqueKey = índice correlativo + key de Open Library
    df = df.copy()
    df["UniqueKey"] = df.index.astype(str) + "_" + df["key"].str.replace("/works/", "", regex=False)

    almacen_datos["Libros"] = df

    log.info(f"UniqueKey generada para {len(df)} libros. Ejemplo: {df['UniqueKey'].iloc[0]}")
    return almacen_datos


# ─────────────────────────────────────────────
# TRANSFORMACIÓN 3
# Tabla resumen con promedio de temperatura en Clima
# ─────────────────────────────────────────────
def resumen_temperatura(almacen_datos: dict) -> dict:
    """
    Calcula el promedio de temperatura de todas las instantáneas
    del dataset Clima y lo almacena como nueva entrada
    'resumen_temperatura' en almacen_datos.
    """
    log.info("Transformación 3: Calculando promedio de temperatura en Clima...")

    df = almacen_datos.get("clima")
    if df is None or df.empty:
        log.warning("Dataset Clima no encontrado o vacío. Saltando transformación 3.")
        return almacen_datos

    col_temp = "temperature"
    if col_temp not in df.columns:
        log.warning(f"Columna '{col_temp}' no encontrada en Clima. Columnas disponibles: {list(df.columns)}")
        return almacen_datos

    resumen = pd.DataFrame([{
        "instantaneas_tomadas": len(df),
        "temperatura_promedio_C": round(df[col_temp].mean(), 2),
        "temperatura_max_C":     round(df[col_temp].max(), 2),
        "temperatura_min_C":     round(df[col_temp].min(), 2),
    }])

    almacen_datos["resumen_temperatura"] = resumen

    log.info(f"Temperatura promedio: {resumen['temperatura_promedio_C'].iloc[0]} °C")
    return almacen_datos


# ─────────────────────────────────────────────
# TRANSFORMACIÓN 4
# Borrar pasajeros menores de 10 años en TITANIC
# ─────────────────────────────────────────────
def filtrar_menores_10(almacen_datos: dict) -> dict:
    """
    Elimina del dataset TITANIC todas las filas de pasajeros
    cuya edad sea menor a 10 años.
    Actualiza la entrada 'Titanic' en almacen_datos.
    """
    log.info("Transformación 4: Eliminando pasajeros menores de 10 años de TITANIC...")

    df = almacen_datos.get("Titanic")
    if df is None or df.empty:
        log.warning("Dataset TITANIC no encontrado o vacío. Saltando transformación 4.")
        return almacen_datos

    col_age = "Age"
    if col_age not in df.columns:
        log.warning(f"Columna '{col_age}' no encontrada en TITANIC.")
        return almacen_datos

    total_antes = len(df)
    df_filtrado = df[df[col_age] >= 10].copy()
    eliminados = total_antes - len(df_filtrado)

    almacen_datos["Titanic"] = df_filtrado

    log.info(f"Registros eliminados (menores de 10 años): {eliminados} | Registros restantes: {len(df_filtrado)}")
    return almacen_datos


# ─────────────────────────────────────────────
# Función principal del módulo
# ─────────────────────────────────────────────
def aplicar_transformaciones(almacen_datos: dict) -> dict:
    """
    Ejecuta las 4 transformaciones en secuencia sobre el almacén de datos.
    Retorna el almacén actualizado con todas las entradas transformadas.
    """
    log.info("=== Iniciando etapa de TRANSFORMACIÓN ===")

    almacen_datos = resumen_sobrevivientes(almacen_datos)
    almacen_datos = agregar_unique_key(almacen_datos)
    almacen_datos = resumen_temperatura(almacen_datos)
    almacen_datos = filtrar_menores_10(almacen_datos)

    log.info("=== Transformaciones completadas ===")
    return almacen_datos
