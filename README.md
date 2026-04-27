# mi-ide-cloud
## Pipeline de Datos – Gestión de Datos para IA (ITY1101)

## Descripción
Pipeline de datos modular y automatizado que cubre las etapas de ingesta y transformación de datos, siguiendo principios de DataOps: trazabilidad, modularidad y control de versiones.

## Estructura del proyecto

mi-ide-cloud/
├── ingestion/
│   ├── lectura_csv.py        # Lee el dataset Titanic desde CSV local
│   ├── leer_batch.py         # Obtiene libros desde API Open Library
│   └── fuente_realtime.py    # Obtiene clima en tiempo real desde API Open-Meteo
├── procesamiento/
│   └── transformacion.py     # Las 4 transformaciones del dataset
├── data/
│   └── processed/
│       └── titanic_limpio.csv  # Dataset Titanic ya transformado
├── Titanic.csv               # Dataset original
├── pipeline.py               # Orquestador principal
├── test.ipynb                # Notebook de pruebas
└── ingesta.log               # Registro de ejecución del pipeline


## Cómo ejecutar
1. Instalar dependencias:
```bash
pip install pandas requests
```
2. Ejecutar el pipeline:
```bash
python pipeline.py
```

## Transformaciones aplicadas
1. **Resumen de sobrevivientes** – Tabla con conteo de pasajeros que sobrevivieron y no sobrevivieron en el Titanic.
2. **UniqueKey en Librería** – Nueva columna con identificador único por libro obtenido desde Open Library.
3. **Resumen de temperatura** – Tabla con promedio, máximo y mínimo de temperatura de las instantáneas del clima de Santiago.
4. **Filtrado menores de 10 años** – Eliminación de pasajeros menores de 10 años del dataset Titanic.

## Trazabilidad
Cada ejecución queda registrada en `ingesta.log` con timestamps y estado de cada etapa.