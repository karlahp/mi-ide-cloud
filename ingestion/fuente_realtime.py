import requests
import pandas as pd

def leer_clima_tiempo_real():
    """
    Fuente de datos real-time. Trae los datos de Santiago.
    """
    url = "https://api.open-meteo.com/v1/forecast?latitude=-33.453654&longitude=-70.573846&current_weather=true"
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        # Extrae datos del dicionario
        data = response.json()['current_weather']
        return pd.DataFrame([data])
    except Exception as e:
        print(f"API del clima fallo: {e}")
        return pd.DataFrame()