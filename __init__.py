from dagster import Definitions, asset
import pandas as pd

@asset
def poblacion_test():
    """Asset de prueba con datos de población de Canarias"""
    data = {
        "isla": ["Tenerife", "Gran Canaria", "Lanzarote", "Fuerteventura"],
        "habitantes": [931646, 855521, 156112, 119732]
    }
    return pd.DataFrame(data)

@asset(deps=[poblacion_test])
def total_canarias(poblacion_test):
    """Calcula el total de habitantes de Canarias"""
    total = poblacion_test['habitantes'].sum()
    print(f"Total de habitantes en Canarias: {total}")
    return poblacion_test

defs = Definitions(
    assets=[poblacion_test, total_canarias]
)