import pandas as pd
from dagster import asset, asset_check, AssetCheckResult, MetadataValue

@asset
def islas_raw():
    df = pd.read_csv("./data/distribucion-renta-canarias.csv", sep=",", encoding="utf-8")
    #EJERCICIO 6
    # Añadimos una fila "sucia" para forzar el fallo del check
    '''
    fila_sucia = pd.DataFrame({
        "TERRITORIO#es": ["tenerife"],
        "TERRITORIO_CODE": ["99999"],
        "TIME_PERIOD#es": [2022],
        "TIME_PERIOD_CODE": [2022],
        "MEDIDAS#es": ["gasto"],
        "MEDIDAS_CODE": ["GASTO"],
        "OBS_VALUE": [1840000]
    })
    return pd.concat([df, fila_sucia], ignore_index=True)
    '''
    return df

@asset_check(asset=islas_raw)
def check_estandarizacion_islas(islas_raw):
    col = "TERRITORIO#es"
    # Contamos categorías únicas originales vs normalizadas
    originales = islas_raw[col].nunique()
    normalizadas = islas_raw[col].str.capitalize().nunique()

    passed = originales == normalizadas

    return AssetCheckResult(
        passed=passed,
        metadata={
            "categorias_detectadas": MetadataValue.int(originales),
            "categorias_esperadas": MetadataValue.int(normalizadas),
            "principio_gestalt": "Similitud (Evitar fragmentación visual)",
            "mensaje": "Si hay nombres inconsistentes, ggplot creará leyendas duplicadas."
        }
    )