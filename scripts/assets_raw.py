"""
Capa 1 – Carga de datos brutos (raw)
Cada asset lee un CSV sin transformar nada.
"""

import pandas as pd
from dagster import asset, MetadataValue

DATA_DIR = "./data/raw"


@asset(group_name="raw", description="Carga bruta del CSV de sector de ocupación por sección")
def ocupacion_raw() -> pd.DataFrame:
    df = pd.read_csv(f"{DATA_DIR}/ocupacion-sc-3.csv")
    return df


@asset(group_name="raw", description="Carga bruta del CSV de actividad económica por sección")
def actividad_raw() -> pd.DataFrame:
    df = pd.read_csv(f"{DATA_DIR}/actividad-sc-3.csv")
    return df


@asset(group_name="raw", description="Carga bruta del CSV de renta media y mediana por sección")
def rentamedia_raw() -> pd.DataFrame:
    df = pd.read_csv(f"{DATA_DIR}/rentamedia-sc-3.csv")
    return df


@asset(group_name="raw", description="Carga bruta de distribución de renta 2019-2023 (5 ficheros ISTAC concatenados)")
def distribucion_raw() -> pd.DataFrame:
    dfs = []
    for anyo in [2019, 2020, 2021, 2022, 2023]:
        path = f"{DATA_DIR}/distribucion-renta-{anyo}.csv"
        df = pd.read_csv(path)
        df["año_dato"] = anyo  # año del dato = año del ejercicio fiscal
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


@asset(group_name="raw", description="Carga bruta del CSV de relación con la actividad por sección (INE tabla 66796)")
def relacion_actividad_raw() -> pd.DataFrame:
    df = pd.read_csv(f"{DATA_DIR}/actividad-relacion.csv", sep=";")
    return df