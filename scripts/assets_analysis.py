"""
Capa 3 – Agregaciones analíticas
Produce DataFrames listos para visualizar.
Un asset por pregunta analítica de la historia.
"""

import pandas as pd
from dagster import asset


@asset(group_name="analysis",
       description="Acto 1: Renta bruta media por zona (volcán / La Palma resto / provincia) y año")
def renta_por_zona(rentamedia_clean: pd.DataFrame) -> pd.DataFrame:
    df = rentamedia_clean[rentamedia_clean["MEDIDAS_CODE"] == "RENTA_BRUTA_MEDIA_HOGAR"].copy()
    agg = (
        df.groupby(["año", "zona"])["OBS_VALUE"]
        .mean()
        .reset_index()
        .rename(columns={"OBS_VALUE": "renta_media"})
    )
    return agg


@asset(group_name="analysis",
       description="Acto 1: Distribución de fuentes de renta (%) en zona volcán vs resto, 2019-2023")
def distribucion_por_zona(distribucion_clean: pd.DataFrame) -> pd.DataFrame:
    agg = (
        distribucion_clean
        .groupby(["año", "zona", "MEDIDAS_CODE"])["pct"]
        .mean()
        .reset_index()
    )
    # Etiqueta legible para la leyenda
    labels = {
        "SUELDOS_SALARIOS": "Sueldos y salarios",
        "PENSIONES": "Pensiones",
        "PRESTACIONES_DESEMPLEO": "Prest. desempleo",
        "OTRAS_PRESTACIONES": "Otras prestaciones",
        "OTROS_INGRESOS": "Otros ingresos",
    }
    agg["fuente"] = agg["MEDIDAS_CODE"].map(labels)
    return agg


@asset(group_name="analysis",
       description="Acto 2: Cambio en actividad económica en zona volcán 2021→2023")
def actividad_zona_volcan(actividad_clean: pd.DataFrame) -> pd.DataFrame:
    df = actividad_clean[actividad_clean["zona"] == "Zona volcán"].copy()
    agg = (
        df.groupby(["año", "actividad"])["num_casos"]
        .sum()
        .reset_index()
    )
    # Calcular porcentaje dentro de cada año
    total_anyo = agg.groupby("año")["num_casos"].transform("sum")
    agg["pct"] = (agg["num_casos"] / total_anyo * 100).round(1)
    return agg


@asset(group_name="analysis",
       description="Acto 2: Construcción post-volcán — índice por municipio de La Palma (base 2021=100)")
def construccion_comparada(actividad_clean: pd.DataFrame) -> pd.DataFrame:
    df = actividad_clean[
        (actividad_clean["es_lapalma"])
        & (actividad_clean["actividad"] == "Construcción")
    ].copy()

    agg = (
        df.groupby(["año", "municipio", "zona"])["num_casos"]
        .sum()
        .reset_index()
    )

    # Índice base 2021 = 100 por municipio
    base = agg[agg["año"] == 2021].set_index("municipio")["num_casos"]
    # Solo municipios que tienen datos en 2021
    municipios_validos = base[base > 0].index
    agg = agg[agg["municipio"].isin(municipios_validos)].copy()
    agg["indice"] = agg.apply(
        lambda r: round(r["num_casos"] / base[r["municipio"]] * 100, 1), axis=1
    )
    return agg


@asset(group_name="analysis",
       description="Acto 3: % prestaciones de desempleo por sección en zona volcán, evolución 2019-2023")
def desempleo_seccion_volcan(distribucion_clean: pd.DataFrame) -> pd.DataFrame:
    df = distribucion_clean[
        (distribucion_clean["zona"] == "Zona volcán")
        & (distribucion_clean["MEDIDAS_CODE"] == "PRESTACIONES_DESEMPLEO")
    ].copy()
    return df[["año", "municipio", "TERRITORIO_CODE", "pct", "zona"]]


@asset(group_name="analysis",
       description="Mapa 2: % prestaciones desempleo por sección en TODA La Palma, 2019-2023")
def desempleo_seccion_lapalma(distribucion_clean: pd.DataFrame) -> pd.DataFrame:
    df = distribucion_clean[
        (distribucion_clean["es_lapalma"])
        & (distribucion_clean["MEDIDAS_CODE"] == "PRESTACIONES_DESEMPLEO")
    ].copy()
    return df[["año", "municipio", "TERRITORIO_CODE", "pct", "zona"]]


@asset(group_name="analysis",
       description="Acto 3: Ocupación cualificada vs elemental en zona volcán 2021-2023")
def ocupacion_calidad_volcan(ocupacion_clean: pd.DataFrame) -> pd.DataFrame:
    df = ocupacion_clean[ocupacion_clean["zona"] == "Zona volcán"].copy()
    # Simplificar categorías
    mapa = {
        "Directores/gerentes y profesionales/técnicos de nivel medio o alto": "Cualificada",
        "Trabajadores cualificados y oficiales/operarios de nivel bajo": "Semi-cualificada",
        "Ocupaciones elementales": "Elemental",
        "No consta": "No consta",
    }
    df["tipo_ocup"] = df["ocupacion"].map(mapa)
    agg = (
        df.groupby(["año", "municipio", "tipo_ocup"])["num_casos"]
        .sum()
        .reset_index()
    )
    total = agg.groupby(["año", "municipio"])["num_casos"].transform("sum")
    agg["pct"] = (agg["num_casos"] / total * 100).round(1)
    return agg


@asset(group_name="analysis",
       description="Acto 4: Renta media normalizada (índice 2021=100) para las tres zonas")
def renta_indice_recuperacion(renta_por_zona: pd.DataFrame) -> pd.DataFrame:
    base = renta_por_zona[renta_por_zona["año"] == 2021].set_index("zona")["renta_media"]
    df = renta_por_zona.copy()
    df["indice"] = df.apply(
        lambda r: round(r["renta_media"] / base[r["zona"]] * 100, 1), axis=1
    )
    return df


@asset(group_name="analysis",
       description="Acto 4: Tasa de paro por zona 2021-2024 (relación con actividad)")
def tasa_paro_zona(relacion_actividad_clean: pd.DataFrame) -> pd.DataFrame:
    df = relacion_actividad_clean[
        relacion_actividad_clean["pais_nacimiento"] == "Total"
    ].copy()

    # Calcular tasa de paro = parados / (ocupados + parados) * 100
    ocupados = df[df["relacion"] == "Ocupado/a"].groupby(["año", "zona"])["num_casos"].sum()
    parados  = df[df["relacion"] == "Parado/a"].groupby(["año", "zona"])["num_casos"].sum()

    tasa = (parados / (ocupados + parados) * 100).reset_index()
    tasa.columns = ["año", "zona", "tasa_paro"]
    tasa["tasa_paro"] = tasa["tasa_paro"].round(1)
    return tasa
    df = rentamedia_clean[
        (rentamedia_clean["es_lapalma"])
        & (rentamedia_clean["MEDIDAS_CODE"] == "RENTA_BRUTA_MEDIA_HOGAR")
    ].copy()
    return df[["año", "municipio", "seccion", "geocode_join", "OBS_VALUE", "zona"]]