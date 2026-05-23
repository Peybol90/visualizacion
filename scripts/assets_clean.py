"""
Capa 2 – Limpieza y transformación
Corrige tipos, espacios, comas decimales y añade columnas derivadas.
No imputa: los NaN de num_casos en actividad se eliminan.
"""

import pandas as pd
from dagster import asset

# Municipios de La Palma (códigos INE)
LA_PALMA_CODES = {
    38007, 38008, 38009, 38014, 38016, 38024, 38027,
    38029, 38030, 38033, 38037, 38045, 38047, 38053,
}

# Municipios directamente afectados por el volcán Tajogaite
ZONA_VOLCAN_CODES = {38027, 38045, 38024}  # El Paso, Tazacorte, Los Llanos de Aridane

# Municipio vecino/receptor principal de desplazados
ZONA_PALMA_RESTO = LA_PALMA_CODES - ZONA_VOLCAN_CODES


def _clasificar_zona(code: int) -> str:
    if code in ZONA_VOLCAN_CODES:
        return "Zona volcán"
    elif code in LA_PALMA_CODES:
        return "La Palma (resto)"
    else:
        return "Resto provincia"


@asset(group_name="clean", description="Ocupación limpia: tipos correctos, columna zona_volcán")
def ocupacion_clean(ocupacion_raw: pd.DataFrame) -> pd.DataFrame:
    df = ocupacion_raw.copy()
    df["municipio"] = df["municipio"].str.strip()
    df["sexo"] = df["sexo"].str.strip()
    df["ocupacion"] = df["ocupacion"].str.strip()
    df = df.dropna(subset=["num_casos"])
    df["zona"] = df["code_municipio"].apply(_clasificar_zona)
    df["es_lapalma"] = df["code_municipio"].isin(LA_PALMA_CODES)
    return df


@asset(group_name="clean", description="Actividad limpia: NaN eliminados, zona volcán etiquetada")
def actividad_clean(actividad_raw: pd.DataFrame) -> pd.DataFrame:
    df = actividad_raw.copy()
    df["municipio"] = df["municipio"].str.strip()
    df["Sexo"] = df["Sexo"].str.strip()
    df["Actividad económica"] = df["Actividad económica"].str.strip()
    # 163 NaN documentados en exploración – se eliminan (no imputar actividad)
    df = df.dropna(subset=["num_casos"])
    df["zona"] = df["cod_municipio"].apply(_clasificar_zona)
    df["es_lapalma"] = df["cod_municipio"].isin(LA_PALMA_CODES)
    df = df.rename(columns={"Periodo": "año", "Sexo": "sexo",
                             "Actividad económica": "actividad"})
    return df


@asset(group_name="clean", description="Renta media limpia: espacios en municipio")
def rentamedia_clean(rentamedia_raw: pd.DataFrame) -> pd.DataFrame:
    df = rentamedia_raw.copy()
    df["municipio"] = df["municipio"].str.strip()
    # TERRITORIO_CODE ya usa año+1 según la regla de la profesora (confirmado en exploración)
    # Ej: datos 2021 → TERRITORIO_CODE 20220101_... → join con secciones_20220101_tenerife.json
    df["geocode_join"] = df["TERRITORIO_CODE"]
    df["zona"] = df["municipio"].apply(
        lambda m: _clasificar_zona_nombre(m)
    )
    df["es_lapalma"] = df["zona"].isin(["Zona volcán", "La Palma (resto)"])
    return df


@asset(group_name="clean", description="Distribución de renta limpia: formato ISTAC normalizado, 2019-2023")
def distribucion_clean(distribucion_raw: pd.DataFrame) -> pd.DataFrame:
    df = distribucion_raw.copy()

    # Filtrar solo filas de sección (TERRITORIO_CODE con formato largo: 20YYYYMMDD_MMMMM_D##_S###)
    df = df[df["TERRITORIO_CODE"].str.match(r"^\d{8}_\d{5}_D\d{2}_S\d{3}$", na=False)].copy()

    # OBS_VALUE ya es float64 en el nuevo formato
    df["pct"] = df["OBS_VALUE"].astype(float)

    # Extraer municipio desde TERRITORIO#es: "Distrito 01, Sección 001 - Arrecife" → "Arrecife"
    df["municipio"] = df["TERRITORIO#es"].str.extract(r"-\s+(.+)$")[0].str.strip()

    # Sin desfase: el fichero etiquetado como año N contiene rentas del ejercicio N.
    # El TERRITORIO_CODE con prefijo año+1 indica solo el GeoJSON de secciones a usar,
    # no implica desfase en el año fiscal. Verificado empíricamente: pico COVID en fichero 2020.
    df["año"] = df["año_dato"]

    df["zona"] = df["municipio"].apply(_clasificar_zona_nombre)
    df["es_lapalma"] = df["zona"].isin(["Zona volcán", "La Palma (resto)"])

    cols = ["año", "municipio", "TERRITORIO_CODE", "MEDIDAS_CODE", "pct", "zona", "es_lapalma"]
    return df[cols].dropna(subset=["pct"])


@asset(group_name="clean", description="Relación con la actividad limpia: geocode construido, Total numérico, 2021-2024")
def relacion_actividad_clean(relacion_actividad_raw: pd.DataFrame) -> pd.DataFrame:
    df = relacion_actividad_raw.copy()

    # Filtrar solo filas con sección rellena
    df = df[df["Secciones"].notna() & (df["Secciones"] != "")].copy()

    # Convertir Total: eliminar puntos de miles → float
    df["num_casos"] = (
        df["Total"].astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .astype(float)
    )

    # Extraer código INE de 10 dígitos: "3800101001 Adeje sección 01001"
    df["cod_ine"] = df["Secciones"].str.extract(r"^(\d{10})")

    # Construir geocode compatible con GeoJSON: YYYYMMDD_MMMMM_D##_S###
    def _build_geocode(row):
        cod = str(row["cod_ine"])
        if len(cod) != 10:
            return None
        mun = cod[0:5]
        dis = cod[5:7]
        sec = cod[7:10]
        return f"{int(row['Periodo'])}0101_{mun}_D{dis}_S{sec}"

    df["geocode"] = df.apply(_build_geocode, axis=1)
    df["municipio"] = df["Municipios"].str.extract(r"^\d+\s+(.+)$")[0].str.strip()
    df["año"] = df["Periodo"].astype(int)
    df = df.rename(columns={
        "Relación con la actividad": "relacion",
        "País de nacimiento": "pais_nacimiento",
    })
    df["zona"] = df["municipio"].apply(_clasificar_zona_nombre)
    df["es_lapalma"] = df["zona"].isin(["Zona volcán", "La Palma (resto)"])

    cols = ["año", "municipio", "geocode", "pais_nacimiento", "relacion", "num_casos", "zona", "es_lapalma"]
    return df[cols].dropna(subset=["geocode"])


# ---------------------------------------------------------------------------
# Mapa inverso nombre → código para clasificar por nombre de municipio
# ---------------------------------------------------------------------------
_NOMBRE_A_ZONA = {}
for code in ZONA_VOLCAN_CODES:
    _NOMBRE_A_ZONA[code] = "Zona volcán"
for code in LA_PALMA_CODES - ZONA_VOLCAN_CODES:
    _NOMBRE_A_ZONA[code] = "La Palma (resto)"

_NOMBRE_MUN_LA_PALMA = {
    "Barlovento", "Breña Alta", "Breña Baja", "Fuencaliente de la Palma",
    "Fuencaliente de La Palma", "Garafía", "Llanos de Aridane, Los",
    "Los Llanos de Aridane", "Paso, El", "El Paso", "Puntagorda",
    "Puntallana", "San Andrés y Sauces", "Santa Cruz de la Palma",
    "Santa Cruz de La Palma", "Tazacorte", "Tijarafe", "Villa de Mazo",
}
_NOMBRE_ZONA_VOLCAN = {
    "Paso, El", "El Paso", "Tazacorte", "Los Llanos de Aridane", "Llanos de Aridane, Los"
}


def _clasificar_zona_nombre(nombre: str) -> str:
    """Clasificación por nombre cuando no hay código disponible."""
    if nombre in _NOMBRE_ZONA_VOLCAN:
        return "Zona volcán"
    elif nombre in _NOMBRE_MUN_LA_PALMA:
        return "La Palma (resto)"
    else:
        return "Resto provincia"