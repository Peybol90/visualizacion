"""
Capa de calidad – Asset Checks (15 checks en 3 capas)
Todos los valores comparativos se convierten a tipos Python nativos
para evitar errores de serialización de numpy en Dagster.
"""

import os
import pandas as pd
from dagster import asset_check, AssetCheckResult

from scripts.assets_raw import (
    ocupacion_raw, actividad_raw, rentamedia_raw, distribucion_raw,
    relacion_actividad_raw,
)
from scripts.assets_clean import (
    ocupacion_clean, actividad_clean, rentamedia_clean, distribucion_clean,
    relacion_actividad_clean,
)
from scripts.assets_viz import (
    viz_renta_por_zona, viz_actividad_zona_volcan,
    viz_construccion_indice, viz_desempleo_evolucion,
    viz_renta_recuperacion, viz_distribucion_volcan_cambio,
)
from scripts.assets_maps import (
    mapa_renta_lapalma_2022, mapa_desempleo_volcan_2022, mapa_construccion_lapalma_2023,
)


OUTPUT_DIR = "./output"

# ─────────────────────────────────────────────────────────────────────────────
# CAPA 1 – Carga (raw)
# ─────────────────────────────────────────────────────────────────────────────

@asset_check(asset=ocupacion_raw)
def check_ocupacion_raw_no_vacio(ocupacion_raw: pd.DataFrame) -> AssetCheckResult:
    cols_esperadas = {"ocupacion", "code_municipio", "año", "sexo", "num_casos", "geocode"}
    presentes = cols_esperadas.issubset(set(ocupacion_raw.columns))
    passed = bool(len(ocupacion_raw) > 0 and presentes)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "filas": int(len(ocupacion_raw)),
            "columnas_ok": bool(presentes),
            "columnas_faltantes": list(cols_esperadas - set(ocupacion_raw.columns)),
        },
    )


@asset_check(asset=actividad_raw)
def check_actividad_raw_no_vacio(actividad_raw: pd.DataFrame) -> AssetCheckResult:
    cols_esperadas = {"Actividad económica", "cod_municipio", "Periodo", "num_casos", "geocode"}
    presentes = cols_esperadas.issubset(set(actividad_raw.columns))
    passed = bool(len(actividad_raw) > 0 and presentes)
    return AssetCheckResult(
        passed=passed,
        metadata={"filas": int(len(actividad_raw)), "columnas_ok": bool(presentes)},
    )


@asset_check(asset=rentamedia_raw)
def check_rentamedia_raw_no_vacio(rentamedia_raw: pd.DataFrame) -> AssetCheckResult:
    cols_esperadas = {"año", "MEDIDAS_CODE", "TERRITORIO_CODE", "OBS_VALUE"}
    presentes = cols_esperadas.issubset(set(rentamedia_raw.columns))
    passed = bool(len(rentamedia_raw) > 0 and presentes)
    return AssetCheckResult(
        passed=passed,
        metadata={"filas": int(len(rentamedia_raw)), "columnas_ok": bool(presentes)},
    )


@asset_check(asset=distribucion_raw)
def check_distribucion_raw_no_vacio(distribucion_raw: pd.DataFrame) -> AssetCheckResult:
    # Nuevo formato ISTAC: columnas TERRITORIO_CODE, MEDIDAS_CODE, OBS_VALUE, año_dato
    cols_esperadas = {"TERRITORIO_CODE", "MEDIDAS_CODE", "OBS_VALUE", "año_dato"}
    presentes = cols_esperadas.issubset(set(distribucion_raw.columns))
    # OBS_VALUE ahora es float64 (nuevo formato sin comas decimales)
    tipo_correcto = bool(distribucion_raw["OBS_VALUE"].dtype in ["float64", "float32"])
    # Debe tener los 5 años
    años_ok = bool(set(distribucion_raw["año_dato"].unique()) >= {2019, 2020, 2021, 2022, 2023})
    passed = bool(len(distribucion_raw) > 0 and presentes and tipo_correcto and años_ok)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "filas": int(len(distribucion_raw)),
            "columnas_ok": bool(presentes),
            "obs_value_es_float": bool(tipo_correcto),
            "años_completos": bool(años_ok),
        },
    )


@asset_check(asset=relacion_actividad_raw)
def check_relacion_actividad_raw_no_vacio(relacion_actividad_raw: pd.DataFrame) -> AssetCheckResult:
    """Verifica columnas clave, años 2021-2024 y que hay filas de sección."""
    cols_esperadas = {"Provincias", "Municipios", "Secciones",
                      "País de nacimiento", "Relación con la actividad", "Periodo", "Total"}
    presentes = cols_esperadas.issubset(set(relacion_actividad_raw.columns))
    años_ok = bool(set(relacion_actividad_raw["Periodo"].unique()) >= {2021, 2022, 2023, 2024})
    filas_seccion = int((relacion_actividad_raw["Secciones"].notna() &
                         (relacion_actividad_raw["Secciones"] != "")).sum())
    passed = bool(len(relacion_actividad_raw) > 0 and presentes and años_ok and filas_seccion > 0)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "filas": int(len(relacion_actividad_raw)),
            "filas_con_seccion": filas_seccion,
            "columnas_ok": bool(presentes),
            "años_completos": bool(años_ok),
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# CAPA 2 – Transformación (clean)
# ─────────────────────────────────────────────────────────────────────────────

@asset_check(asset=ocupacion_clean)
def check_ocupacion_sin_nulos(ocupacion_clean: pd.DataFrame) -> AssetCheckResult:
    nulos = int(ocupacion_clean[["año", "geocode", "num_casos", "zona"]].isnull().sum().sum())
    passed = bool(nulos == 0)
    return AssetCheckResult(passed=passed, metadata={"nulos_clave": nulos})


@asset_check(asset=actividad_clean)
def check_actividad_sin_nulos(actividad_clean: pd.DataFrame) -> AssetCheckResult:
    nulos = int(actividad_clean[["año", "geocode", "num_casos", "zona"]].isnull().sum().sum())
    passed = bool(nulos == 0)
    return AssetCheckResult(passed=passed, metadata={"nulos_clave": nulos})


@asset_check(asset=rentamedia_clean)
def check_rentamedia_sin_nulos(rentamedia_clean: pd.DataFrame) -> AssetCheckResult:
    """
    OBS_VALUE puede tener NaN por confidencialidad estadística del ISTAC
    (secciones con pocos hogares). Se permite hasta un 5% de nulos.
    """
    total = len(rentamedia_clean)
    nulos = int(rentamedia_clean["OBS_VALUE"].isna().sum())
    pct_nulos = round(nulos / total * 100, 1) if total > 0 else 0
    # Nulos en año, geocode_join y zona nunca deben existir
    nulos_clave = int(rentamedia_clean[["año", "geocode_join", "zona"]].isnull().sum().sum())
    passed = bool(nulos_clave == 0 and pct_nulos <= 5.0)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "nulos_obs_value": nulos,
            "pct_nulos_obs_value": float(pct_nulos),
            "nulos_columnas_clave": nulos_clave,
        },
    )


@asset_check(asset=relacion_actividad_clean)
def check_relacion_actividad_clean(relacion_actividad_clean: pd.DataFrame) -> AssetCheckResult:
    """Verifica geocode construido, años 2021-2024, num_casos numérico y zonas presentes."""
    nulos_geo = int(relacion_actividad_clean["geocode"].isna().sum())
    años_ok = bool(set(relacion_actividad_clean["año"].unique()) >= {2021, 2022, 2023, 2024})
    es_float = bool(relacion_actividad_clean["num_casos"].dtype in ["float64", "float32"])
    zonas_ok = bool({"Zona volcán", "La Palma (resto)", "Resto provincia"}.issubset(
        set(relacion_actividad_clean["zona"].unique())
    ))
    passed = bool(nulos_geo == 0 and años_ok and es_float and zonas_ok)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "nulos_geocode": nulos_geo,
            "años_completos": bool(años_ok),
            "num_casos_float": bool(es_float),
            "zonas_ok": bool(zonas_ok),
        },
    )


@asset_check(asset=distribucion_clean)
def check_distribucion_pct_rango(distribucion_clean: pd.DataFrame) -> AssetCheckResult:
    """El campo pct son porcentajes: deben estar en [0, 100]."""
    fuera = int(((distribucion_clean["pct"] < 0) | (distribucion_clean["pct"] > 100)).sum())
    passed = bool(fuera == 0)
    return AssetCheckResult(
        passed=passed,
        metadata={"valores_fuera_rango": fuera, "rango": "[0, 100]"},
    )


@asset_check(asset=distribucion_clean)
def check_distribucion_coma_corregida(distribucion_clean: pd.DataFrame) -> AssetCheckResult:
    """Verifica que la conversión coma→punto se realizó: pct debe ser float, no string."""
    es_float = bool(distribucion_clean["pct"].dtype in ["float64", "float32"])
    passed = es_float
    return AssetCheckResult(
        passed=passed,
        metadata={"dtype_pct": str(distribucion_clean["pct"].dtype)},
    )


@asset_check(asset=rentamedia_clean)
def check_rentamedia_geocode_desfase(rentamedia_clean: pd.DataFrame) -> AssetCheckResult:
    """
    Verifica la regla de desfase: datos 2021 → TERRITORIO_CODE empieza por 2022...
    Para cada año en [2021,2022,2023], el prefijo de TERRITORIO_CODE debe ser año+1.
    """
    errores = 0
    for anyo in [2021, 2022, 2023]:
        subset = rentamedia_clean[rentamedia_clean["año"] == anyo]
        if len(subset) == 0:
            continue
        prefijo_esperado = str(anyo + 1)
        mal = int((~subset["geocode_join"].str.startswith(prefijo_esperado)).sum())
        errores += mal
    passed = bool(errores == 0)
    return AssetCheckResult(
        passed=passed,
        metadata={"filas_con_prefijo_incorrecto": errores},
    )


@asset_check(asset=ocupacion_clean)
def check_zonas_cubiertas(ocupacion_clean: pd.DataFrame) -> AssetCheckResult:
    """Las tres zonas narrativas deben estar presentes en los datos."""
    zonas = set(ocupacion_clean["zona"].unique())
    esperadas = {"Zona volcán", "La Palma (resto)", "Resto provincia"}
    faltantes = list(esperadas - zonas)
    passed = bool(len(faltantes) == 0)
    return AssetCheckResult(
        passed=passed,
        metadata={"zonas_presentes": list(zonas), "zonas_faltantes": faltantes},
    )


# ─────────────────────────────────────────────────────────────────────────────
# CAPA 3 – Visualización (archivos generados)
# ─────────────────────────────────────────────────────────────────────────────

def _png_result(ruta: str) -> AssetCheckResult:
    """Lógica compartida: verifica existencia y tamaño mínimo (10 KB) del PNG."""
    existe = bool(os.path.isfile(ruta))
    tam = int(os.path.getsize(ruta)) if existe else 0
    passed = bool(existe and tam > 10_000)
    return AssetCheckResult(
        passed=passed,
        metadata={"ruta": ruta, "existe": existe, "bytes": tam},
    )


@asset_check(asset=viz_renta_por_zona)
def check_viz_renta_zona(viz_renta_por_zona: str) -> AssetCheckResult:
    return _png_result(viz_renta_por_zona)


@asset_check(asset=viz_actividad_zona_volcan)
def check_viz_actividad_volcan(viz_actividad_zona_volcan: str) -> AssetCheckResult:
    return _png_result(viz_actividad_zona_volcan)


@asset_check(asset=viz_construccion_indice)
def check_viz_construccion(viz_construccion_indice: str) -> AssetCheckResult:
    return _png_result(viz_construccion_indice)


@asset_check(asset=viz_desempleo_evolucion)
def check_viz_desempleo(viz_desempleo_evolucion: str) -> AssetCheckResult:
    return _png_result(viz_desempleo_evolucion)


@asset_check(asset=viz_renta_recuperacion)
def check_viz_recuperacion(viz_renta_recuperacion: str) -> AssetCheckResult:
    return _png_result(viz_renta_recuperacion)


@asset_check(asset=viz_distribucion_volcan_cambio)
def check_viz_cambio(viz_distribucion_volcan_cambio: str) -> AssetCheckResult:
    return _png_result(viz_distribucion_volcan_cambio)


@asset_check(asset=mapa_renta_lapalma_2022)
def check_mapa_renta(mapa_renta_lapalma_2022: str) -> AssetCheckResult:
    return _png_result(mapa_renta_lapalma_2022)


@asset_check(asset=mapa_desempleo_volcan_2022)
def check_mapa_desempleo(mapa_desempleo_volcan_2022: str) -> AssetCheckResult:
    return _png_result(mapa_desempleo_volcan_2022)


@asset_check(asset=mapa_construccion_lapalma_2023)
def check_mapa_construccion(mapa_construccion_lapalma_2023: str) -> AssetCheckResult:
    return _png_result(mapa_construccion_lapalma_2023)