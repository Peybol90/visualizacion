import os
import re
import pandas as pd
from dagster import asset_check, AssetCheckResult, MetadataValue
from scripts.pipeline_renta import (
    renta_raw,
    codislas_raw,
    nivelestudios_raw,
    renta_limpia,
    codislas_limpio,
    renta_con_islas,
    grafico_barras_empleo,
    grafico_barras_desempleo,
    grafico_boxplot_empleo,
    grafico_scatter_estudios_empleo,
    grafico_mapa_municipios,
    codigo_generado_ia,
    visualizacion_ia,
)

# ─────────────────────────────────────────────────────────────────────────────
# CHECKS DE CARGA
# ─────────────────────────────────────────────────────────────────────────────

@asset_check(asset=renta_raw)
def check_renta_raw_no_vacio(renta_raw):
    """El CSV de rentas debe tener filas y las columnas mínimas esperadas."""
    n_filas = len(renta_raw)
    columnas_esperadas = {"TERRITORIO#es", "TERRITORIO_CODE", "OBS_VALUE", "MEDIDAS_CODE"}
    columnas_presentes = columnas_esperadas.issubset(set(renta_raw.columns))
    passed = n_filas > 0 and columnas_presentes

    return AssetCheckResult(
        passed=passed,
        metadata={
            "filas_cargadas": MetadataValue.int(n_filas),
            "columnas_esperadas_presentes": MetadataValue.text(str(columnas_presentes)),
            "columnas_encontradas": MetadataValue.text(str(list(renta_raw.columns))),
            "mensaje": MetadataValue.text(
                "OK" if passed else "El CSV está vacío o le faltan columnas clave."
            ),
        },
    )


@asset_check(asset=codislas_raw)
def check_codislas_raw_no_vacio(codislas_raw):
    """El CSV de islas debe tener filas y las columnas CPRO, CMUN, ISLA, NOMBRE."""
    n_filas = len(codislas_raw)
    columnas_esperadas = {"CPRO", "CMUN", "ISLA", "NOMBRE"}
    columnas_presentes = columnas_esperadas.issubset(set(codislas_raw.columns))
    passed = n_filas > 0 and columnas_presentes

    return AssetCheckResult(
        passed=passed,
        metadata={
            "filas_cargadas": MetadataValue.int(n_filas),
            "columnas_esperadas_presentes": MetadataValue.text(str(columnas_presentes)),
            "mensaje": MetadataValue.text(
                "OK" if passed else "El CSV de islas está vacío o le faltan columnas."
            ),
        },
    )


@asset_check(asset=nivelestudios_raw)
def check_nivelestudios_raw_no_vacio(nivelestudios_raw):
    """El Excel de estudios debe tener filas."""
    n_filas = len(nivelestudios_raw)
    passed = n_filas > 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "filas_cargadas": MetadataValue.int(n_filas),
            "columnas_encontradas": MetadataValue.text(str(list(nivelestudios_raw.columns))),
            "mensaje": MetadataValue.text(
                "OK" if passed else "El Excel de estudios está vacío."
            ),
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# CHECKS DE TRANSFORMACIÓN
# ─────────────────────────────────────────────────────────────────────────────

@asset_check(asset=renta_limpia)
def check_renta_limpia_codigos(renta_limpia):
    """Todos los códigos de municipio deben tener exactamente 5 dígitos numéricos."""
    mask_invalidos = ~renta_limpia["codigo"].str.match(r"^\d{5}$", na=True)
    n_invalidos = mask_invalidos.sum()
    passed = bool(int(n_invalidos) == 0)

    return AssetCheckResult(
        passed=passed,
        metadata={
            "codigos_invalidos": MetadataValue.int(int(n_invalidos)),
            "total_filas": MetadataValue.int(len(renta_limpia)),
            "ejemplos_invalidos": MetadataValue.text(
                str(renta_limpia.loc[mask_invalidos, "codigo"].head(5).tolist())
            ),
            "mensaje": MetadataValue.text(
                "OK" if passed else f"Hay {n_invalidos} códigos que no son de 5 dígitos."
            ),
        },
    )


@asset_check(asset=renta_limpia)
def check_renta_limpia_sin_nulos(renta_limpia):
    """Las columnas clave no deben tener valores nulos tras la limpieza."""
    columnas_clave = ["codigo", "medida_code", "valor", "anio"]
    nulos_por_col = renta_limpia[columnas_clave].isnull().sum().to_dict()
    total_nulos = sum(nulos_por_col.values())
    passed = bool(total_nulos == 0)

    return AssetCheckResult(
        passed=passed,
        metadata={
            "nulos_por_columna": MetadataValue.md(
                "\n".join([f"- `{col}`: {n}" for col, n in nulos_por_col.items()])
            ),
            "total_nulos": MetadataValue.int(total_nulos),
            "mensaje": MetadataValue.text(
                "OK" if passed else f"Hay {total_nulos} nulos en columnas clave."
            ),
        },
    )


@asset_check(asset=renta_limpia)
def check_renta_valores_porcentaje(renta_limpia):
    """Los valores deben estar entre 0 y 100 (son porcentajes de renta)."""
    fuera_rango = renta_limpia[(renta_limpia["valor"] < 0) | (renta_limpia["valor"] > 100)]
    n_fuera = len(fuera_rango)
    passed = bool(n_fuera == 0)

    return AssetCheckResult(
        passed=passed,
        metadata={
            "filas_fuera_de_rango": MetadataValue.int(n_fuera),
            "min_valor": MetadataValue.float(float(renta_limpia["valor"].min())),
            "max_valor": MetadataValue.float(float(renta_limpia["valor"].max())),
            "mensaje": MetadataValue.text(
                "OK" if passed else f"{n_fuera} valores fuera del rango [0, 100]."
            ),
        },
    )


@asset_check(asset=codislas_limpio)
def check_islas_normalizadas(codislas_limpio):
    """Los nombres de isla no deben tener el formato invertido (ej. 'Gomera, La')."""
    islas_con_coma = codislas_limpio[codislas_limpio["isla"].str.contains(",", na=False)]
    n_mal = len(islas_con_coma)
    passed = bool(n_mal == 0)

    return AssetCheckResult(
        passed=passed,
        metadata={
            "islas_con_formato_incorrecto": MetadataValue.int(n_mal),
            "ejemplos": MetadataValue.text(str(islas_con_coma["isla"].unique().tolist())),
            "mensaje": MetadataValue.text(
                "OK" if passed else f"Hay {n_mal} islas con formato 'Nombre, El/La'."
            ),
        },
    )


@asset_check(asset=renta_con_islas)
def check_join_cobertura(renta_con_islas):
    """El join no debe dejar más del 5% de municipios sin isla asignada."""
    total = len(renta_con_islas)
    sin_isla = renta_con_islas["isla"].isnull().sum()
    pct_sin_isla = float(round(sin_isla / total * 100, 2)) if total > 0 else 100.0
    passed = bool(pct_sin_isla <= 5.0)

    return AssetCheckResult(
        passed=passed,
        metadata={
            "total_filas": MetadataValue.int(total),
            "filas_sin_isla": MetadataValue.int(int(sin_isla)),
            "pct_sin_isla": MetadataValue.float(pct_sin_isla),
            "umbral_maximo": MetadataValue.text("5%"),
            "mensaje": MetadataValue.text(
                "OK" if passed else f"El {pct_sin_isla}% de filas no tiene isla."
            ),
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# CHECKS DE VISUALIZACIÓN — gráficos originales
# ─────────────────────────────────────────────────────────────────────────────

@asset_check(asset=grafico_barras_empleo)
def check_barras_empleo_existe(grafico_barras_empleo):
    """El archivo PNG del gráfico de empleo debe haberse generado."""
    existe = os.path.isfile(grafico_barras_empleo)
    return AssetCheckResult(
        passed=existe,
        metadata={
            "ruta": MetadataValue.path(grafico_barras_empleo),
            "mensaje": MetadataValue.text("OK" if existe else "El PNG no se ha generado."),
        },
    )


@asset_check(asset=grafico_barras_desempleo)
def check_barras_desempleo_existe(grafico_barras_desempleo):
    """El archivo PNG del gráfico de desempleo debe haberse generado."""
    existe = os.path.isfile(grafico_barras_desempleo)
    return AssetCheckResult(
        passed=existe,
        metadata={
            "ruta": MetadataValue.path(grafico_barras_desempleo),
            "mensaje": MetadataValue.text("OK" if existe else "El PNG no se ha generado."),
        },
    )


@asset_check(asset=grafico_boxplot_empleo)
def check_boxplot_empleo_existe(grafico_boxplot_empleo):
    """El archivo PNG del boxplot debe haberse generado."""
    existe = os.path.isfile(grafico_boxplot_empleo)
    return AssetCheckResult(
        passed=existe,
        metadata={
            "ruta": MetadataValue.path(grafico_boxplot_empleo),
            "mensaje": MetadataValue.text("OK" if existe else "El PNG no se ha generado."),
        },
    )


@asset_check(asset=grafico_scatter_estudios_empleo)
def check_scatter_existe(grafico_scatter_estudios_empleo):
    """El archivo PNG del scatter debe haberse generado."""
    existe = os.path.isfile(grafico_scatter_estudios_empleo)
    return AssetCheckResult(
        passed=existe,
        metadata={
            "ruta": MetadataValue.path(grafico_scatter_estudios_empleo),
            "mensaje": MetadataValue.text("OK" if existe else "El PNG no se ha generado."),
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# CHECKS NUEVOS — práctica 4
# ─────────────────────────────────────────────────────────────────────────────

@asset_check(asset=grafico_mapa_municipios)
def check_mapa_municipios_existe(grafico_mapa_municipios):
    """El mapa coroplético de municipios debe haberse generado correctamente."""
    existe = os.path.isfile(grafico_mapa_municipios)
    tamanio_bytes = os.path.getsize(grafico_mapa_municipios) if existe else 0
    # Un PNG válido de un mapa debe pesar al menos 50 KB
    tamanio_ok = tamanio_bytes > 50_000
    passed = existe and tamanio_ok

    return AssetCheckResult(
        passed=passed,
        metadata={
            "ruta": MetadataValue.path(grafico_mapa_municipios),
            "tamanio_bytes": MetadataValue.int(tamanio_bytes),
            "mensaje": MetadataValue.text(
                "OK" if passed
                else f"Archivo inexistente o demasiado pequeño ({tamanio_bytes} bytes)."
            ),
        },
    )


@asset_check(asset=codigo_generado_ia)
def check_codigo_ia_valido(codigo_generado_ia):
    """
    El código generado por el LLM debe:
    1. Contener la función 'generar_plot'.
    2. No tener bloques markdown residuales.
    3. Ser compilable (sintaxis Python válida).
    """
    tiene_funcion = "def generar_plot" in codigo_generado_ia
    sin_markdown = "```" not in codigo_generado_ia

    sintaxis_ok = False
    error_sintaxis = ""
    try:
        compile(codigo_generado_ia, "<llm_output>", "exec")
        sintaxis_ok = True
    except SyntaxError as e:
        error_sintaxis = str(e)

    passed = tiene_funcion and sin_markdown and sintaxis_ok

    return AssetCheckResult(
        passed=passed,
        metadata={
            "tiene_funcion_generar_plot": MetadataValue.bool(tiene_funcion),
            "sin_markdown_residual": MetadataValue.bool(sin_markdown),
            "sintaxis_python_valida": MetadataValue.bool(sintaxis_ok),
            "error_sintaxis": MetadataValue.text(error_sintaxis or "ninguno"),
            "longitud_codigo": MetadataValue.int(len(codigo_generado_ia)),
            "mensaje": MetadataValue.text(
                "OK" if passed
                else "El código generado por el LLM no cumple los requisitos mínimos."
            ),
        },
    )


@asset_check(asset=visualizacion_ia)
def check_visualizacion_ia_existe(visualizacion_ia):
    """El PNG generado por el código de la IA debe existir y tener tamaño razonable."""
    existe = os.path.isfile(visualizacion_ia)
    tamanio_bytes = os.path.getsize(visualizacion_ia) if existe else 0
    tamanio_ok = tamanio_bytes > 10_000  # > 10 KB
    passed = existe and tamanio_ok

    return AssetCheckResult(
        passed=passed,
        metadata={
            "ruta": MetadataValue.path(visualizacion_ia),
            "tamanio_bytes": MetadataValue.int(tamanio_bytes),
            "mensaje": MetadataValue.text(
                "OK" if passed
                else f"PNG inexistente o vacío ({tamanio_bytes} bytes)."
            ),
        },
    )
