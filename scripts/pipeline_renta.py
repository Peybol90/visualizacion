import os
import pandas as pd
from plotnine import (
    ggplot, aes, geom_col, geom_point, geom_text, coord_flip, geom_boxplot,
    theme_minimal, labs, theme, scale_fill_brewer
)
from dagster import asset, MetadataValue

# ── ASSET 1: Carga del CSV de rentas ─────────────────────────────────────────
@asset
def renta_raw():
    df = pd.read_csv("./data/distribucion-renta-canarias.csv", sep=",", encoding="utf-8")

    # PASO 8 — FALLO CAPA CARGA: renombrar columna esperada por el check
    # check_renta_raw_no_vacio busca "OBS_VALUE" — al renombrarla, el check falla
    # df = df.rename(columns={"OBS_VALUE": "OBS_VALUE_RENAMED"})

    return df

# ── ASSET 2: Carga de codislas ────────────────────────────────────────────────
@asset
def codislas_raw():
    df = pd.read_csv("./data/codislas.csv", sep=";", encoding="latin-1")
    return df

# ── ASSET 3: Limpieza ─────────────────────────────────────────────────────────
@asset
def renta_limpia(renta_raw):
    df = renta_raw.copy()

    # Renombrar columnas a nombres manejables
    df = df.rename(columns={
        "TERRITORIO#es":   "territorio",
        "TERRITORIO_CODE": "codigo",
        "TIME_PERIOD#es":  "anio",
        "TIME_PERIOD_CODE":"anio_code",
        "MEDIDAS#es":      "medida",
        "MEDIDAS_CODE":    "medida_code",
        "OBS_VALUE":       "valor"
    })

    # Quedarse solo con municipios (códigos numéricos de 5 dígitos, sin agregados ES7xx)
    df = df[df["codigo"].str.match(r"^\d{5}$", na=False)]

    # Eliminar filas sin valor
    df = df.dropna(subset=["valor"])

    # PASO 8 — FALLO CAPA TRANSFORMACIÓN: inyectar valor fuera de rango
    # check_renta_valores_porcentaje exige valores entre 0 y 100
    '''
    fila_sucia = pd.DataFrame({
        "territorio": ["Municipio Ficticio"],
        "codigo": ["99999"],
        "anio": [df["anio"].max()],
        "anio_code": [df["anio_code"].max()],
        "medida": ["Sueldos y salarios"],
        "medida_code": ["SUELDOS_SALARIOS"],
        "valor": [999.0]   # ← fuera del rango [0, 100]
    })
    df = pd.concat([df, fila_sucia], ignore_index=True)
    '''

    return df

# ── ASSET 4: Preparar codislas para el join ───────────────────────────────────
@asset
def codislas_limpio(codislas_raw):
    df = codislas_raw.copy()
    df["codigo"] = df["CPRO"].astype(str).str.zfill(2) + df["CMUN"].astype(str).str.zfill(3)
    df = df.rename(columns={"ISLA": "isla", "NOMBRE": "municipio"})

    # Quitar espacios y corregir nombres
    df["isla"] = df["isla"].str.strip().replace({
        "Gomera, La": "La Gomera",
        "Palma, La": "La Palma",
        "Hierro, El": "El Hierro"
    })

    # PASO 8 — FALLO CAPA TRANSFORMACIÓN: dejar nombre con formato incorrecto
    # check_islas_normalizadas detecta nombres con coma (ej. "Gomera, La")
    # df["isla"] = df["isla"].replace({"La Gomera": "Gomera, La"})

    return df[["codigo", "isla", "municipio"]]

# ── ASSET 5: Join renta + islas ───────────────────────────────────────────────
@asset
def renta_con_islas(renta_limpia, codislas_limpio):
    df = renta_limpia.merge(codislas_limpio, on="codigo", how="left")
    return df

# ── ASSET 6: Visualización ────────────────────────────────────────────────────
@asset
def grafico_barras_empleo(renta_con_islas):
    df = renta_con_islas.copy()

    ultimo_anio = df["anio"].max()
    df_filtrado = df[
        (df["anio"] == ultimo_anio) &
        (df["medida_code"] == "SUELDOS_SALARIOS")
    ].dropna(subset=["municipio"])

    df_top = df_filtrado.nlargest(20, "valor")

    plot = (
        ggplot(df_top, aes(x="reorder(municipio, valor)", y="valor", fill="isla"))
        + geom_col()
        + coord_flip()
        + theme_minimal()
        + scale_fill_brewer(type="qual", palette="Set2")
        + labs(
            title=f"Sueldos y Salarios en Canarias ({ultimo_anio})",
            subtitle="Top 20 municipios · Fuente: ISTAC",
            x="Municipio",
            y="% renta procedente de sueldos y salarios",
            fill="Isla"
        )
        + theme(figure_size=(10, 7))
    )

    os.makedirs("./output", exist_ok=True)
    plot.save("./output/barras_empleo.png", dpi=150)

    # PASO 8 — FALLO CAPA VISUALIZACIÓN: devolver ruta incorrecta
    # check_barras_empleo_existe busca el PNG en "./output/barras_empleo.png"
    # return "./output/ruta_incorrecta.png"

    return "./output/barras_empleo.png"


@asset
def grafico_barras_desempleo(renta_con_islas):
    df = renta_con_islas.copy()

    ultimo_anio = df["anio"].max()
    df_filtrado = df[
        (df["anio"] == ultimo_anio) &
        (df["medida_code"] == "PRESTACIONES_DESEMPLEO")
    ].dropna(subset=["municipio"])

    df_top = df_filtrado.nlargest(20, "valor")

    plot = (
        ggplot(df_top, aes(x="reorder(municipio, valor)", y="valor", fill="isla"))
        + geom_col()
        + coord_flip()
        + theme_minimal()
        + scale_fill_brewer(type="qual", palette="Set2")
        + labs(
            title=f"Prestaciones por desempleo en Canarias ({ultimo_anio})",
            subtitle="Top 20 municipios · Fuente: ISTAC",
            x="% renta procedente de prestaciones por desempleo",
            y="Municipio",
            fill="Isla"
        )
        + theme(figure_size=(10, 7))
    )

    os.makedirs("./output", exist_ok=True)
    plot.save("./output/barras_desempleo.png", dpi=150)
    return "./output/barras_desempleo.png"


@asset
def nivelestudios_raw():
    df = pd.read_excel("./data/nivelestudios.xlsx")
    return df


@asset
def grafico_scatter_estudios_empleo(renta_con_islas, nivelestudios_raw):
    df_est = nivelestudios_raw.copy()
    df_est = df_est.rename(columns={
        "Municipios de 500 habitantes o más": "municipio_raw",
        "Nivel de estudios en curso": "nivel_estudios",
        "Total": "total"
    })
    df_est["codigo"] = df_est["municipio_raw"].str[:5].str.strip()
    df_est["total"] = pd.to_numeric(df_est["total"], errors="coerce")

    df_uni = df_est[df_est["nivel_estudios"] == "No cursa estudios"].groupby("codigo")["total"].sum().reset_index()
    df_uni = df_uni.rename(columns={"total": "no_estudiantes"})

    df_total = df_est[df_est["nivel_estudios"] == "Total"].groupby("codigo")["total"].sum().reset_index()
    df_total = df_total.rename(columns={"total": "total_estudiantes"})

    df_pct = df_uni.merge(df_total, on="codigo", how="inner")
    df_pct["pct_no_estudiantes"] = (df_pct["no_estudiantes"] / df_pct["total_estudiantes"] * 100).round(1)

    ultimo_anio = renta_con_islas["anio"].max()
    df_renta = renta_con_islas[
        (renta_con_islas["anio"] == ultimo_anio) &
        (renta_con_islas["medida_code"] == "SUELDOS_SALARIOS")
    ][["codigo", "municipio", "isla", "valor"]].dropna()

    df_joined = df_renta.merge(df_pct, on="codigo", how="inner")

    plot = (
        ggplot(df_joined, aes(x="pct_no_estudiantes", y="valor", color="isla"))
        + geom_point(size=3, alpha=0.7)
        + geom_text(aes(label="municipio"), size=6, nudge_y=0.5)
        + theme_minimal()
        + labs(
            title=f"Estudios universitarios vs sueldos ({ultimo_anio})",
            subtitle="Por municipio · Fuente: ISTAC",
            x="% población que no cursa estudios",
            y="% renta procedente de sueldos y salarios",
            color="Isla"
        )
        + theme(figure_size=(12, 8))
    )

    os.makedirs("./output", exist_ok=True)
    plot.save("./output/scatter_estudios_empleo.png", dpi=150)
    return "./output/scatter_estudios_empleo.png"


@asset
def grafico_boxplot_empleo(renta_con_islas):
    df = renta_con_islas.copy()

    ultimo_anio = df["anio"].max()
    df_box = df[
        (df["anio"] == ultimo_anio) &
        (df["medida_code"] == "SUELDOS_SALARIOS")
    ].dropna(subset=["isla"])

    plot = (
        ggplot(df_box, aes(x="isla", y="valor", fill="isla"))
        + geom_boxplot()
        + theme_minimal()
        + scale_fill_brewer(type="qual", palette="Set2")
        + labs(
            title=f"Distribución de sueldos por isla ({ultimo_anio})",
            subtitle="Fuente: ISTAC",
            x="Isla",
            y="% renta procedente de sueldos y salarios",
            fill="Isla"
        )
        + theme(figure_size=(10, 6))
    )

    os.makedirs("./output", exist_ok=True)
    plot.save("./output/boxplot_empleo.png", dpi=150)
    return "./output/boxplot_empleo.png"
