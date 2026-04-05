import os
import re
import json
import subprocess
import requests
import pandas as pd
from plotnine import (
    ggplot, aes, geom_col, geom_point, geom_text, geom_map,
    coord_flip, geom_boxplot,
    theme_minimal, theme_void, labs, theme, scale_fill_brewer,
    scale_fill_gradient, element_text
)
import geopandas as gpd
from dagster import asset, MetadataValue, Output

# ─────────────────────────────────────────────────────────────────────────────
# ASSETS ORIGINALES (práctica anterior)
# ─────────────────────────────────────────────────────────────────────────────

@asset
def renta_raw():
    df = pd.read_csv("./data/distribucion-renta-canarias.csv", sep=",", encoding="utf-8")
    return df

@asset
def codislas_raw():
    df = pd.read_csv("./data/codislas.csv", sep=";", encoding="latin-1")
    return df

@asset
def renta_limpia(renta_raw):
    df = renta_raw.copy()
    df = df.rename(columns={
        "TERRITORIO#es":   "territorio",
        "TERRITORIO_CODE": "codigo",
        "TIME_PERIOD#es":  "anio",
        "TIME_PERIOD_CODE":"anio_code",
        "MEDIDAS#es":      "medida",
        "MEDIDAS_CODE":    "medida_code",
        "OBS_VALUE":       "valor"
    })
    df = df[df["codigo"].str.match(r"^\d{5}$", na=False)]
    df = df.dropna(subset=["valor"])
    return df

@asset
def codislas_limpio(codislas_raw):
    df = codislas_raw.copy()
    df["codigo"] = df["CPRO"].astype(str).str.zfill(2) + df["CMUN"].astype(str).str.zfill(3)
    df = df.rename(columns={"ISLA": "isla", "NOMBRE": "municipio"})
    df["isla"] = df["isla"].str.strip().replace({
        "Gomera, La": "La Gomera",
        "Palma, La":  "La Palma",
        "Hierro, El": "El Hierro"
    })
    return df[["codigo", "isla", "municipio"]]

@asset
def renta_con_islas(renta_limpia, codislas_limpio):
    df = renta_limpia.merge(codislas_limpio, on="codigo", how="left")
    return df

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


# ─────────────────────────────────────────────────────────────────────────────
# ASSETS NUEVOS — PRÁCTICA 4
# ─────────────────────────────────────────────────────────────────────────────

# ── ASSET 7: Mapa de rentas por municipio con GeoJSON ─────────────────────────
@asset
def grafico_mapa_municipios(renta_con_islas):
    gdf = gpd.read_file("./data/Municipios-2024.json")

    # La columna de código municipal en este GeoJSON es 'geocode'
    gdf["codigo"] = gdf["geocode"].astype(str).str.zfill(5)

    # Filtrar renta al último año y medida de sueldos
    ultimo_anio = renta_con_islas["anio"].max()
    df_renta = renta_con_islas[
        (renta_con_islas["anio"] == ultimo_anio) &
        (renta_con_islas["medida_code"] == "SUELDOS_SALARIOS")
    ][["codigo", "valor", "municipio"]].dropna()

    # Join GeoJSON + renta
    gdf_merged = gdf.merge(df_renta, on="codigo", how="left")
    gdf_wgs84 = gdf_merged.to_crs(epsg=4326)

    plot = (
        ggplot(gdf_wgs84)
        + geom_map(aes(fill="valor"), color="white", size=0.2)
        + scale_fill_gradient(low="#FFF5EB", high="#7F2704", na_value="#CCCCCC")
        + theme_void()
        + labs(
            title=f"% Renta procedente de sueldos y salarios por municipio ({ultimo_anio})",
            subtitle="Canarias · Fuente: ISTAC",
            fill="% Sueldos"
        )
        + theme(
            figure_size=(14, 8),
            plot_title=element_text(size=13, face="bold"),
            plot_subtitle=element_text(size=10),
        )
    )

    os.makedirs("./output", exist_ok=True)
    ruta = "./output/mapa_municipios_renta.png"
    plot.save(ruta, dpi=150)
    return ruta


# ── ASSET 8: Plantilla para el LLM ───────────────────────────────────────────
@asset
def template_ia(renta_con_islas):
    """
    Construye la petición al LLM siguiendo la gramática de gráficos de Wickham.
    Describe el gráfico en lenguaje natural parametrizado con variables reales
    del dataset para que el LLM genere código plotnine ejecutable.
    """
    columnas = ", ".join(renta_con_islas.columns)
    islas = renta_con_islas["isla"].dropna().unique().tolist()
    ultimo_anio = renta_con_islas["anio"].max()

    # Template que el LLM debe completar (estructura fija)
    template_tecnico = """
def generar_plot(df):
    # El código debe seguir esta estructura:
    # plot = (ggplot(df, aes(...)) + geom_...)
    # return plot
"""

    system_content = (
        "Eres un experto en la gramática de gráficos y Plotnine. "
        "Tu tarea es traducir descripciones en lenguaje natural a código Python ejecutable. "
        f"Usa siempre este template exacto: {template_tecnico}. "
        "La función debe llamarse exactamente 'generar_plot' y aceptar un DataFrame 'df'. "
        "Importa ÚNICAMENTE desde plotnine (ya está disponible en el entorno). "
        "Devuelve exclusivamente el código Python, sin explicaciones, sin bloques markdown."
    )

    descripcion_grafico = f"""
- Dataset: df (columnas disponibles: {columnas})
- Año a visualizar: filtra con df['anio'] == df['anio'].max() (el campo 'anio' es de tipo int64, NO uses strings ni comillas)
- Medida a usar: SUELDOS_SALARIOS (filtra df donde df['medida_code'] == 'SUELDOS_SALARIOS')
- Estéticas:
  * Variable 'isla' mapeada al eje X.
  * Variable 'valor' mapeada al eje Y (es un porcentaje entre 0 y 100).
  * Variable 'isla' mapeada al relleno (fill) para distinguir islas por color.
- Geometría: Boxplot (geom_boxplot) para mostrar la distribución de valores por isla.
- Etiquetas:
  * Título: 'Distribución de sueldos y salarios por isla ({ultimo_anio})'.
  * Eje X: 'Isla'.
  * Eje Y: '% renta procedente de sueldos y salarios'.
  * Leyenda fill: 'Isla'.
- Escala de color: scale_fill_brewer con type='qual' y palette='Set2'.
- Principio Gestalt (Punto Focal):
  * Resaltar 'Tenerife' con un color distintivo (p.ej. '#E63946').
  * Resto de islas en un color neutro ('#A8DADC').
  * Usar scale_fill_manual para definir estos colores explícitamente.
- Tema: theme_minimal(), figure_size=(10, 6).
"""

    user_content = f"Basándote en esta descripción, completa el template:\n{descripcion_grafico}"

    return {
        "model": "ollama/llama3.1:8b",
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user",   "content": user_content}
        ],
        "temperature": 0.1,
        "stream": False
    }


# ── ASSET 9: Generación de código via LLM ─────────────────────────────────────
LLM_URL = "http://gpu1.esit.ull.es:4000/v1/chat/completions"
LLM_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": "Bearer sk-1234"
}

@asset
def codigo_generado_ia(context, template_ia):
    """
    Envía la petición al LLM y extrae el código Python de la respuesta.
    Limpia posibles bloques markdown (```python ... ```) que el modelo
    pueda añadir aunque se le indique que no lo haga.
    """
    response = requests.post(LLM_URL, headers=LLM_HEADERS, json=template_ia, timeout=60)
    response.raise_for_status()

    data = response.json()
    codigo_raw = data["choices"][0]["message"]["content"]

    context.log.info(f"Respuesta raw del LLM:\n{codigo_raw}")

    # Limpieza: eliminar bloques markdown si el modelo los añade
    codigo_limpio = re.sub(r"```(?:python)?\s*", "", codigo_raw)
    codigo_limpio = re.sub(r"```", "", codigo_limpio)
    codigo_limpio = codigo_limpio.strip()

    # Correcciones de errores comunes del LLM con plotnine
    codigo_limpio = re.sub(r'\bscale_fill_manual\s*\(\s*value\s*=', 'scale_fill_manual(values=', codigo_limpio)
    codigo_limpio = re.sub(r'\bscale_color_manual\s*\(\s*value\s*=', 'scale_color_manual(values=', codigo_limpio)
    codigo_limpio = re.sub(r'\bscale_colour_manual\s*\(\s*value\s*=', 'scale_colour_manual(values=', codigo_limpio)

    # Validación mínima: debe contener la definición de función
    if "def generar_plot" not in codigo_limpio:
        raise ValueError(
            "El LLM no generó una función 'generar_plot'. "
            f"Respuesta recibida:\n{codigo_limpio}"
        )

    # Validación: solo debe contener código Python (no texto libre)
    lineas = codigo_limpio.splitlines()
    lineas_validas = [l for l in lineas if l.strip() == "" or l.strip().startswith("#")
                      or re.match(r"^[\w\s=\(\)\[\]\{\},:\.+\-\*\/\"'#@]", l)]
    if len(lineas_validas) < len(lineas) * 0.8:
        raise ValueError("La respuesta del LLM contiene demasiado texto no-Python.")

    context.log.info(f"Código generado y validado:\n{codigo_limpio}")

    return Output(
        value=codigo_limpio,
        metadata={
            "longitud_codigo": MetadataValue.int(len(codigo_limpio)),
            "contiene_generar_plot": MetadataValue.bool(True),
            "modelo": MetadataValue.text(template_ia["model"]),
        }
    )


# ── ASSET 10: Ejecución del código generado por IA ────────────────────────────
@asset
def visualizacion_ia(context, codigo_generado_ia, renta_con_islas):
    import plotnine
    import re as _re

    df = renta_con_islas.copy()

    codigo_final = codigo_generado_ia

    # Correcciones de errores comunes del LLM con plotnine
    codigo_final = _re.sub(
        r'\+\s*scale_fill_manual\([^)]*\)',
        "+ scale_fill_manual(values={'Tenerife': '#E63946', 'Gran Canaria': '#A8DADC', 'Lanzarote': '#A8DADC', 'Fuerteventura': '#A8DADC', 'La Palma': '#A8DADC', 'La Gomera': '#A8DADC', 'El Hierro': '#A8DADC', 'La Graciosa': '#A8DADC'})",
        codigo_final
    )
    codigo_final = _re.sub(
        r'theme_minimal\s*\(\s*figure_size\s*=\s*\([^)]*\)\s*\)',
        'theme_minimal() + theme(figure_size=(10, 6))',
        codigo_final
    )
    codigo_final = _re.sub(
        r'theme_bw\s*\(\s*figure_size\s*=\s*\([^)]*\)\s*\)',
        'theme_bw() + theme(figure_size=(10, 6))',
        codigo_final
    )
    # figure_size fuera de theme() — moverlo dentro
    codigo_final = _re.sub(
        r'\+\s*theme\s*\(([^)]*?)figure_size\s*=\s*(\([^)]*\))([^)]*?)\)',
        lambda m: f'+ theme({m.group(1)}figure_size={m.group(2)}{m.group(3)})',
        codigo_final
    )
    # Corregir filtro de año con string en lugar de int
    codigo_final = _re.sub(
        r"df\[.anio.\]\s*==\s*['\"](\d{4})['\"]",
        r"df['anio'] == df['anio'].max()",
        codigo_final
    )

    context.log.info(f"Código a ejecutar:\n{codigo_final}")

    entorno_ejecucion = {}
    entorno_ejecucion["plotnine"] = plotnine
    entorno_ejecucion.update({
        k: v for k, v in plotnine.__dict__.items() if not k.startswith("_")
    })
    entorno_ejecucion["pd"] = pd

    try:
        exec(codigo_final, entorno_ejecucion)

        if "generar_plot" not in entorno_ejecucion:
            raise ValueError("El código ejecutado no definió la función 'generar_plot'.")

        grafico = entorno_ejecucion["generar_plot"](df)

        os.makedirs("./output", exist_ok=True)
        ruta_archivo = "./output/visualizacion_ia.png"
        grafico.save(ruta_archivo, width=10, height=6, dpi=150)

        context.log.info(f"Gráfico guardado en {ruta_archivo}")

        return Output(
            value=ruta_archivo,
            metadata={
                "ruta": MetadataValue.path(ruta_archivo),
                "mensaje": MetadataValue.text("Gráfico generado correctamente por el LLM"),
            }
        )

    except Exception as e:
        context.log.error(f"Error al ejecutar el código de la IA: {e}")
        raise


# ── ASSET 11: Despliegue en GitHub Pages ──────────────────────────────────────
@asset
def despliegue_github(context, visualizacion_ia):
    ruta_imagen = visualizacion_ia

    if not os.path.isfile(ruta_imagen):
        raise FileNotFoundError(f"No se encontró el archivo a subir: {ruta_imagen}")

    # git add
    r = subprocess.run(["git", "add", ruta_imagen], capture_output=True, text=True)
    context.log.info(f"$ git add {ruta_imagen}\n{r.stdout}{r.stderr}")
    if r.returncode != 0:
        raise RuntimeError(f"Error en git add:\n{r.stderr}")

    # git commit — returncode 1 con "nothing to commit" es OK
    r = subprocess.run(
        ["git", "commit", "-m", "Actualización automática del gráfico generado por IA"],
        capture_output=True, text=True
    )
    context.log.info(f"$ git commit\n{r.stdout}{r.stderr}")
    sin_cambios = "nothing to commit" in r.stdout + r.stderr or "no changes added" in r.stdout + r.stderr
    if r.returncode != 0 and not sin_cambios:
        raise RuntimeError(f"Error en git commit:\n{r.stderr}")

    # git push
    r = subprocess.run(
        ["git", "push", "--set-upstream", "origin", "practica-calidad-checks"],
        capture_output=True, text=True
    )
    context.log.info(f"$ git push\n{r.stdout}{r.stderr}")
    if r.returncode != 0:
        raise RuntimeError(f"Error en git push:\n{r.stderr}")

    return Output(
        value=ruta_imagen,
        metadata={
            "archivo_subido": MetadataValue.path(ruta_imagen),
            "mensaje": MetadataValue.text("Despliegue en GitHub completado"),
        }
    )
