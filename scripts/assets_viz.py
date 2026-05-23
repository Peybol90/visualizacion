"""
Capa 4 – Visualizaciones locales con plotnine
Gramática de gráficos explícita en cada asset.
Paleta: zona volcán=#E63946 (rojo), La Palma resto=#457B9D (azul), Resto=#A8DADC (gris-azul)
"""

import os
import pandas as pd
from dagster import asset
from plotnine import (
    ggplot, aes, geom_col, geom_line, geom_point, geom_boxplot,
    geom_hline, geom_text, facet_wrap,
    scale_fill_manual, scale_color_manual, scale_x_continuous,
    scale_y_continuous, scale_size_identity, coord_flip, theme_minimal, theme, labs,
    element_text, element_blank, element_rect,
    position_dodge,
)
from plotnine.scales import scale_fill_brewer

OUTPUT_DIR = "./output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

PALETA_ZONA = {
    "Zona volcán":      "#E63946",
    "La Palma (resto)": "#457B9D",
    "Resto provincia":  "#A8DADC",
}


# ─────────────────────────────────────────────────────────────────────────────
# ACTO 1 – Contexto
# ─────────────────────────────────────────────────────────────────────────────

@asset(group_name="viz", description="Acto 1 – Renta media por zona 2021-2023 (líneas)")
def viz_renta_por_zona(renta_por_zona: pd.DataFrame) -> str:
    p = (
        ggplot(renta_por_zona, aes(x="año", y="renta_media", color="zona", group="zona"))
        + geom_line(size=1.2)
        + geom_point(size=3)
        + scale_color_manual(values=PALETA_ZONA, name="Zona")
        + scale_x_continuous(breaks=[2021, 2022, 2023])
        + scale_y_continuous(labels=lambda lst: [f"{int(v):,}€" for v in lst])
        + labs(
            title="Renta bruta media por hogar · 2021-2023",
            subtitle="Zona volcán vs La Palma (resto) vs Provincia de Santa Cruz de Tenerife",
            x="Año", y="Renta bruta media (€)",
            caption="Fuente: ISTAC – E30325A_000009",
        )
        + theme_minimal()
        + theme(
            plot_title=element_text(size=13, face="bold"),
            plot_subtitle=element_text(size=10, color="#555555"),
            legend_position="bottom",
        )
    )
    path = f"{OUTPUT_DIR}/viz_01_renta_zona.png"
    p.save(path, dpi=150, width=9, height=5)
    return path


# ─────────────────────────────────────────────────────────────────────────────
# ACTO 2 – El golpe
# ─────────────────────────────────────────────────────────────────────────────

@asset(group_name="viz", description="Acto 2 – Actividad económica zona volcán 2021-2023 (barras agrupadas)")
def viz_actividad_zona_volcan(actividad_zona_volcan: pd.DataFrame) -> str:
    df = actividad_zona_volcan[actividad_zona_volcan["actividad"] != "No consta"].copy()
    df["año"] = df["año"].astype(str)
    p = (
        ggplot(df, aes(x="actividad", y="pct", fill="año"))
        + geom_col(position=position_dodge(width=0.8), width=0.7)
        + scale_fill_manual(
            values={"2021": "#A8DADC", "2022": "#457B9D", "2023": "#1D3557"},
            name="Año",
        )
        + coord_flip()
        + labs(
            title="Distribución de la actividad económica · Zona volcán",
            subtitle="El Paso · Tazacorte · Los Llanos de Aridane  (% sobre total trabajadores)",
            x="", y="% sobre total",
            caption="Fuente: INE – Censo de Población y Viviendas",
        )
        + theme_minimal()
        + theme(plot_title=element_text(size=13, face="bold"))
    )
    path = f"{OUTPUT_DIR}/viz_03_actividad_volcan.png"
    p.save(path, dpi=150, width=10, height=5)
    return path


@asset(group_name="viz", description="Acto 2 – Índice de construcción por municipio de La Palma (base 2021=100)")
def viz_construccion_indice(construccion_comparada: pd.DataFrame) -> str:
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors

    df = construccion_comparada.copy()
    muns_volcan = ["Paso, El", "Tazacorte", "Llanos de Aridane, Los"]

    # Identificar top 3 municipios por índice en 2023
    top3_2023 = (
        df[df["año"] == 2023]
        .nlargest(3, "indice")["municipio"]
        .tolist()
    )

    # Etiquetas solo en 2023 para top3 y municipios volcán
    etiquetar = set(top3_2023 + muns_volcan)
    df_labels = df[(df["año"] == 2023) & (df["municipio"].isin(etiquetar))].copy()

    # Paleta: volcán=rojos, top3=naranja, resto=azul claro uniforme
    muns_resto = sorted([m for m in df["municipio"].unique() if m not in muns_volcan])
    paleta = {m: "#B0C4DE" for m in muns_resto}  # azul claro uniforme para el fondo
    for m in top3_2023:
        if m not in muns_volcan:
            paleta[m] = "#F4A261"  # naranja para top3 no volcán
    paleta.update({
        "Paso, El":     "#E63946",
        "Tazacorte":    "#C1121F",
        "Llanos de Aridane, Los": "#FF6B6B",
    })

    # Grosor: destacar top3 y volcán
    df["grosor"] = df["municipio"].apply(
        lambda m: 1.8 if m in etiquetar else 0.6
    )

    p = (
        ggplot(df, aes(x="año", y="indice", color="municipio", group="municipio",
                       size="grosor"))
        + geom_hline(yintercept=100, linetype="dashed", color="#AAAAAA", size=0.8)
        + geom_line()
        + geom_point(data=df[df["municipio"].isin(etiquetar)], size=3)
        + geom_text(
            data=df_labels,
            mapping=aes(label="municipio"),
            nudge_x=0.05, nudge_y=1.5,
            size=7, ha="left",
        )
        + scale_color_manual(values=paleta)
        + scale_size_identity()
        + scale_x_continuous(breaks=[2021, 2022, 2023], limits=[2020.8, 2023.8])
        + labs(
            title="Sector construcción por municipio · La Palma · Índice 2021 = 100",
            subtitle="Top 3 en naranja · Zona volcán en rojo · Resto en azul",
            x="Año", y="Índice (2021 = 100)",
            caption="Fuente: INE – Censo de Población y Viviendas",
        )
        + theme_minimal()
        + theme(
            plot_title=element_text(size=13, face="bold"),
            plot_subtitle=element_text(size=10, color="#555555"),
            legend_position="none",
        )
    )
    path = f"{OUTPUT_DIR}/viz_04_construccion_indice.png"
    p.save(path, dpi=150, width=11, height=6)
    return path


# ─────────────────────────────────────────────────────────────────────────────
# ACTO 3 – La cicatriz
# ─────────────────────────────────────────────────────────────────────────────

@asset(group_name="viz", description="Acto 3 – % prestaciones desempleo por zona 2019-2023 (líneas)")
def viz_desempleo_evolucion(distribucion_por_zona: pd.DataFrame) -> str:
    df = distribucion_por_zona[
        distribucion_por_zona["MEDIDAS_CODE"] == "PRESTACIONES_DESEMPLEO"
    ].copy()
    df["zona"] = pd.Categorical(
        df["zona"],
        categories=["Zona volcán", "La Palma (resto)", "Resto provincia"],
        ordered=True,
    )
    p = (
        ggplot(df, aes(x="año", y="pct", color="zona", group="zona"))
        + geom_line(size=1.3)
        + geom_point(size=4)
        + geom_text(aes(label="pct"), nudge_y=0.15, size=8)
        + scale_color_manual(values=PALETA_ZONA, name="Zona")
        + scale_x_continuous(breaks=[2019, 2020, 2021, 2022, 2023])
        + labs(
            title="% de renta procedente de prestaciones por desempleo · 2019-2023",
            subtitle="Evolución pre y post volcán Tajogaite",
            x="Año", y="% sobre renta total",
            caption="Fuente: ISTAC – E30325A_000002  |  Volcán sept. 2021: impacto en prestaciones",
        )
        + theme_minimal()
        + theme(
            plot_title=element_text(size=13, face="bold"),
            plot_subtitle=element_text(size=10, color="#555555"),
            legend_position="bottom",
        )
    )
    path = f"{OUTPUT_DIR}/viz_05_desempleo_evolucion.png"
    p.save(path, dpi=150, width=10, height=5)
    return path


# ─────────────────────────────────────────────────────────────────────────────
# ACTO 4 – Recuperación
# ─────────────────────────────────────────────────────────────────────────────

@asset(group_name="viz", description="Acto 4 – Índice de recuperación de renta 2021-2023 por zona")
def viz_renta_recuperacion(renta_indice_recuperacion: pd.DataFrame) -> str:
    p = (
        ggplot(renta_indice_recuperacion,
               aes(x="año", y="indice", color="zona", group="zona"))
        + geom_hline(yintercept=100, linetype="dashed", color="#AAAAAA", size=0.8)
        + geom_line(size=1.3)
        + geom_point(size=4)
        + geom_text(aes(label="indice"), nudge_y=1.5, size=8)
        + scale_color_manual(values=PALETA_ZONA, name="Zona")
        + scale_x_continuous(breaks=[2021, 2022, 2023])
        + labs(
            title="Recuperación económica post-volcán · Índice renta 2021 = 100",
            subtitle="¿Ha recuperado la zona afectada su nivel de renta previo?",
            x="Año", y="Índice (2021 = 100)",
            caption="Fuente: ISTAC – E30325A_000009",
        )
        + theme_minimal()
        + theme(
            plot_title=element_text(size=13, face="bold"),
            plot_subtitle=element_text(size=10, color="#555555"),
            legend_position="bottom",
        )
    )
    path = f"{OUTPUT_DIR}/viz_07_renta_recuperacion.png"
    p.save(path, dpi=150, width=9, height=5)
    return path


@asset(group_name="viz", description="Acto 4 – Distribución fuentes de renta zona volcán 2021 vs 2023")
def viz_distribucion_volcan_cambio(distribucion_por_zona: pd.DataFrame) -> str:
    df = distribucion_por_zona[
        (distribucion_por_zona["zona"] == "Zona volcán")
        & (distribucion_por_zona["año"].isin([2019, 2023]))
    ].copy()
    df["año"] = df["año"].astype(str)
    p = (
        ggplot(df, aes(x="fuente", y="pct", fill="año"))
        + geom_col(position=position_dodge(width=0.75), width=0.65)
        + scale_fill_manual(
            values={"2019": "#A8DADC", "2023": "#E63946"},
            name="Año",
        )
        + coord_flip()
        + labs(
            title="Fuentes de renta en la zona volcán: 2019 vs 2023",
            subtitle="Rentas 2018 (pre-volcán) vs rentas 2022 (post-volcán)",
            x="", y="% sobre renta total",
            caption="Fuente: ISTAC – E30325A_000002  |  2019=pre-volcán, 2021=año volcán, 2023=post-volcán",
        )
        + theme_minimal()
        + theme(
            plot_title=element_text(size=13, face="bold"),
            legend_position="bottom",
        )
    )
    path = f"{OUTPUT_DIR}/viz_08_dist_volcan_cambio.png"
    p.save(path, dpi=150, width=10, height=5)
    return path


@asset(group_name="viz", description="Acto 4 – Tasa de paro por zona 2021-2024 (líneas, incluye 2024)")
def viz_tasa_paro(tasa_paro_zona: pd.DataFrame) -> str:
    p = (
        ggplot(tasa_paro_zona, aes(x="año", y="tasa_paro", color="zona", group="zona"))
        + geom_line(size=1.3)
        + geom_point(size=4)
        + geom_text(aes(label="tasa_paro"), nudge_y=0.8, size=8)
        + scale_color_manual(values=PALETA_ZONA, name="Zona")
        + scale_x_continuous(breaks=[2021, 2022, 2023, 2024])
        + labs(
            title="Tasa de paro por zona · 2021-2024",
            subtitle="Parados / (Ocupados + Parados)  |  INE – Censo Anual de Población tabla 66796",
            x="Año", y="Tasa de paro (%)",
            caption="2024: año más reciente disponible en INE",
        )
        + theme_minimal()
        + theme(
            plot_title=element_text(size=13, face="bold"),
            plot_subtitle=element_text(size=10, color="#555555"),
            legend_position="bottom",
        )
    )
    path = f"{OUTPUT_DIR}/viz_09_tasa_paro.png"
    p.save(path, dpi=150, width=9, height=5)
    return path


@asset(group_name="viz", description="Acto 4 – Ocupados nacidos en el extranjero vs España en zona volcán 2021-2024")
def viz_ocupados_origen(relacion_actividad_clean: pd.DataFrame) -> str:
    df = relacion_actividad_clean[
        (relacion_actividad_clean["zona"] == "Zona volcán")
        & (relacion_actividad_clean["relacion"] == "Ocupado/a")
        & (relacion_actividad_clean["pais_nacimiento"] != "Total")
    ].copy()
    agg = df.groupby(["año", "pais_nacimiento"])["num_casos"].sum().reset_index()
    total = agg.groupby("año")["num_casos"].transform("sum")
    agg["pct"] = (agg["num_casos"] / total * 100).round(1)
    agg["año"] = agg["año"].astype(str)
    p = (
        ggplot(agg, aes(x="año", y="pct", fill="pais_nacimiento"))
        + geom_col(position=position_dodge(width=0.7), width=0.6)
        + geom_text(aes(label="pct"), position=position_dodge(width=0.7), nudge_y=1, size=8)
        + scale_fill_manual(
            values={"España": "#457B9D", "Extranjero": "#E63946"},
            name="País de nacimiento",
        )
        + labs(
            title="Ocupados por país de nacimiento · Zona volcán · 2021-2024",
            subtitle="¿Trajo la reconstrucción post-volcán trabajadores extranjeros?",
            x="Año", y="% sobre ocupados totales",
            caption="Fuente: INE – Censo Anual de Población tabla 66796",
        )
        + theme_minimal()
        + theme(
            plot_title=element_text(size=13, face="bold"),
            plot_subtitle=element_text(size=10, color="#555555"),
            legend_position="bottom",
        )
    )
    path = f"{OUTPUT_DIR}/viz_11_ocupados_origen.png"
    p.save(path, dpi=150, width=9, height=5)
    return path