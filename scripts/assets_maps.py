"""
Capa 4c – Mapas coropléticos a nivel de sección
GeoJSONs: secciones_20YYMMDD.json en ./data/geojson/
Fuente: datos.canarias.es (ISTAC) – cubre toda Canarias incluyendo La Palma

Regla de join por dataset:
  ocupacion/actividad: geocode usa año propio  → GeoJSON mismo año
  rentamedia:          TERRITORIO_CODE usa año+1 → GeoJSON año+1

Columna geocode formato: 20230101_38027_D01_S001
Código municipio: posiciones [9:14]

Todos los mapas muestran La Palma completa con facet_wrap por año.
"""

import os
import pandas as pd
import geopandas as gpd
from dagster import asset
from plotnine import (
    ggplot, aes, geom_map, scale_fill_gradient,
    facet_wrap, theme_void, theme, labs,
    element_text, element_rect,
)

OUTPUT_DIR = "./output"
GEOJSON_DIR = "./data/geojson"
os.makedirs(OUTPUT_DIR, exist_ok=True)

LA_PALMA_CODES = {
    "38007", "38008", "38009", "38014", "38016", "38024", "38027",
    "38029", "38030", "38033", "38037", "38045", "38047", "38053",
}

AÑOS = [2021, 2022, 2023]

# Mapeo año dato → prefijo GeoJSON
# ocupacion/actividad: mismo año
# rentamedia: año+1
GEO_MISMO_AÑO = {a: f"{a}0101" for a in AÑOS}
GEO_AÑO_SIGUIENTE = {a: f"{a+1}0101" for a in AÑOS}


def _load_geojson(year_prefix: str) -> gpd.GeoDataFrame:
    paths = [
        f"{GEOJSON_DIR}/secciones_{year_prefix}.json",
        f"{GEOJSON_DIR}/secciones_{year_prefix}_tenerife.json",
    ]
    for path in paths:
        if os.path.isfile(path):
            gdf = gpd.read_file(path)
            if gdf.crs and gdf.crs.to_epsg() != 4326:
                gdf = gdf.to_crs(epsg=4326)
            return gdf
    raise FileNotFoundError(
        f"No se encontró GeoJSON para {year_prefix}. Buscado: {paths}"
    )


def _filtrar_lapalma(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    return gdf[gdf["gcd_isla"] == "ES707"].copy()


def _gdf_lapalma_anyo(year_prefix: str, anyo: int) -> gpd.GeoDataFrame:
    """Carga GeoJSON, filtra La Palma y añade columna año."""
    gdf = _load_geojson(year_prefix)
    gdf_lp = _filtrar_lapalma(gdf)
    gdf_lp["año"] = anyo
    return gdf_lp


# ─────────────────────────────────────────────────────────────────────────────
# Mapa 1 – Renta bruta media por sección · La Palma · 2021-2023
# ─────────────────────────────────────────────────────────────────────────────

@asset(group_name="maps",
       description="Mapa 1 – Renta bruta media por sección en La Palma · 2021-2023 (facet)")
def mapa_renta_lapalma_2022(rentamedia_clean: pd.DataFrame) -> str:
    df = rentamedia_clean[
        (rentamedia_clean["es_lapalma"])
        & (rentamedia_clean["MEDIDAS_CODE"] == "RENTA_BRUTA_MEDIA_HOGAR")
    ].copy()
    df["geocode_join"] = df["geocode_join"].astype(str)

    # Un GeoDataFrame por año con su GeoJSON correspondiente
    gdfs = []
    for anyo in AÑOS:
        geo_prefix = GEO_AÑO_SIGUIENTE[anyo]
        gdf_lp = _gdf_lapalma_anyo(geo_prefix, anyo)
        df_anyo = df[df["año"] == anyo][["geocode_join", "OBS_VALUE"]].copy()
        merged = gdf_lp.merge(df_anyo, left_on="geocode", right_on="geocode_join", how="left")
        gdfs.append(merged)

    combined = pd.concat(gdfs, ignore_index=True)
    combined = gpd.GeoDataFrame(combined, geometry="geometry")
    combined["año"] = combined["año"].astype(str)

    p = (
        ggplot(combined)
        + geom_map(aes(fill="OBS_VALUE"), color="white", size=0.1)
        + scale_fill_gradient(
            low="#FFF5EB", high="#7F2704",
            name="Renta bruta\nmedia (€)",
            na_value="#DDDDDD",
        )
        + facet_wrap("~año", ncol=3)
        + labs(
            title="Renta bruta media por hogar · Secciones de La Palma · 2021-2023",
            subtitle="Evolución antes, durante y después del volcán Tajogaite  |  ISTAC E30325A_000009",
            caption="Secciones grises: dato no disponible",
        )
        + theme_void()
        + theme(
            plot_title=element_text(size=12, face="bold"),
            plot_subtitle=element_text(size=9, color="#555555"),
            strip_background=element_rect(fill="#F0F0F0"),
            strip_text=element_text(size=10, face="bold"),
        )
    )
    path = f"{OUTPUT_DIR}/mapa_01_renta_lapalma.png"
    p.save(path, dpi=150, width=18, height=8)
    return path


# ─────────────────────────────────────────────────────────────────────────────
# Mapa 2 – % prestaciones desempleo · La Palma completa · 2021-2023
# ─────────────────────────────────────────────────────────────────────────────

@asset(group_name="maps",
       description="Mapa 2 – % prestaciones desempleo en La Palma completa · 2019-2023 (facet 5 paneles)")
def mapa_desempleo_volcan_2022(desempleo_seccion_lapalma: pd.DataFrame) -> str:
    df = desempleo_seccion_lapalma.copy()
    df["TERRITORIO_CODE"] = df["TERRITORIO_CODE"].astype(str)

    AÑOS_5 = [2019, 2020, 2021, 2022, 2023]
    gdfs = []
    for anyo in AÑOS_5:
        geo_prefix = f"{anyo + 1}0101"
        gdf_lp = _gdf_lapalma_anyo(geo_prefix, anyo)
        df_anyo = df[df["año"] == anyo][["TERRITORIO_CODE", "pct"]].copy()
        merged = gdf_lp.merge(df_anyo, left_on="geocode", right_on="TERRITORIO_CODE", how="left")
        gdfs.append(merged)

    import geopandas as gpd
    combined = pd.concat(gdfs, ignore_index=True)
    combined = gpd.GeoDataFrame(combined, geometry="geometry")
    combined["año"] = combined["año"].astype(str)

    p = (
        ggplot(combined)
        + geom_map(aes(fill="pct"), color="white", size=0.1)
        + scale_fill_gradient(
            low="#FFFFFF", high="#E63946",
            name="% prest.\ndesempleo",
            na_value="#DDDDDD",
        )
        + facet_wrap("~año", ncol=5)
        + labs(
            title="Prestaciones de desempleo · La Palma · 2019-2023",
            subtitle="Evolución pre y post volcán Tajogaite (sept. 2021)  |  ISTAC E30325A_000002",
            caption="Secciones grises: dato no disponible",
        )
        + theme_void()
        + theme(
            plot_title=element_text(size=12, face="bold"),
            plot_subtitle=element_text(size=9, color="#555555"),
            strip_background=element_rect(fill="#F0F0F0"),
            strip_text=element_text(size=10, face="bold"),
        )
    )
    path = f"{OUTPUT_DIR}/mapa_02_desempleo_lapalma.png"
    p.save(path, dpi=150, width=24, height=8, limitsize=False)
    return path


# ─────────────────────────────────────────────────────────────────────────────
# Mapa 3 – Construcción por sección · La Palma · 2021-2023
# ─────────────────────────────────────────────────────────────────────────────

@asset(group_name="maps",
       description="Mapa 3 – Trabajadores en construcción por sección · La Palma · 2021-2023 (facet)")
def mapa_construccion_lapalma_2023(actividad_clean: pd.DataFrame) -> str:
    df = actividad_clean[
        (actividad_clean["es_lapalma"])
        & (actividad_clean["actividad"] == "Construcción")
    ].copy()
    agg = df.groupby(["año", "geocode"])["num_casos"].sum().reset_index()
    agg["geocode"] = agg["geocode"].astype(str)

    gdfs = []
    for anyo in AÑOS:
        geo_prefix = GEO_MISMO_AÑO[anyo]
        gdf_lp = _gdf_lapalma_anyo(geo_prefix, anyo)
        agg_anyo = agg[agg["año"] == anyo][["geocode", "num_casos"]].copy()
        merged = gdf_lp.merge(agg_anyo, left_on="geocode", right_on="geocode", how="left")
        gdfs.append(merged)

    combined = pd.concat(gdfs, ignore_index=True)
    combined = gpd.GeoDataFrame(combined, geometry="geometry")
    combined["año"] = combined["año"].astype(str)

    p = (
        ggplot(combined)
        + geom_map(aes(fill="num_casos"), color="white", size=0.1)
        + scale_fill_gradient(
            low="#F0F4FF", high="#1D3557",
            name="Trabajadores\nconstrucción",
            na_value="#DDDDDD",
        )
        + facet_wrap("~año", ncol=3)
        + labs(
            title="Sector construcción por sección · La Palma · 2021-2023",
            subtitle="Reconstrucción post-volcán Tajogaite  |  INE Censo Anual de Población",
            caption="Secciones grises: sin dato disponible",
        )
        + theme_void()
        + theme(
            plot_title=element_text(size=12, face="bold"),
            plot_subtitle=element_text(size=9, color="#555555"),
            strip_background=element_rect(fill="#F0F0F0"),
            strip_text=element_text(size=10, face="bold"),
        )
    )
    path = f"{OUTPUT_DIR}/mapa_03_construccion_lapalma.png"
    p.save(path, dpi=150, width=18, height=8)
    return path


# ─────────────────────────────────────────────────────────────────────────────
# Mapa 5 – % prestaciones desempleo por sección · Tenerife · 2019-2023
# Mismos datos que mapa_02 pero para la isla de Tenerife
# ─────────────────────────────────────────────────────────────────────────────

# Municipios de Tenerife (códigos INE 38001-38059, excluyendo La Palma, La Gomera, El Hierro)
TENERIFE_CODES = {
    "38001", "38002", "38003", "38004", "38005", "38006", "38010", "38011",
    "38012", "38013", "38015", "38017", "38018", "38019", "38020", "38021",
    "38022", "38023", "38025", "38026", "38028", "38031", "38032", "38034",
    "38035", "38036", "38038", "38039", "38040", "38041", "38042", "38043",
    "38044", "38046", "38048", "38049", "38050", "38051", "38052", "38054",
    "38055", "38056", "38057", "38058", "38059",
}


def _filtrar_tenerife(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    return gdf[gdf["gcd_isla"] == "ES709"].copy()


@asset(group_name="maps",
       description="Mapa 5 – % prestaciones desempleo por sección · Tenerife · 2019-2023 (facet 5 paneles)")
def mapa_desempleo_tenerife(distribucion_clean: pd.DataFrame) -> str:
    df = distribucion_clean[
        distribucion_clean["MEDIDAS_CODE"] == "PRESTACIONES_DESEMPLEO"
    ].copy()
    df["TERRITORIO_CODE"] = df["TERRITORIO_CODE"].astype(str)

    AÑOS_5 = [2019, 2020, 2021, 2022, 2023]
    gdfs = []
    for anyo in AÑOS_5:
        geo_prefix = f"{anyo + 1}0101"
        gdf = _load_geojson(geo_prefix)
        gdf_tf = _filtrar_tenerife(gdf)
        gdf_tf = gdf_tf.copy()
        gdf_tf["año"] = anyo
        df_anyo = df[df["año"] == anyo][["TERRITORIO_CODE", "pct"]].copy()
        merged = gdf_tf.merge(df_anyo, left_on="geocode", right_on="TERRITORIO_CODE", how="left")
        gdfs.append(merged)

    combined = pd.concat(gdfs, ignore_index=True)
    combined = gpd.GeoDataFrame(combined, geometry="geometry")
    combined["año"] = combined["año"].astype(str)

    from plotnine import ggplot, aes, geom_map, scale_fill_gradient, facet_wrap, theme_void, theme, labs, element_text, element_rect
    p = (
        ggplot(combined)
        + geom_map(aes(fill="pct"), color="white", size=0.05)
        + scale_fill_gradient(
            low="#FFFFFF", high="#E63946",
            name="% prest.\ndesempleo",
            na_value="#DDDDDD",
        )
        + facet_wrap("~año", ncol=5)
        + labs(
            title="Prestaciones de desempleo · Tenerife · 2019-2023",
            subtitle="Contexto provincial  |  ISTAC E30325A_000002",
            caption="Secciones grises: dato no disponible",
        )
        + theme_void()
        + theme(
            plot_title=element_text(size=12, face="bold"),
            plot_subtitle=element_text(size=9, color="#555555"),
            strip_background=element_rect(fill="#F0F0F0"),
            strip_text=element_text(size=10, face="bold"),
        )
    )
    path = f"{OUTPUT_DIR}/mapa_05_desempleo_tenerife.png"
    p.save(path, dpi=150, width=24, height=7, limitsize=False)
    return path