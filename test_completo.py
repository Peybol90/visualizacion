import pandas as pd, sys, os
sys.path.insert(0, ".")
from scripts.assets_raw import *
from scripts.assets_clean import *
from scripts.assets_analysis import *
from scripts.assets_viz import *
from scripts.assets_maps import *

# Raw
ocu_r  = ocupacion_raw()
act_r  = actividad_raw()
rent_r = rentamedia_raw()
dist_r = distribucion_raw()
rel_r  = relacion_actividad_raw()

# Clean
ocu_c  = ocupacion_clean(ocu_r)
act_c  = actividad_clean(act_r)
rent_c = rentamedia_clean(rent_r)
dist_c = distribucion_clean(dist_r)
rel_c  = relacion_actividad_clean(rel_r)

# Analysis
renta_z   = renta_por_zona(rent_c)
dist_z    = distribucion_por_zona(dist_c)
act_vz    = actividad_zona_volcan(act_c)
const_c   = construccion_comparada(act_c)
desemp_lp = desempleo_seccion_lapalma(dist_c)
ocup_cal  = ocupacion_calidad_volcan(ocu_c)
renta_idx = renta_indice_recuperacion(renta_z)
tasa      = tasa_paro_zona(rel_c)

# Viz
vizs = [
    ("viz_01", lambda: viz_renta_por_zona(renta_z)),
    ("viz_02", lambda: viz_distribucion_renta_2021(dist_z)),
    ("viz_03", lambda: viz_actividad_zona_volcan(act_vz)),
    ("viz_04", lambda: viz_construccion_indice(const_c)),
    ("viz_05", lambda: viz_desempleo_evolucion(dist_z)),
    ("viz_06", lambda: viz_ocupacion_calidad(ocup_cal)),
    ("viz_07", lambda: viz_renta_recuperacion(renta_idx)),
    ("viz_08", lambda: viz_distribucion_volcan_cambio(dist_z)),
    ("viz_09", lambda: viz_tasa_paro(tasa)),
    ("viz_10", lambda: viz_composicion_actividad(rel_c)),
    ("viz_11", lambda: viz_ocupados_origen(rel_c)),
]

mapas = [
    ("mapa_01", lambda: mapa_renta_lapalma_2022(rent_c)),
    ("mapa_02", lambda: mapa_desempleo_volcan_2022(desemp_lp)),
    ("mapa_03", lambda: mapa_construccion_lapalma_2023(act_c)),
    ("mapa_04", lambda: mapa_paro_lapalma_2021_2024(rel_c)),
    ("mapa_05", lambda: mapa_desempleo_tenerife(dist_c)),
]

print("=== VISUALIZACIONES ===")
for nombre, fn in vizs:
    try:
        path = fn()
        print(f"OK  {nombre}: {os.path.getsize(path):,} bytes")
    except Exception as e:
        print(f"ERR {nombre}: {e}")

print("\n=== MAPAS ===")
for nombre, fn in mapas:
    try:
        path = fn()
        print(f"OK  {nombre}: {os.path.getsize(path):,} bytes")
    except Exception as e:
        print(f"ERR {nombre}: {e}")