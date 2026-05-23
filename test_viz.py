import pandas as pd
import sys
sys.path.insert(0, ".")

from scripts.assets_raw import *
from scripts.assets_clean import *
from scripts.assets_analysis import *
from scripts.assets_viz import *

# Raw
ocu_r  = ocupacion_raw()
act_r  = actividad_raw()
rent_r = rentamedia_raw()
dist_r = distribucion_raw()

# Clean
ocu_c  = ocupacion_clean(ocu_r)
act_c  = actividad_clean(act_r)
rent_c = rentamedia_clean(rent_r)
dist_c = distribucion_clean(dist_r)

# Analysis
renta_z   = renta_por_zona(rent_c)
dist_z    = distribucion_por_zona(dist_c)
act_vz    = actividad_zona_volcan(act_c)
const_c   = construccion_comparada(act_c)
desemp    = desempleo_seccion_volcan(dist_c)
renta_idx = renta_indice_recuperacion(renta_z)

# Viz - una a una para localizar errores
vizs = [
    ("viz_01", lambda: viz_renta_por_zona(renta_z)),
    ("viz_03", lambda: viz_actividad_zona_volcan(act_vz)),
    ("viz_04", lambda: viz_construccion_indice(const_c)),
    ("viz_05", lambda: viz_desempleo_evolucion(dist_z)),
    ("viz_07", lambda: viz_renta_recuperacion(renta_idx)),
    ("viz_08", lambda: viz_distribucion_volcan_cambio(dist_z)),
]

for nombre, fn in vizs:
    try:
        path = fn()
        import os
        tam = os.path.getsize(path)
        print(f"OK  {nombre}: {path} ({tam:,} bytes)")
    except Exception as e:
        print(f"ERR {nombre}: {e}")