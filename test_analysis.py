import pandas as pd
import sys
sys.path.insert(0, ".")

from scripts.assets_raw import *
from scripts.assets_clean import *
from scripts.assets_analysis import *

# Capa raw
ocu_r = ocupacion_raw()
act_r = actividad_raw()
rent_r = rentamedia_raw()
dist_r = distribucion_raw()

# Capa clean
ocu_c = ocupacion_clean(ocu_r)
act_c = actividad_clean(act_r)
rent_c = rentamedia_clean(rent_r)
dist_c = distribucion_clean(dist_r)

# Capa analysis
renta_z    = renta_por_zona(rent_c)
dist_z     = distribucion_por_zona(dist_c)
act_vz     = actividad_zona_volcan(act_c)
const_c    = construccion_comparada(act_c)
desemp     = desempleo_seccion_volcan(dist_c)
ocup_cal   = ocupacion_calidad_volcan(ocu_c)
renta_idx  = renta_indice_recuperacion(renta_z)
top_sec    = top_secciones_renta_lapalma(rent_c)

for nombre, df in [
    ("renta_por_zona",            renta_z),
    ("distribucion_por_zona",     dist_z),
    ("actividad_zona_volcan",     act_vz),
    ("construccion_comparada",    const_c),
    ("desempleo_seccion_volcan",  desemp),
    ("ocupacion_calidad_volcan",  ocup_cal),
    ("renta_indice_recuperacion", renta_idx),
    ("top_secciones_renta_lapalma", top_sec),
]:
    print(f"{nombre}: {df.shape} | NaNs: {df.isnull().sum().sum()}")
    print(f"  {df.head(2).to_string(index=False)}")
    print()