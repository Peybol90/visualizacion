import pandas as pd
import sys
sys.path.insert(0, ".")

from scripts.assets_raw import *
from scripts.assets_clean import *
from scripts.assets_analysis import *
from scripts.assets_maps import *

ocu_r  = ocupacion_raw()
act_r  = actividad_raw()
rent_r = rentamedia_raw()
dist_r = distribucion_raw()

ocu_c  = ocupacion_clean(ocu_r)
act_c  = actividad_clean(act_r)
rent_c = rentamedia_clean(rent_r)
dist_c = distribucion_clean(dist_r)

desemp_lp = desempleo_seccion_lapalma(dist_c)
print("desempleo_lapalma:", desemp_lp.shape)
print("municipios:", sorted(desemp_lp["municipio"].unique()))

import os
for nombre, fn in [
    ("mapa_01", lambda: mapa_renta_lapalma_2022(rent_c)),
    ("mapa_02", lambda: mapa_desempleo_volcan_2022(desemp_lp)),
    ("mapa_03", lambda: mapa_construccion_lapalma_2023(act_c)),
]:
    try:
        path = fn()
        print(f"OK {nombre}: {os.path.getsize(path):,} bytes")
    except Exception as e:
        print(f"ERR {nombre}: {e}")