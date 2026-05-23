import pandas as pd, sys, os
sys.path.insert(0, ".")
from scripts.assets_raw import *
from scripts.assets_clean import *
from scripts.assets_analysis import *
from scripts.assets_viz import *

act_r = actividad_raw()
act_c = actividad_clean(act_r)
const_c = construccion_comparada(act_c)

# Ver top 3 en 2023
print("Top 3 en 2023:")
print(const_c[const_c["año"]==2023].nlargest(3, "indice")[["municipio","indice"]])

path = viz_construccion_indice(const_c)
print(f"OK: {path} ({os.path.getsize(path):,} bytes)")