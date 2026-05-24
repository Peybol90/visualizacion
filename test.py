import pandas as pd, sys
sys.path.insert(0, ".")
from scripts.assets_raw import *
from scripts.assets_clean import *

ocu_r = ocupacion_raw()
ocu_c = ocupacion_clean(ocu_r)
print(ocu_c["ocupacion"].unique())
print()
# Ver cuántas filas quedan con el filtro actual
mask = ocu_c["ocupacion"].str.contains("construcc", case=False, na=False)
print("Filas con 'construcc':", mask.sum())