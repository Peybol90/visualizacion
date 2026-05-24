import pandas as pd, sys
sys.path.insert(0, ".")
from scripts.assets_raw import *
from scripts.assets_clean import *

rent_r = rentamedia_raw()
rent_c = rentamedia_clean(rent_r)

print("Nulos por columna:")
print(rent_c[["año", "geocode_join", "OBS_VALUE", "zona"]].isnull().sum())
print()
print("Filas con nulos:")
mask = rent_c[["año", "geocode_join", "OBS_VALUE", "zona"]].isnull().any(axis=1)
print(rent_c[mask][["año", "municipio", "MEDIDAS_CODE", "OBS_VALUE", "zona"]].head(10))