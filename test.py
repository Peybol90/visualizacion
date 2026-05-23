import pandas as pd, sys
sys.path.insert(0, ".")
from scripts.assets_raw import *
from scripts.assets_clean import *
from scripts.assets_analysis import *

act_r = actividad_raw()
act_c = actividad_clean(act_r)
const_c = construccion_comparada(act_c)
print(const_c.columns.tolist())
print(const_c.head(3))