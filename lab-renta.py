import pandas as pd
from plotnine import (
    ggplot, aes, geom_col, coord_flip,
    theme_minimal, labs, theme, scale_fill_brewer
)

# Carga de datos
renta = pd.read_csv("./data/distribucion-renta-canarias.csv", sep=",", encoding="utf-8")
codislas = pd.read_csv("./data/codislas.csv", sep=";", encoding="latin-1")

# Limpieza
renta = renta.rename(columns={
    "TERRITORIO#es":   "territorio",
    "TERRITORIO_CODE": "codigo",
    "TIME_PERIOD#es":  "anio",
    "TIME_PERIOD_CODE":"anio_code",
    "MEDIDAS#es":      "medida",
    "MEDIDAS_CODE":    "medida_code",
    "OBS_VALUE":       "valor"
})
renta = renta[renta["codigo"].str.match(r"^\d{5}$", na=False)]
renta = renta.dropna(subset=["valor"])

# Join con codislas
codislas["codigo"] = codislas["CPRO"].astype(str).str.zfill(2) + codislas["CMUN"].astype(str).str.zfill(3)
codislas = codislas.rename(columns={"ISLA": "isla", "NOMBRE": "municipio"})
df = renta.merge(codislas[["codigo", "isla", "municipio"]], on="codigo", how="left")

# Filtro
ultimo_anio = df["anio"].max()
df_filtrado = df[
    (df["anio"] == ultimo_anio) &
    (df["medida_code"] == "SUELDOS_SALARIOS")
].dropna(subset=["municipio"])

df_top = df_filtrado.nlargest(20, "valor")

# Gráfico
from plotnine import geom_boxplot

df_box = df[
    (df["anio"] == ultimo_anio) &
    (df["medida_code"] == "SUELDOS_SALARIOS")
].dropna(subset=["isla"])

plot = (
    ggplot(df_box, aes(x="isla", y="valor", fill="isla"))
    + geom_boxplot()
    + theme_minimal()
    + labs(title="Distribución de sueldos por isla", x="Isla", y="Valor (índice)")
)

plot.show()