"""
Sensor y Job – Automatización
El sensor vigila ./data/raw/ y dispara el pipeline completo
cuando detecta cambios en cualquier fichero de datos.
Intervalo mínimo: 3600 s (1 hora) para entorno de producción.
"""

import os
from dagster import (
    sensor, RunRequest, define_asset_job, AssetSelection,
)

pipeline_completo_job = define_asset_job(
    name="pipeline_completo_job",
    selection=AssetSelection.all(),
)


@sensor(job=pipeline_completo_job, minimum_interval_seconds=3600)
def sensor_cambios_datos(context):
    data_dir = "./data/raw"
    if not os.path.isdir(data_dir):
        return

    ficheros = [
        os.path.join(data_dir, f)
        for f in os.listdir(data_dir)
        if os.path.isfile(os.path.join(data_dir, f))
    ]
    if not ficheros:
        return

    mtime_actual = str(max(os.path.getmtime(f) for f in ficheros))

    if mtime_actual != (context.cursor or "0"):
        context.update_cursor(mtime_actual)
        yield RunRequest(run_key=mtime_actual)