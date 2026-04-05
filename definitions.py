import os
from dagster import (
    Definitions,
    load_assets_from_modules,
    load_asset_checks_from_modules,
    define_asset_job,
    AssetSelection,
    sensor,
    RunRequest,
    ScheduleDefinition,
)
from scripts import pipeline_renta, checks_renta

# ── Job: materializa todos los assets del pipeline ────────────────────────────
pipeline_completo_job = define_asset_job(
    name="pipeline_completo_job",
    selection=AssetSelection.all(),
)

# ── Sensor: vigila cambios en la carpeta data/ ────────────────────────────────
@sensor(job=pipeline_completo_job, minimum_interval_seconds=30)
def sensor_cambios_datos(context):
    """
    Comprueba si algún fichero de la carpeta ./data/ ha sido modificado
    desde la última ejecución del sensor. Si detecta cambios, lanza el
    pipeline completo.

    El cursor almacena el mtime máximo de todos los ficheros de la carpeta,
    de forma que solo dispara cuando hay un fichero nuevo o actualizado.
    """
    carpeta = "./data"

    if not os.path.isdir(carpeta):
        context.log.warning(f"La carpeta '{carpeta}' no existe, sensor en espera.")
        return

    # Calcular el mtime más reciente de todos los ficheros de la carpeta
    mtimes = []
    for nombre in os.listdir(carpeta):
        ruta = os.path.join(carpeta, nombre)
        if os.path.isfile(ruta):
            mtimes.append(os.path.getmtime(ruta))

    if not mtimes:
        return

    mtime_actual = str(max(mtimes))
    mtime_anterior = context.cursor or "0"

    if mtime_actual != mtime_anterior:
        context.log.info(
            f"Cambio detectado en '{carpeta}'. "
            f"mtime anterior={mtime_anterior}, actual={mtime_actual}"
        )
        context.update_cursor(mtime_actual)
        yield RunRequest(run_key=mtime_actual)
    else:
        context.log.debug("Sin cambios en la carpeta de datos.")


# ── Definiciones de Dagster ───────────────────────────────────────────────────
defs = Definitions(
    assets=load_assets_from_modules([pipeline_renta]),
    asset_checks=load_asset_checks_from_modules([checks_renta]),
    jobs=[pipeline_completo_job],
    sensors=[sensor_cambios_datos],
)
