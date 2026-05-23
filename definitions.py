"""
Proyecto Final – Visualización
Tajogaite: Anatomía de un territorio herido
Dagster definitions entry point
"""

from dagster import (
    Definitions,
    load_assets_from_modules,
    load_asset_checks_from_modules,
)

from scripts import (
    assets_raw,
    assets_clean,
    assets_analysis,
    assets_viz,
    assets_maps,
    checks,
    sensor,
)

all_assets = load_assets_from_modules([
    assets_raw,
    assets_clean,
    assets_analysis,
    assets_viz,
    assets_maps,
])

all_checks = load_asset_checks_from_modules([checks])

defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
    sensors=[sensor.sensor_cambios_datos],
    jobs=[sensor.pipeline_completo_job],
)