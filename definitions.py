from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
from scripts import pipeline_renta, checks_renta

defs = Definitions(
    assets=load_assets_from_modules([pipeline_renta]),
    asset_checks=load_asset_checks_from_modules([checks_renta]),
)