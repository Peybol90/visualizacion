from dagster import Definitions, load_assets_from_modules
from scripts import pipeline_renta

defs = Definitions(
    assets=load_assets_from_modules([pipeline_renta])
)