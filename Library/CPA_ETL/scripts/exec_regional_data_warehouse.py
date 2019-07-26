"""Execution code for regional data (file) warehouse processing."""
import argparse
import logging
import os

import arcetl
import arcpy
from etlassist.pipeline import Job, execute_pipeline

from helper import database
from helper import path
from helper import transform


##TODO: Replace source views with update kwargs to make transformations.

LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""

GEOCODE_PATH = os.path.join(path.REGIONAL_DATA, "geocode")
"""str: Path in regional warehouse for geocoder files."""
KWARGS_DATASETS = [
    {
        "source_path": os.path.join(
            database.ETL_LOAD_A.path, "dbo.OutputView_Locator_Road"
        ),
        "output_path": os.path.join(GEOCODE_PATH, "Geocode_Data.gdb", "Road"),
    },
    {
        "source_path": os.path.join(
            database.ETL_LOAD_A.path, "dbo.OutputView_Locator_SiteAddress"
        ),
        "output_path": os.path.join(GEOCODE_PATH, "Geocode_Data.gdb", "Site_Address"),
    },
    {
        "source_path": os.path.join(
            database.ETL_LOAD_A.path, "dbo.OutputView_Locator_SiteAddress_SansUnit"
        ),
        "output_path": os.path.join(
            GEOCODE_PATH, "Geocode_Data.gdb", "Site_Address_Sans_Unit"
        ),
    },
    ##TODO: Wait for GeoDART to finally be updated away from the VBA that requires this
    ##format; repoint them to RLIDGeo FC; remove output & source view.
    {
        "source_path": os.path.join(
            database.ETL_LOAD_A.path, "dbo.OutputView_GeoDART_SiteAddressEug"
        ),
        "adjust_for_shapefile": True,
        "output_path": os.path.join(path.REGIONAL_DATA, "point\\eug", "addresseug.shp"),
    },
    {
        "source_path": os.path.join(
            database.ETL_LOAD_A.path, "dbo.OutputView_FireView_Road"
        ),
        "output_path": os.path.join(
            path.REGIONAL_DATA, "psap\\FireViewData.gdb", "Roads_FV"
        ),
    },
    {
        "source_path": os.path.join(database.EUGENEGEO.path, "eugadm.Travway"),
        "output_path": os.path.join(
            path.REGIONAL_DATA, "transport\\eug\\Transportation.gdb", "Travway"
        ),
    },
    {
        "source_path": os.path.join(
            database.ETL_LOAD_A.path, "dbo.OutputView_SunGardCAD_SiteAddress"
        ),
        "output_path": os.path.join(
            database.ADDRESSING.path, "dbo.SunGardCAD_SiteAddress"
        ),
    },
    {
        "source_path": os.path.join(
            database.ETL_LOAD_A.path, "dbo.OutputView_LaneAccela_SiteAddress"
        ),
        "output_path": os.path.join(
            path.REGIONAL_STAGING, "Accela\\Lane\\Accela.gdb", "SiteAddress_Extract"
        ),
    },
    {
        "source_path": os.path.join(
            database.ETL_LOAD_A.path, "dbo.OutputView_LTDTrapeze_Road"
        ),
        "output_path": os.path.join(database.LCOGGEO.path, "dbo.LTDTrapeze_Road"),
    },
]
"""list: Collection of keyword arguments for update."""
LOCATOR_NAMES = ["Road_Range", "Site_Address", "Site_Address_Sans_Unit"]
"""list: Collection of locator names in the regional warehouse."""


# ETLs.


def datasets_update():
    """Run update for regional data warehouse datasets."""
    LOG.info("Start: Update datasets in regional data warehouse.")
    for kwargs in KWARGS_DATASETS:
        if kwargs.get("source_path"):
            transform.etl_dataset(**kwargs)
    LOG.info("End: Update.")


def locators_update():
    """Run update for map server locators/geocoders."""
    for name in LOCATOR_NAMES:
        locator_path = os.path.join(GEOCODE_PATH, name)
        package_path = os.path.join(GEOCODE_PATH, "packaged", name + ".gcpk")
        arcetl.workspace.build_locator(locator_path)
        ##TODO: Create arcetl.workspace.package_locator function, then use here.
        old_overwrite_output = arcpy.env.overwriteOutput
        arcpy.env.overwriteOutput = True
        arcpy.PackageLocator_management(locator_path, package_path)
        arcpy.env.overwriteOutput = old_overwrite_output


# Jobs.


WEEKLY_JOB = Job(
    "Regional_Data_Warehouse_Weekly", etls=[datasets_update, locators_update]
)


# Execution.


def main():
    """Script execution code."""
    args = argparse.ArgumentParser()
    args.add_argument("pipelines", nargs="*", help="Pipeline(s) to run")
    available_names = {key for key in list(globals()) if not key.startswith("__")}
    pipeline_names = args.parse_args().pipelines
    if pipeline_names and available_names.issuperset(pipeline_names):
        pipelines = [globals()[arg] for arg in args.parse_args().pipelines]
        for pipeline in pipelines:
            execute_pipeline(pipeline)
    else:
        console = logging.StreamHandler()
        LOG.addHandler(console)
        if not pipeline_names:
            LOG.error("No pipeline arguments.")
        for arg in pipeline_names:
            if arg not in available_names:
                LOG.error("`%s` not available in exec.", arg)
        LOG.error(
            "Available objects in exec: %s",
            ", ".join("`{}`".format(name) for name in sorted(available_names)),
        )


if __name__ == "__main__":
    main()
