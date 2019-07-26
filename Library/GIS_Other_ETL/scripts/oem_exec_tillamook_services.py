"""Execution code for updating data for Tillamook web services."""
import argparse
import logging
import os

from etlassist.pipeline import Job, execute_pipeline

from helper import credential
from helper import database
from helper import path
from helper import transform


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""

DATA_PATH = os.path.join(path.RLID_MAPS_DATA_SHARE, 'LCOG', 'Tillamook')
GIS_DATA_PATH = os.path.join(path.TILLAMOOK_PROJECT, 'GIS_Data')
GDB_PATH = os.path.join(GIS_DATA_PATH, 'Tillamook.gdb')
GDB_WORKING_PATH = os.path.join(GIS_DATA_PATH, 'Tillamook_Working.gdb')
RESOURCE_DATA_PATH = os.path.join(GIS_DATA_PATH, 'Tillamook_Projects_Data',
                                  'E911_Maintenance', 'Resource_Data')

KWARGS_NIGHTLY_DATASETS = {
    'Tillamook.gdb': (
        {'source_path': os.path.join(database.LCOG_TILLAMOOK_ECD.path,
                                     'dbo.Address_Point'),
         'output_name': 'Address_Point'},
        {'source_path': os.path.join(database.LCOG_TILLAMOOK_ECD.path,
                                     'dbo.City_Limits'),
         'output_name': 'City_Limits'},
        {'source_path': os.path.join(database.LCOG_TILLAMOOK_ECD.path,
                                     'dbo.Emergency_Service_Zone'),
         'output_name': 'Emergency_Service_Zone'},
        {'source_path': os.path.join(database.LCOG_TILLAMOOK_ECD.path,
                                     'dbo.Road_Centerline'),
         'output_name': 'Road_Centerline'},
        ),
    }
KWARGS_MONTHLY_DATASETS = {
    'Tillamook.gdb': (
        {'source_path': os.path.join(GDB_WORKING_PATH, 'Address_Grid'),
         'output_name': 'Address_Grid'},
        {'source_path': os.path.join(GDB_WORKING_PATH, 'Address_Grid_Index'),
         'output_name': 'Address_Grid_Index'},
        {'source_path': os.path.join(RESOURCE_DATA_PATH,
                                     'TILLAMOOK_BRIDGES.shp'),
         'output_name': 'Bridges'},
        {'source_path': os.path.join(GIS_DATA_PATH, 'DOGAMI',
                                     'tillamook_buildings.shp'),
         'output_name': 'Building_Footprints'},
        {'source_path': os.path.join(RESOURCE_DATA_PATH,
                                     'TILLAMOOK_CELLTOWER.shp'),
         'output_name': 'Cell_Towers'},
        {'source_path': os.path.join(GDB_PATH, 'County_Line'),
         'output_name': 'County_Line'},
        {'source_path': os.path.join(GDB_WORKING_PATH, 'ODF_Driveways_Snapped'),
         'output_name': 'Driveways'},
        {'source_path': os.path.join(GDB_PATH, 'Emergency_Responders'),
         'output_name': 'Emergency_Responders'},
        {'source_path': os.path.join(database.LCOG_TILLAMOOK_ECD.path,
                                     'dbo.EMS'),
         'output_name': 'EMS'},
        {'source_path': os.path.join(database.LCOG_TILLAMOOK_ECD.path,
                                     'dbo.Fire'),
         'output_name': 'Fire'},
        {'source_path': os.path.join(GDB_PATH, 'GNIS_Feature_Points'),
         'output_name': 'GNIS_Feature_Points'},
        {'source_path': os.path.join(GDB_PATH, 'Mile_Posts'),
         'output_name': 'Mile_Posts'},
        {'source_path': os.path.join(GDB_PATH, 'ODF_Land_Management'),
         'output_name': 'ODF_Land_Management'},
        {'source_path': os.path.join(GDB_PATH, 'Oregon_Locations'),
         'output_name': 'Oregon_Locations'},
        {'source_path': os.path.join(database.LCOG_TILLAMOOK_ECD.path,
                                     'dbo.Police'),
         'output_name': 'Police'},
        {'source_path': os.path.join(database.LCOG_TILLAMOOK_ECD.path,
                                     'dbo.Postal_Community'),
         'output_name': 'Postal_Community'},
        {'source_path': os.path.join(GDB_PATH, 'PSAP_Boundary_OEM'),
         'output_name': 'PSAP_Boundary_OEM'},
        {'source_path': os.path.join(GDB_PATH, 'Public_Facilities'),
         'output_name': 'Public_Facilities'},
        {'source_path': os.path.join(GDB_PATH, 'Railroads'),
         'output_name': 'Railroads'},
        {'source_path': os.path.join(GDB_PATH, 'Rivers_Lakes'),
         'output_name': 'Rivers_Lakes'},
        {'source_path': os.path.join(GDB_PATH, 'Rivers_Streams'),
         'output_name': 'Rivers_Streams'},
        {'source_path': os.path.join(GDB_PATH, 'Schools'),
         'output_name': 'Schools'},
        # {'source_path': os.path.join(GDB_PATH, 'Township_Sections'),
        #  'output_name': 'Sections'},
        {'source_path': os.path.join(GDB_PATH, 'Shoreline_CUSP'),
         'output_name': 'Shoreline_CUSP'},
        {'source_path': os.path.join(GDB_PATH, 'State_Parks'),
         'output_name': 'State_Parks'},
        {'source_path': os.path.join(GDB_PATH, 'Tax_Lots'),
         'output_name': 'Tax_Lots'},
        # {'source_path': os.path.join(GDB_PATH, 'Townships'),
        #  'output_name': 'Townships'},
        {'source_path': os.path.join(GDB_PATH, 'Urban_Growth_Boundary'),
         'output_name': 'Urban_Growth_Boundary'},
        {'source_path': os.path.join(GDB_PATH, 'Water_Access'),
         'output_name': 'Water_Access'},
        ),
    }


# ETLs.

def service_datasets_nightly_etl():
    """Run ETL for services datasets with nightly update cycle.

    This script should only be used for updating geodatabase datasets & other
    managed data stores. Purely file-based formats like shapefiles are best
    updated in another manner, for reasons related to locking mechanisms.
    """
    conn = credential.UNCPathCredential(path.RLID_MAPS_DATA_SHARE,
                                        **credential.CPA_MAP_SERVER)
    with conn:
        for gdb_relpath in sorted(KWARGS_NIGHTLY_DATASETS):
            LOG.info("Update datasets in %s", gdb_relpath)
            gdb_path = os.path.join(DATA_PATH, gdb_relpath)
            for kwargs in KWARGS_NIGHTLY_DATASETS[gdb_relpath]:
                kwargs['output_path'] = os.path.join(gdb_path,
                                                     kwargs['output_name'])
                transform.etl_dataset(**kwargs)


def service_datasets_monthly_etl():
    """Run ETL for GIMap datasets with weekly update cycle.

    This script should only be used for updating geodatabase datasets & other
    managed data stores. Purely file-based formats like shapefiles are best
    updated in another manner, for reasons related to locking mechanisms.
    """
    conn = credential.UNCPathCredential(path.RLID_MAPS_DATA_SHARE,
                                        **credential.CPA_MAP_SERVER)
    with conn:
        for gdb_relpath in sorted(KWARGS_MONTHLY_DATASETS):
            LOG.info("Update datasets in %s", gdb_relpath)
            gdb_path = os.path.join(DATA_PATH, gdb_relpath)
            for kwargs in KWARGS_MONTHLY_DATASETS[gdb_relpath]:
                kwargs['output_path'] = os.path.join(gdb_path,
                                                     kwargs['output_name'])
                transform.etl_dataset(**kwargs)


# Jobs.

NIGHTLY_JOB = Job('OEM_Tillamook_Service_Datasets_Nightly',
                  etls=(
                      service_datasets_nightly_etl,
                      ))


MONTHLY_JOB = Job('OEM_Tillamook_Service_Datasets_Monthly',
                  etls=(
                      service_datasets_monthly_etl,
                      ))


# Execution.

DEFAULT_PIPELINES = ()


def main():
    """Script execution code."""
    args = argparse.ArgumentParser()
    args.add_argument('pipelines', nargs='*', help="Pipeline(s) to run")
    # Collect pipeline objects.
    pipelines = (tuple(globals()[arg] for arg in args.parse_args().pipelines)
                 if args.parse_args().pipelines else DEFAULT_PIPELINES)
    # Execute.
    for pipeline in pipelines:
        execute_pipeline(pipeline)


if __name__ == '__main__':
    main()
