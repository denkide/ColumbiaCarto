"""File system path objects.

Extends the `path` submodule from the ETLAssist package.
"""
import os

from etlassist.path import *  # pylint: disable=wildcard-import, unused-wildcard-import


# Servers.


# GISRV100 (steward: Jacob Blair).
CPA_FILE_SERVER = "\\\\gisrv100.ris5.net"
# GIMAP240 (steward: Nick Seigal).
CPA_MAP_SERVER = "\\\\open.maps.rlid.org"
# GIWEB300 (steward: Ann Terrell).
CPA_WEB_SERVER = "\\\\rlid.org"
# CESRV811 (steward: Jim Beasley).
EUGENE_FILE_SERVER = "\\\\cesrv811.eugene1.net"
# LCIMG17A (steward: Scott Noble).
LANE_FILE_SERVER = "\\\\lcimg17a.lc100.net"
LCOG_FILE_SERVER = "\\\\clsrv111.int.lcog.org"


# Network shares.


CPA_WORK_SHARE = os.path.join(CPA_FILE_SERVER, "work")
EUGENE_DATABASE_SHARE = os.path.join(EUGENE_FILE_SERVER, "DATABASE")
EUGENE_IMAGES_SHARE = os.path.join(EUGENE_FILE_SERVER, "Images")
EUGENE_IMAGE2_SHARE = os.path.join(EUGENE_FILE_SERVER, "Image2")
LANE_IMAGES_SHARE = os.path.join(LANE_FILE_SERVER, "LCImage")
LANE_TAX_MAPS_SHARE = os.path.join(LANE_FILE_SERVER, "TAX_MAPS")
LCOG_GIS_SHARE = os.path.join(LCOG_FILE_SERVER, "gis")
REGIONAL_FILE_SHARE = os.path.join(CPA_FILE_SERVER, "regional")
RLID_MAPS_DATA_SHARE = os.path.join(CPA_MAP_SERVER, "Data")
RLID_MAPS_STAGING_SHARE = os.path.join(CPA_MAP_SERVER, "Staging")
RLID_MAPS_WWW_SHARE = os.path.join(CPA_MAP_SERVER, "www")
RLID_DATA_SHARE = os.path.join(CPA_WEB_SERVER, "RLIDData")
RLID_DATA_STAGING_SHARE = os.path.join(CPA_WEB_SERVER, "RLIDData_Staging")
SDE_DATA_BACKUP_SHARE = os.path.join(CPA_FILE_SERVER, "SDE_Data_Backup")


# Repository paths.


# First-level.
CPA_ETL = os.path.normpath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
)
LANE_TAX_MAP_IMAGES = os.path.join(LANE_IMAGES_SHARE, "TaxMap")
LANE_PROPERTY_CARDS = os.path.join(LANE_TAX_MAPS_SHARE, "ATDCard")
LCOG_GIS_PROJECTS = os.path.join(LCOG_GIS_SHARE, "projects")
REGIONAL_DATA = os.path.join(REGIONAL_FILE_SHARE, "data")
REGIONAL_STAGING = os.path.join(REGIONAL_FILE_SHARE, "staging")
# Second-level.
RESOURCES = os.path.join(CPA_ETL, "resources")
# Third-level.
BUILT_RESOURCES = os.path.join(RESOURCES, "Built_Resources.gdb")


# Script paths.


GENERATE_BUILD_SQL = os.path.join(RESOURCES, "generate_database_build_sql.ps1")
