"""File system path objects for non-CPA projects.

Extends the ETLAssist path submodule.
"""
import os

from etlassist.path import *


# Network shares.


LCOG_GIS_SHARE = "\\\\clsrv111.int.lcog.org\\gis"
REGIONAL_FILE_SHARE = "\\\\gisrv100.ris5.net\\regional"
RLID_DB_STAGING = "\\\\gisql113.ris5.net\\Staging"
# GIMAP240.
RLID_MAPS_DATA_SHARE = "\\\\open.maps.rlid.org\\Data"
RLID_MAPS_WWW_SHARE = "\\\\open.maps.rlid.org\\www"


# Repository paths.


GIS_OTHER_ETL = os.path.normpath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
)
LCOG_GIS_PROJECTS = os.path.join(LCOG_GIS_SHARE, "projects")


# Project paths.


LCSO_EIS_CAD_PROJECT = os.path.join(LCOG_GIS_PROJECTS, "Public_Safety\\LCSO\\EIS_CAD")
LANE_ADDRESS_POINTS_PROJECT = os.path.join(LCOG_GIS_PROJECTS, "Address_Points")
TILLAMOOK_PROJECT = os.path.join(LCOG_GIS_PROJECTS, "Tillamook")
