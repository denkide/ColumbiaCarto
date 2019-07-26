"""Execution code for GIMap server dataset updating."""
import argparse
import logging
import os

import arcetl
from etlassist.pipeline import Job, execute_pipeline

from helper import credential
from helper import database
from helper import dataset
from helper.misc import IGNORE_PATTERNS_RLIDGEO_SNAPSHOT
from helper import path
from helper import transform
from helper import url


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""

DATA_PATH = path.RLID_MAPS_DATA_SHARE
STAGING_PATH = path.RLID_MAPS_STAGING_SHARE

SERVER_URL = url.RLID_MAPS + "arcgis/"
SERVICES_URL = SERVER_URL + "rest/services/"

DATASET_KWARGS_DAILY = [
    {
        "source_path": dataset.COMPARABLE_SALE_TAXLOT.path("pub"),
        "output_path": os.path.join(DATA_PATH, "RLID.gdb\\ComparableSaleTaxlot"),
    }
]
DATASET_KWARGS_FILE = [
    ##TODO: Remove from vicinity service & kill output
    ##OR: Align output with source.
    {
        "source_path": os.path.join(
            path.REGIONAL_DATA, "base\\eug\\EugeneBaseData.gdb\\AirportPavement_FC"
        ),
        "adjust_for_shapefile": True,
        "output_path": os.path.join(STAGING_PATH, "base\\aireug_line.shp"),
    },
    ##TODO: Remove from vicinity service & kill output.
    ##OR: Source from base\eug\EugeneBaseData.gdb\RegBuildings, align output.
    {
        "source_path": None,
        "adjust_for_shapefile": True,
        "output_path": os.path.join(STAGING_PATH, "base\\airport_bld_poly.shp"),
    },
    ##TODO: Align output with source.
    {
        "source_path": dataset.FACILITY.path("maint"),
        "field_name_change_map": {"eug_amenity": "eug_amenit"},
        "adjust_for_shapefile": True,
        "output_path": os.path.join(STAGING_PATH, "base\\facilities.shp"),
    },
    ##TODO: Align output with source.
    {
        "source_path": os.path.join(path.REGIONAL_DATA, "base\\All_Parks.gdb\\Parks"),
        "adjust_for_shapefile": True,
        "output_path": os.path.join(STAGING_PATH, "base\\lane_parks.shp"),
    },
    ##TODO: Align output with source.
    ##TODO: Is this necessary/the best source?
    {
        "source_path": os.path.join(
            path.REGIONAL_DATA, "boundary\\districts\\lc_aoi\\polygon"
        ),
        "adjust_for_shapefile": True,
        "output_path": os.path.join(STAGING_PATH, "base\\lc_aoi.shp"),
    },
    ##TODO: Is this necessary/the best source?
    {
        "source_path": os.path.join(path.REGIONAL_DATA, "base\\lc_highpoints\\point"),
        "adjust_for_shapefile": True,
        "output_path": os.path.join(STAGING_PATH, "base\\lc_highpoints.shp"),
    },
    ##TODO: Replace usage with FC version, remove output.
    {
        "source_path": os.path.join(path.REGIONAL_DATA, "base\\lchydro24k\\arc"),
        "adjust_for_shapefile": True,
        "output_path": os.path.join(STAGING_PATH, "base\\lchydro24k_arc.shp"),
    },
    ##TODO: Replace usage with FC version, remove output.
    {
        "source_path": os.path.join(path.REGIONAL_DATA, "base\\lchydro24k\\polygon"),
        "adjust_for_shapefile": True,
        "output_path": os.path.join(STAGING_PATH, "base\\lchydro24k_poly.shp"),
    },
    ##TODO: Source from RR.gdb\lc_rail, align output.
    {
        "source_path": os.path.join(path.REGIONAL_DATA, "base\\RR.gdb\\RRLineFC"),
        "adjust_for_shapefile": True,
        "output_path": os.path.join(STAGING_PATH, "base\\lcrail.shp"),
    },
    ##TODO: Replace usage with FC version, remove output.
    {
        "source_path": dataset.ROAD.path("pub"),
        "extract_where_sql": "county = 'Lane'",
        "field_name_change_map": {"contributor": "contributo"},
        "adjust_for_shapefile": True,
        "output_path": os.path.join(STAGING_PATH, "base\\lcrds_rlid.shp"),
    },
    ##TODO: Find source dataset for this, align output.
    {
        "source_path": None,
        "output_path": os.path.join(STAGING_PATH, "base\\lcrivers_minor.shp"),
    },
    ##TODO: Source from base\eug\EugeneBaseData.gdb\AirportPavement_FC, align output.
    {
        "source_path": os.path.join(path.REGIONAL_DATA, "base\\eug\\airport_pav.shp"),
        "output_path": os.path.join(STAGING_PATH, "base\\eug\\airport_pav.shp"),
    },
    {
        "source_path": os.path.join(path.REGIONAL_DATA, "base\\eug\\POS_GIS.shp"),
        "output_path": os.path.join(STAGING_PATH, "base\\eug\\POS_GIS.shp"),
    },
    {
        "source_path": os.path.join(
            path.REGIONAL_DATA, "base\\eug\\POS_GIS_single_pol.shp"
        ),
        "output_path": os.path.join(STAGING_PATH, "base\\eug\\POS_GIS_single_pol.shp"),
    },
    ##TODO: Replace usage with FC version, remove output.
    {
        "source_path": dataset.COUNTY_BOUNDARY.path(),
        "adjust_for_shapefile": True,
        "output_path": os.path.join(STAGING_PATH, "boundary\\lanecnty.shp"),
    },
    ##TODO: Replace usage with FC version, remove output.
    {
        "source_path": dataset.INCORPORATED_CITY_LIMITS.path(),
        "field_name_change_map": {"inccityabbr": "city", "inccityname": "name"},
        "adjust_for_shapefile": True,
        "output_path": os.path.join(STAGING_PATH, "boundary\\city_limits\\lccity.shp"),
    },
    ##TODO: Is this necessary/the best source?
    {
        "source_path": os.path.join(
            path.REGIONAL_DATA, "boundary\\districts\\lc_odf_dists\\polygon"
        ),
        "adjust_for_shapefile": True,
        "output_path": os.path.join(
            STAGING_PATH, "boundary\\districts\\lc_odf_dists.shp"
        ),
    },
    ##TODO: Align output with source.
    {
        "source_path": os.path.join(
            path.REGIONAL_DATA, "transport\\ltd\\2015 RideSource Boundary.shp"
        ),
        "output_path": os.path.join(STAGING_PATH, "boundary\\districts\\ltdar.shp"),
    },
    ##TODO: Align output with source.
    {
        "source_path": os.path.join(
            path.REGIONAL_DATA, "transport\\ltd\\2012 LTD Boundary.shp"
        ),
        "output_path": os.path.join(STAGING_PATH, "boundary\\districts\\ltdbndry.shp"),
    },
    ##TODO: Replace usage with FC version, remove output.
    {
        "source_path": os.path.join(
            path.REGIONAL_DATA,
            "boundary\\districts\\eug\\Boundary.gdb\\EugNeighborhoods",
        ),
        "adjust_for_shapefile": True,
        "output_path": os.path.join(
            STAGING_PATH, "boundary\\districts\\eug\\Eugnbhd.shp"
        ),
    },
    ##TODO: Replace usage with FC version, remove output.
    {
        "source_path": dataset.UGB.path("pub"),
        "field_name_change_map": {"ugbcity": "city", "ugbcityname": "label"},
        "adjust_for_shapefile": True,
        "output_path": os.path.join(STAGING_PATH, "boundary\\ugb\\lcugb.shp"),
    },
    ##TODO: Replace usage with FC version, remove output.
    {
        "source_path": os.path.join(path.REGIONAL_DATA, "control\\lcsections\\polygon"),
        "adjust_for_shapefile": True,
        "output_path": os.path.join(STAGING_PATH, "control\\lcsections.shp"),
    },
    ##TODO: Replace usage with FC version, remove output.
    {
        "source_path": os.path.join(path.REGIONAL_DATA, "control\\lctwnrng\\polygon"),
        "adjust_for_shapefile": True,
        "output_path": os.path.join(STAGING_PATH, "control\\lctwnrng.shp"),
    },
    ##TODO: Find source dataset for this, align output (perhaps deprecate?).
    {
        "source_path": None,
        "output_path": os.path.join(STAGING_PATH, "elevation\\lcrelief_map_50.sid"),
    },
    {
        "source_path": os.path.join(path.REGIONAL_DATA, "elevation\\eug\\bench.shp"),
        "output_path": os.path.join(STAGING_PATH, "elevation\\eug\\Bench.shp"),
    },
    # The "Eugene" subfolder is entirely managed by the city, and I have no idea about
    # sourcing or needs. Leave maintenance to city.
    # No need to update census datasets; no changes until after 2020 Census.
    ##TODO: Replace usage with FC version, remove output.
    # {
    #     "source_path": os.path.join(
    #         path.REGIONAL_DATA,
    #         "federal\\census\\lane\\2010\\lc_Census2010.gdb\\lc_blocks2010",
    #     ),
    #     "adjust_for_shapefile": True,
    #     "output_path": os.path.join(
    #         STAGING_PATH, "federal\\census\\lane\\2010\\lc_blocks2010.shp"
    #     ),
    # },
    ##TODO: Replace usage with FC version, remove output.
    # {
    #     "source_path": os.path.join(
    #         path.REGIONAL_DATA,
    #         "federal\\census\\lane\\2010\\lc_Census2010.gdb\\lc_tracts2010",
    #     ),
    #     "adjust_for_shapefile": True,
    #     "output_path": os.path.join(
    #         STAGING_PATH, "federal\\census\\lane\\2010\\lc_tracts2010.shp"
    #     ),
    # },
    # The "image" subfolder should be managed by manual updates/additions-static data.
    ##TODO: Find source dataset for this, align output.
    {
        "source_path": None,
        "output_path": os.path.join(STAGING_PATH, "infra\\Buildings.shp"),
    },
    ##TODO (preferred): Replace usage with Buildings (above).
    ##OR: Find source dataset for this, align output.
    {
        "source_path": None,
        "output_path": os.path.join(STAGING_PATH, "infra\\Buildings_Addresses.shp"),
    },
    ##TODO: Source does not exist. Clarify with Eugene.
    {
        # "source_path": os.path.join(
        #     path.REGIONAL_DATA, "infra\\eug\\projcurline_web.shp"
        # ),
        "output_path": os.path.join(STAGING_PATH, "infra\\eug\\projcurline_web.shp"),
    },
    ##TODO: Source does not exist. Clarify with Eugene.
    {
        # "source_path": os.path.join(
        #     path.REGIONAL_DATA, "infra\\eug\\projcurpoint_web.shp"
        # ),
        "output_path": os.path.join(STAGING_PATH, "infra\\eug\\projcurpoint_web.shp"),
    },
    {
        "source_path": os.path.join(
            path.REGIONAL_DATA, "infra\\eug\\projfutline_web.shp"
        ),
        "output_path": os.path.join(STAGING_PATH, "infra\\eug\\projfutline_web.shp"),
    },
    {
        "source_path": os.path.join(
            path.REGIONAL_DATA, "infra\\eug\\projfutpoint_web.shp"
        ),
        "output_path": os.path.join(STAGING_PATH, "infra\\eug\\projfutpoint_web.shp"),
    },
    ##TODO: Find source dataset for this, align output (perhaps deprecate?).
    {
        "source_path": None,
        "output_path": os.path.join(STAGING_PATH, "natural\\eug\\CityTrees.shp"),
    },
    {
        "source_path": os.path.join(
            path.REGIONAL_DATA, "natural\\eug\\waterbodies.shp"
        ),
        "output_path": os.path.join(STAGING_PATH, "natural\\eug\\waterbodies.shp"),
    },
    ##TODO: Current FHA doesn"t line-up with attribute schema.
    ##TODO: ETL for FloodHazardArea & FloodZoneCode;
    ##repoint service to join of two; remove output.
    {
        "source_path": None,
        "output_path": os.path.join(STAGING_PATH, "natural\\flood\\lcdfirmfldhaz.shp"),
    },
    ##TODO: Replace usage with FC version, remove output.
    {
        "source_path": os.path.join(
            path.REGIONAL_DATA, "natural\\soils\\Soils.gdb\\Soil"
        ),
        "adjust_for_shapefile": True,
        "output_path": os.path.join(STAGING_PATH, "natural\\soils\\lcsoils.shp"),
    },
    ##TODO: Replace usage with FC version, remove output, remove source view.
    {
        "source_path": os.path.join(
            database.ETL_LOAD_A.path, "dbo.TEMP_OutputView_GIMap_Taxlot"
        ),
        "adjust_for_shapefile": True,
        "output_path": os.path.join(STAGING_PATH, "parcel\\taxloteug.shp"),
    },
    ##TODO: Replace usage with FC version, remove output, remove source view.
    {
        "source_path": os.path.join(
            database.ETL_LOAD_A.path, "dbo.TEMP_OutputView_GIMap_Taxlot"
        ),
        "adjust_for_shapefile": True,
        "output_path": os.path.join(STAGING_PATH, "parcel\\taxlots.shp"),
    },
    ##TODO: Replace usage with FC version, remove output.
    {
        "source_path": dataset.SITE_ADDRESS.path("pub"),
        "extract_where_sql": "archived = 'N' and valid = 'Y'",
        "field_name_change_map": {
            "concat_address": "concat_add",
            "house_suffix_code": "house_suff",
            "pre_direction_code": "pre_direct",
            "street_name": "street_nam",
            "street_type_code": "street_typ",
            "unit_type_code": "unit_type_",
            "city_name_abbr": "city_name_",
            "five_digit_zip_code": "five_digit",
            "four_digit_zip_code": "four_digit",
            "initial_create_date": "initial_cr",
            "last_update_date": "last_updat",
            "plandesdesc": "plandesdes",
            "x_coordinate": "x_coordina",
            "y_coordinate": "y_coordina",
        },
        "adjust_for_shapefile": True,
        "output_path": os.path.join(STAGING_PATH, "point\\address.shp"),
    },
    ##TODO: Replace usage with FC version, remove output.
    # Note: This is not what requires the GeoDART shapefile for VBA. That is
    # \\gisrv100.ris5.net\regional\data\point\eug\addresseug.shp.
    {
        "source_path": os.path.join(
            database.ETL_LOAD_A.path, "dbo.OutputView_GeoDART_SiteAddressEug"
        ),
        "adjust_for_shapefile": True,
        "output_path": os.path.join(STAGING_PATH, "point\\addresseug.shp"),
    },
    {
        "source_path": os.path.join(path.REGIONAL_DATA, "state\\laneppl.shp"),
        "output_path": os.path.join(STAGING_PATH, "state\\laneppl.shp"),
    },
    ##TODO: Get newer source from GEO/OSDL, align output with source.
    ##OR: Replace with GEO Admin Framework map service.
    {
        "source_path": os.path.join(
            path.REGIONAL_DATA, "state\\orcntys24_blm\\polygon"
        ),
        "adjust_for_shapefile": True,
        "output_path": os.path.join(STAGING_PATH, "state\\orcntys24_blm_poly.shp"),
    },
    ##TODO: Get newer source from GEO/OSDL, align output with source.
    ##OR: Replace with GEO Admin Framework map service.
    {
        "source_path": os.path.join(
            path.REGIONAL_DATA, "state\\OSDL_Downloads.gdb\\citylim_2015"
        ),
        "extract_where_sql": """
            city_name is not null and city_name not in (
                '', 'Coburg', 'Cottage Grove', 'Creswell', 'Dunes City', 'Eugene',
                'Florence', 'Junction City', 'Lowell', 'Oakridge', 'Springfield',
                'Veneta', 'Westfir'
            )
        """,
        "adjust_for_shapefile": True,
        "output_path": os.path.join(STAGING_PATH, "state\\ore_citylims_poly_nonlc.shp"),
    },
    ##TODO: Get newer source from GEO/OSDL, align output with source.
    ##OR: Replace with GEO Admin Framework map service.
    {
        "source_path": os.path.join(
            path.REGIONAL_DATA, "state\\OSDL_Downloads.gdb\\waterbodies"
        ),
        "adjust_for_shapefile": True,
        "output_path": os.path.join(
            STAGING_PATH, "state\\ore_waterbods_poly_nonlc.shp"
        ),
    },
    {
        "source_path": os.path.join(path.REGIONAL_DATA, "transport\\tma_bndy.shp"),
        "output_path": os.path.join(STAGING_PATH, "transport\\tma_bndy.shp"),
    },
    {
        "source_path": os.path.join(path.REGIONAL_DATA, "transport\\eug\\ROWMask.shp"),
        "output_path": os.path.join(STAGING_PATH, "transport\\eug\\ROWMask.shp"),
    },
]
DATASET_KWARGS_WEEKLY = [
    {
        "source_path": dataset.TAXLOT_FOCUS_BOX.path("large"),
        "field_name_change_map": {"maptaxlot": "maplot"},
        "output_path": os.path.join(DATA_PATH, "GeneralUse.gdb\\FocusBox_lg"),
    },
    # No other source - no update needed.
    {
        "source_path": None,
        "output_path": os.path.join(DATA_PATH, "GeneralUse.gdb\\FocusBox_lg_template"),
    },
    {
        "source_path": dataset.TAXLOT_FOCUS_BOX.path("small"),
        "field_name_change_map": {"maptaxlot": "maplot"},
        "output_path": os.path.join(DATA_PATH, "GeneralUse.gdb\\FocusBox_sm"),
    },
    # No other source - no update needed.
    {
        "source_path": None,
        "output_path": os.path.join(DATA_PATH, "GeneralUse.gdb\\FocusBox_sm_template"),
    },
    # No other source - no update needed.
    {
        "source_path": None,
        "output_path": os.path.join(DATA_PATH, "Misc.gdb\\LaneCountyMaxWebExtent"),
    },
    # No other source - no update needed.
    ##TODO: Move output to Misc.gdb.
    {
        "source_path": None,
        "output_path": os.path.join(DATA_PATH, "RLIDGeo.gdb\\BufferDistance"),
    },
    ##TODO: Repoint services to singular, remove output.
    {
        "source_path": dataset.EWEB_COMMISSIONER.path("pub"),
        "output_path": os.path.join(DATA_PATH, "RLIDGeo.gdb\\EWEBCommissioners"),
    },
    ##TODO: (1) Rename MailingCityArea --> Postal_Community, move to RLIDGeo.
    ##(2) Change update cycle to weekly (put in boundaries ETLs).
    ##(3) Add PC to RLIDGeo.gdb. (4) Repoint services to PC, remove output.
    {
        "source_path": None,
        "output_path": os.path.join(DATA_PATH, "RLIDGeo.gdb\\Site_Address_City_Name"),
    },
    ##TODO: Replace usage with queries to RLID.dbo.Service_Facility.
    {
        "source_path": None,
        "output_path": os.path.join(
            DATA_PATH, "RLIDQuickLook.gdb\\ServiceAnalysis_MetroFireStation"
        ),
    },
    ##TODO: Replace usage with queries to RLID.dbo.Service_Facility.
    {
        "source_path": None,
        "output_path": os.path.join(
            DATA_PATH, "RLIDQuickLook.gdb\\ServiceAnalysis_MetroHydrant"
        ),
    },
    ##TODO: Does this need a sourcing update?
    {
        "source_path": None,
        "output_path": os.path.join(
            DATA_PATH, "RLIDQuickLook.gdb\\SW_GW_GWMA_UNION_FINAL"
        ),
    },
    ##TODO: Source from RLIDGeo.gdb\TaxlotFireProtection, remove output.
    {
        "source_path": None,
        "output_path": os.path.join(
            DATA_PATH, "RLIDQuickLook.gdb\\TaxlotCityFireOverlay"
        ),
    },
    {
        "source_path": os.path.join(
            path.REGIONAL_DATA, "base\\All_Parks.gdb\\Campgrounds"
        ),
        "output_path": os.path.join(DATA_PATH, "base\\All_Parks.gdb\\Campgrounds"),
    },
    {
        "source_path": os.path.join(path.REGIONAL_DATA, "base\\All_Parks.gdb\\Parks"),
        "output_path": os.path.join(DATA_PATH, "base\\All_Parks.gdb\\Parks"),
    },
    {
        "source_path": dataset.FACILITY.path("maint"),
        "output_path": os.path.join(DATA_PATH, "base\\Facilities.gdb\\Facilities"),
    },
    {
        "source_path": dataset.HYDRANT.path(),
        "output_path": os.path.join(DATA_PATH, "base\\Facilities.gdb\\Hydrants"),
    },
    {
        "source_path": os.path.join(
            path.REGIONAL_DATA, "base\\Hydro_24k.gdb\\lchydro24k_arcs"
        ),
        "output_path": os.path.join(DATA_PATH, "base\\Hydro_24k.gdb\\lchydro24k_arcs"),
    },
    {
        "source_path": os.path.join(
            path.REGIONAL_DATA, "base\\Hydro_24k.gdb\\lchydro24k_polys"
        ),
        "output_path": os.path.join(DATA_PATH, "base\\Hydro_24k.gdb\\lchydro24k_polys"),
    },
    {
        "source_path": os.path.join(path.REGIONAL_DATA, "base\\Hydro_24k.gdb\\ocean"),
        "output_path": os.path.join(DATA_PATH, "base\\Hydro_24k.gdb\\ocean"),
    },
    ##TODO: Figure out how to update annotation (if necessary).
    {
        # "source_path": os.path.join(
        #     path.REGIONAL_DATA, "base\\eug\\EugeneBaseData.gdb\\BenchmarksAnno"
        # ),
        "output_path": os.path.join(
            DATA_PATH, "base\\eug\\EugeneBaseData.gdb\\BenchmarksAnno"
        )
    },
    ##TODO: Figure out how to update annotation (if necessary).
    {
        # "source_path": os.path.join(
        #     path.REGIONAL_DATA, "base\\eug\\EugeneBaseData.gdb\\MajorRoadsAnno"
        # ),
        "output_path": os.path.join(
            DATA_PATH, "base\\eug\\EugeneBaseData.gdb\\MajorRoadsAnno"
        )
    },
    ##TODO: Figure out how to update annotation (if necessary).
    {
        # "source_path": os.path.join(
        #     path.REGIONAL_DATA, "base\\eug\\EugeneBaseData.gdb\\NaturalFeaturesAnno"
        # ),
        "output_path": os.path.join(
            DATA_PATH, "base\\eug\\EugeneBaseData.gdb\\NaturalFeaturesAnno"
        )
    },
    {
        "source_path": os.path.join(
            path.REGIONAL_DATA, "base\\eug\\EugeneBaseData.gdb\\RegBuildings"
        ),
        "output_path": os.path.join(
            DATA_PATH, "base\\eug\\EugeneBaseData.gdb\\RegBuildings"
        ),
    },
    {
        "source_path": os.path.join(
            path.REGIONAL_DATA, "base\\eug\\EugeneBaseData.gdb\\RoadSigns"
        ),
        "output_path": os.path.join(
            DATA_PATH, "base\\eug\\EugeneBaseData.gdb\\RoadSigns"
        ),
    },
    ##TODO: Figure out how to update annotation (if necessary).
    {
        # "source_path": os.path.join(
        #     path.REGIONAL_DATA, "base\\eug\\EugeneBaseData.gdb\\RoadSignsAnno"
        # ),
        "output_path": os.path.join(
            DATA_PATH, "base\\eug\\EugeneBaseData.gdb\\RoadSignsAnno"
        )
    },
    ##TODO: Align output with source.
    {
        "source_path": os.path.join(path.REGIONAL_DATA, "base\\RR.gdb\\RRLineFC"),
        "output_path": os.path.join(DATA_PATH, "base\\eug\\RR.gdb\\RRLineFC"),
    },
    # Not updating: static.
    {
        # "source_path": os.path.join(
        #     path.REGIONAL_DATA, "elevation\\LC_elev_10m.gdb\\LC_contours"
        # ),
        "output_path": os.path.join(
            DATA_PATH, "elevation\\LC_elev_10m.gdb\\LC_contours"
        )
    },
    # Not updating: static.
    {
        # "source_path": os.path.join(
        #     path.REGIONAL_DATA, "elevation\\LC_elev_10m.gdb\\lchillshdRS"
        # ),
        "output_path": os.path.join(
            DATA_PATH, "elevation\\LC_elev_10m.gdb\\lchillshdRS"
        )
    },
    # The Eugene subfolder is entirely managed by the city, and I have no idea about
    # sourcing or needs. Leave maintenance to city.
    {
        "source_path": os.path.join(
            database.ETL_LOAD_A.path, "dbo.OutputView_Locator_Road"
        ),
        "output_path": os.path.join(DATA_PATH, "geocode\\Geocode_Data.gdb\\Road"),
    },
    {
        "source_path": os.path.join(
            database.ETL_LOAD_A.path, "dbo.OutputView_Locator_SiteAddress"
        ),
        "output_path": os.path.join(
            DATA_PATH, "geocode\\Geocode_Data.gdb\\SiteAddress"
        ),
    },
    {
        "source_path": os.path.join(
            database.ETL_LOAD_A.path, "dbo.OutputView_Locator_SiteAddress_SansUnit"
        ),
        "output_path": os.path.join(
            DATA_PATH, "geocode\\Geocode_Data.gdb\\SiteAddress_SansUnit"
        ),
    },
    # Leaving `image` folder contents to be managed by manual means.
    ##TODO: Align output with source.
    {
        "source_path": os.path.join(
            path.REGIONAL_DATA, "natural\\soils\\Soils.gdb\\Component"
        ),
        "output_path": os.path.join(
            DATA_PATH, "natural\\soils\\LaneCountySoils_geo.gdb\\Component"
        ),
    },
    ##TODO: Align output with source.
    {
        "source_path": os.path.join(
            path.REGIONAL_DATA, "natural\\soils\\Soils.gdb\\Soil"
        ),
        "output_path": os.path.join(
            DATA_PATH, "natural\\soils\\LaneCountySoils_geo.gdb\\Lane_Alsea_Soils"
        ),
    },
    ##TODO: Align output with source.
    {
        "source_path": os.path.join(
            path.REGIONAL_DATA, "natural\\soils\\Soils.gdb\\MapUnit"
        ),
        "output_path": os.path.join(
            DATA_PATH, "natural\\soils\\LaneCountySoils_geo.gdb\\MapUnit"
        ),
    },
]
LOCATOR_KWARGS = [
    {
        "service_url": SERVICES_URL + "Geocode/RoadRange/GeocodeServer",
        "locator_path": os.path.join(DATA_PATH, "geocode\\RoadRange"),
    },
    {
        "service_url": SERVICES_URL + "Geocode/SiteAddress/GeocodeServer",
        "locator_path": os.path.join(DATA_PATH, "geocode\\SiteAddress"),
    },
    {
        "service_url": (SERVICES_URL + "Geocode/SiteAddress_SansUnit/GeocodeServer"),
        "locator_path": os.path.join(DATA_PATH, "geocode\\SiteAddress_SansUnit"),
    },
]


# ETLs.


def daily_datasets_etl():
    """Run ETL for map server datasets with daily update cycle.

    This script should only be used for updating geodatabase datasets & other managed
    data stores. Purely file-based formats like shapefiles are best updated via
    `file_datasets_etl`, for reasons related to locking mechanisms.
    """
    conn = credential.UNCPathCredential(DATA_PATH, **credential.CPA_MAP_SERVER)
    with conn:
        for kwargs in DATASET_KWARGS_DAILY:
            if kwargs.get("source_path"):
                transform.etl_dataset(**kwargs)


def file_datasets_etl():
    """Run ETL for map server file-based datasets.

    This script should only be used for updating shapefiles & other purely file-based
    datasets. Managed data store formats like geodatabases are best updated via
    `etl_gimap_dataset`, for reasons related to locking mechanisms.

    Essentially, the file-based formats will not append-load on shapefiles locked by a
    service. So we pre-load them to a staging copy, where a server-side batch script
    can clear the locks & wholly replace the files.
    """
    conn = credential.UNCPathCredential(STAGING_PATH, **credential.CPA_MAP_SERVER)
    with conn:
        for kwargs in DATASET_KWARGS_FILE:
            if kwargs.get("source_path"):
                transform.etl_dataset(**kwargs)


def locators_etl():
    """Run ETL for map server locators/geocoders.

    Need to shut down service before rebuilding.
    """
    conn = credential.UNCPathCredential(DATA_PATH, **credential.CPA_MAP_SERVER)
    with conn:
        token = arcetl.services.generate_token(SERVER_URL, **credential.RLID_MAPS)
        for kwargs in LOCATOR_KWARGS:
            arcetl.services.toggle_service(token=token, stop_service=True, **kwargs)
            arcetl.workspace.build_locator(**kwargs)
            arcetl.services.toggle_service(token=token, start_service=True, **kwargs)
    # LOG.info(
    #     "Not updating the locators at the moment."
    #     " Having issues with service not restarting."
    # )


def rlidgeo_datasets_etl():
    """Run ETL for map server datasets in the RLIDGeo replica geodatabase."""
    conn = credential.UNCPathCredential(DATA_PATH, **credential.CPA_MAP_SERVER)
    with conn:
        for name in arcetl.workspace.dataset_names(database.RLIDGEO.path):
            if any(
                pattern.lower() in name.lower()
                for pattern in IGNORE_PATTERNS_RLIDGEO_SNAPSHOT
            ):
                LOG.warning("%s matches ignore-pattern: Skipping.", name)
                continue

            transform.etl_dataset(
                source_path=os.path.join(database.RLIDGEO.path, name),
                output_path=os.path.join(DATA_PATH, "RLIDGeo.gdb", name.split(".")[-1]),
            )


def weekly_datasets_etl():
    """Run ETL for map server datasets with weekly update cycle.

    This script should only be used for updating geodatabase datasets & other managed
    data stores. Purely file-based formats like shapefiles are best updated via
    `file_datasets_etl`, for reasons related to locking mechanisms.
    """
    conn = credential.UNCPathCredential(DATA_PATH, **credential.CPA_MAP_SERVER)
    with conn:
        for kwargs in DATASET_KWARGS_WEEKLY:
            if kwargs.get("source_path"):
                transform.etl_dataset(**kwargs)


# Jobs.


DAILY_JOB = Job("GIMAP_Datasets_Daily", etls=[daily_datasets_etl])

WEEKLY_01_JOB = Job("GIMAP_Datasets_Weekly_01", etls=[file_datasets_etl])

WEEKLY_02_JOB = Job(
    "GIMAP_Datasets_Weekly_02",
    etls=[rlidgeo_datasets_etl, weekly_datasets_etl, locators_etl],
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
