"""Execution code for address processing."""
import argparse
import datetime
import logging
import os

import arcetl
from etlassist.pipeline import Job, execute_pipeline

from helper.communicate import send_email
from helper import dataset
from helper.misc import address_intid_to_uuid_map
from helper import path
from helper.value import concatenate_arguments, is_numeric, maptaxlot_separated
from helper import transform


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""

OVERRIDE_ATTRS = {
    # Outside Lane county, but necessary for Central Lane PSAP response.
    "97089 Five Rivers Rd": {
        "where_sql": """
            house_nbr = 97089
            and street_name = 'FIVE RIVERS'
            and street_type_code = 'RD'
            and city_name_abbr = 'TID'
        """,
        "overlay_kwargs": [
            {"field_name": "ambulance_district", "value": "NW"},
            {"field_name": "psap_code", "value": "LI"},
        ],
        "constant_kwargs": [{"field_name": "county_name", "value": "Lincoln"}],
    },
    # Outside Lane county, but necessary for Central Lane PSAP response.
    "26629 Bennett Blvd": {
        "where_sql": """
            house_nbr = 26629
            and street_name = 'BENNETT'
            and street_type_code = 'BLVD'
            and city_name_abbr = 'MON'
        """,
        "overlay_kwargs": [
            {"field_name": "ambulance_district", "value": "NC"},
            {"field_name": "psap_code", "value": "CL"},
        ],
        "constant_kwargs": [{"field_name": "county_name", "value": "Benton"}],
    },
}

KWARGS_ISSUES_MESSAGE = {
    "subject": "Address Publication Update Issues",
    "recipients": ["keith.oregon@gmail.com"],
    "copy_recipients": ["david@davidrenz.com"],
    "blind_copy_recipients": ["david@davidrenz.com"],
    "reply_to": "david@davidrenz.com",
    "body": """
        <p>Issues were found that prevent certain addresses from updating to
            publication datasets.<br />
            <em>Refer to feature class Addressing.dbo.Issues for all current
            issues.</em>
        </p>
        <table style="width:100%">
            {}{}
        </table>
    """,
    "body_format": "HTML",
}
KWARGS_NEW_LINCOLN_MESSAGE = {
    "subject": "Recently-added Addresses in Lincoln PSAP Area",
    "recipients": ["keith.oregon@gmail.com"],
    "copy_recipients": ["david@davidrenz.com"],
    "blind_copy_recipients": ["david@davidrenz.com"],
    "reply_to": "david@davidrenz.com",
    "body": """
        <p>Recently-added addresses were found in the Lincoln PSAP area.</p>
        <table style="width:100%">
            {}{}
        </table>
        """,
    "body_format": "HTML",
}


# Helpers.


def city_state_zip(**kwargs):
    """Return "{city}, {state code}  {ZIP code}"."""
    result = "{city_name}, {state_code}".format(**kwargs)
    if kwargs["five_digit_zip_code"]:
        # RLID for some reason has two spaces between state & ZIP.
        result += "  {five_digit_zip_code}".format(**kwargs)
    return result


def concat_address_full(**kwargs):
    """Return concatenated full-address for RLID."""
    result = "{concat_address} {city_name}, {state_code}".format(**kwargs)
    if kwargs["five_digit_zip_code"]:
        result += " {five_digit_zip_code}".format(**kwargs)
    if kwargs["four_digit_zip_code"]:
        result += "-{four_digit_zip_code}".format(**kwargs)
    return result


def send_new_lincom_address_message():
    """Send message for any recently-added address in the Lincoln PSAP area."""
    keys = ["city_name", "concat_address", "geofeat_id", "initial_create_date"]
    addresses = sorted(
        addr
        for addr in arcetl.attributes.as_iters(
            dataset.SITE_ADDRESS.path("pub"),
            field_names=keys,
            dataset_where_sql="psap_code = 'LI' ",
        )
        if (datetime.datetime.now() - addr[-1]).days < 15
    )
    table_header = "<tr>{}</tr>".format(
        "".join("<th>{}</th>".format(key) for key in keys)
    )
    row_template = "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>"
    if addresses:
        LOG.warning("Found new addresses in Lincoln PSAP area: sending email.")
        table_rows = "".join(row_template.format(*addr) for addr in addresses)
        KWARGS_NEW_LINCOLN_MESSAGE["body"] = KWARGS_NEW_LINCOLN_MESSAGE["body"].format(
            table_header, table_rows
        )
        send_email(**KWARGS_NEW_LINCOLN_MESSAGE)
    else:
        LOG.info("No new addresses in Lincoln PSAP area found. Not sending email.")


def send_publication_issues_message():
    """Send message of issues that affect address publication."""
    keys = ["description", "city_name", "concat_address", "geofeat_id"]
    issues = sorted(
        arcetl.attributes.as_iters(
            dataset.ADDRESS_ISSUES.path(),
            field_names=keys,
            dataset_where_sql="update_publication = 0",
        )
    )
    table_header = "<tr>{}</tr>".format(
        "".join("<th>{}</th>".format(key) for key in keys)
    )
    row_template = "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>"
    if issues:
        LOG.warning("Found validation publication issues: sending email.")
        table_rows = "".join(row_template.format(*issue) for issue in issues)
        KWARGS_ISSUES_MESSAGE["body"] = KWARGS_ISSUES_MESSAGE["body"].format(
            table_header, table_rows
        )
        send_email(**KWARGS_ISSUES_MESSAGE)
    else:
        LOG.info("No validation publication issues found. Not sending email.")


# ETLs.


def facility_etl():
    """Run ETL for facilities.

    Currently only undertaken for other ETL purposes--not publication.
    """
    with arcetl.ArcETL("Facilities") as etl:
        etl.extract(dataset.FACILITY.path("maint"))
        etl.transform(
            arcetl.dataset.rename_field,
            field_name="geofeat_id",
            new_field_name="address_intid",
        )
        # Clean maintenance values.
        transform.clear_nonpositive(etl, field_names=["address_intid"])
        transform.clean_whitespace(
            etl, field_names=["label", "label_full", "type", "type_full"]
        )
        transform.force_lowercase(etl, field_names=["type"])
        transform.force_uppercase(etl, field_names=["label"])
        transform.add_missing_fields(etl, dataset.FACILITY, tags=["pub"])
        # Assign geometry attributes.
        coordinate_system_xy_keys = {
            2914: {"x": "x_coordinate", "y": "y_coordinate"},
            4326: {"x": "longitude", "y": "latitude"},
        }
        for spatial_reference_id, xy_key in coordinate_system_xy_keys.items():
            for axis, key in xy_key.items():
                etl.transform(
                    arcetl.attributes.update_by_geometry,
                    field_name=key,
                    spatial_reference_item=spatial_reference_id,
                    geometry_properties=["centroid", axis],
                )
        etl.transform(
            arcetl.attributes.update_by_mapping,
            field_name="address_uuid",
            mapping=address_intid_to_uuid_map,
            key_field_names=["address_intid"],
        )
        etl.load(dataset.FACILITY.path("pub"))


def site_address_etl():
    """Run ETL for site addresses."""
    with arcetl.ArcETL("Site Addresses") as etl:
        etl.extract(dataset.SITE_ADDRESS.path("maint"))
        # Clean maintenance values.
        transform.clear_nonpositive(etl, field_names=["house_nbr"])
        transform.clean_whitespace(
            etl,
            field_names=[
                "house_suffix_code",
                "pre_direction_code",
                "street_name",
                "street_type_code",
                "unit_type_code",
                "unit_id",
                "city_name",
                "landuse",
                "maptaxlot",
                "account",
            ],
        )
        transform.force_uppercase(
            etl,
            field_names=[
                "house_suffix_code",
                "pre_direction_code",
                "street_name",
                "street_type_code",
                "unit_type_code",
                "unit_id",
                "maptaxlot",
                "valid",
                "archived",
            ],
        )
        transform.clear_non_numeric_text(etl, field_names=["account"])
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="landuse",
            function=(lambda x: x if is_numeric(x) else "0"),
        )
        transform.force_yn(etl, field_names=["archived"], default="N")
        transform.force_yn(etl, field_names=["valid"], default="Y")
        transform.add_missing_fields(etl, dataset.SITE_ADDRESS, tags=["pub"])
        # Assign geometry attributes.
        coordinate_system_xy_keys = {
            2914: {"x": "x_coordinate", "y": "y_coordinate"},
            4326: {"x": "longitude", "y": "latitude"},
        }
        for spatial_reference_id, xy_key in coordinate_system_xy_keys.items():
            for axis, key in xy_key.items():
                etl.transform(
                    arcetl.attributes.update_by_geometry,
                    field_name=key,
                    spatial_reference_item=spatial_reference_id,
                    geometry_properties=["centroid", axis],
                )
        # Assign overlays.
        overlay_kwargs = [
            # City attributes.
            {
                "field_name": "geocity",
                "overlay_field_name": "inccityabbr",
                "overlay_dataset_path": dataset.INCORPORATED_CITY_LIMITS.path(),
            },
            {
                "field_name": "annexhist",
                "overlay_field_name": "annexnum",
                "overlay_dataset_path": dataset.ANNEXATION_HISTORY.path("pub"),
            },
            # Have to do overlay rather than join because some lack codes.
            {
                "field_name": "yearanx",
                "overlay_field_name": "annexyear",
                "overlay_dataset_path": dataset.ANNEXATION_HISTORY.path("pub"),
            },
            {
                "field_name": "ugb",
                "overlay_field_name": "ugbcity",
                "overlay_dataset_path": dataset.UGB.path("pub"),
            },
            # Planning & zoning attributes.
            {
                "field_name": "greenwy",
                "overlay_field_name": "greenway",
                "overlay_dataset_path": dataset.WILLAMETTE_RIVER_GREENWAY.path("pub"),
            },
            {
                "field_name": "nodaldev",
                "overlay_field_name": "nodearea",
                "overlay_dataset_path": dataset.NODAL_DEVELOPMENT_AREA.path("pub"),
            },
            {
                "field_name": "plandes_id",
                "overlay_field_name": "plandes_id",
                "overlay_dataset_path": dataset.PLAN_DESIGNATION.path("pub"),
            },
            {
                "field_name": "sprsvcbndy",
                "overlay_field_name": "is_inside",
                "overlay_dataset_path": dataset.SPRINGFIELD_HANSEN_EXTENT.path(),
            },
            # Public safety attributes.
            {
                "field_name": "ambulance_district",
                "overlay_field_name": "asacode",
                "overlay_dataset_path": dataset.AMBULANCE_SERVICE_AREA.path("pub"),
            },
            {
                "field_name": "firedist",
                "overlay_field_name": "fireprotprov",
                "overlay_dataset_path": dataset.FIRE_PROTECTION_AREA.path("pub"),
            },
            {
                "field_name": "police_beat",
                "overlay_field_name": "CAD",
                "overlay_dataset_path": os.path.join(
                    path.LCOG_GIS_PROJECTS,
                    "Public_Safety\\PSAPS\\CLPSAP\\SunGard_CAD\\Maintained_Layers",
                    "Maintained_Layers.gdb\\Fire_Law_Tow\\law_beat",
                ),
            },
            {
                "field_name": "psap_code",
                "overlay_field_name": "psap_code",
                "overlay_dataset_path": dataset.PSAP_AREA.path("pub"),
            },
            # Election attributes.
            {
                "field_name": "electionpr",
                "overlay_field_name": "precntnum",
                "overlay_dataset_path": dataset.ELECTION_PRECINCT.path("pub"),
            },
            {
                "field_name": "ccward",
                "overlay_field_name": "ward",
                "overlay_dataset_path": dataset.CITY_WARD.path(),
            },
            {
                "field_name": "clpud_subdivision",
                "overlay_field_name": "SUBDIVISIO",
                "overlay_dataset_path": os.path.join(
                    path.LCOG_GIS_PROJECTS,
                    "UtilityDistricts\\CentralLincolnPUD\\Redistricting2012",
                    "CLPUD_Subdivisions.shp",
                ),
            },
            {
                "field_name": "cocommdist",
                "overlay_field_name": "commrdist",
                "overlay_dataset_path": (
                    dataset.COUNTY_COMMISSIONER_DISTRICT.path("pub")
                ),
            },
            {
                "field_name": "epud",
                "overlay_field_name": "boardid",
                "overlay_dataset_path": dataset.EPUD_SUBDISTRICT.path("pub"),
            },
            {
                "field_name": "hwpud_subdivision",
                "overlay_field_name": "BoardZone",
                "overlay_dataset_path": os.path.join(
                    path.LCOG_GIS_PROJECTS,
                    "UtilityDistricts\\HecetaWaterPUD\\NewBoardSubzones",
                    "HecetaData.gdb",
                    "ScenarioB",
                ),
            },
            {
                "field_name": "lcczone",
                "overlay_field_name": "lccbrdzone",
                "overlay_dataset_path": dataset.LCC_BOARD_ZONE.path("pub"),
            },
            {
                "field_name": "senatedist",
                "overlay_field_name": "sendist",
                "overlay_dataset_path": dataset.STATE_SENATOR_DISTRICT.path("pub"),
            },
            {
                "field_name": "strepdist",
                "overlay_field_name": "repdist",
                "overlay_dataset_path": (
                    dataset.STATE_REPRESENTATIVE_DISTRICT.path("pub")
                ),
            },
            {
                "field_name": "swcd",
                "overlay_field_name": "swcdist",
                "overlay_dataset_path": (
                    dataset.SOIL_WATER_CONSERVATION_DISTRICT.path("pub")
                ),
            },
            {
                "field_name": "swcdzone",
                "overlay_field_name": "swczone",
                "overlay_dataset_path": (
                    dataset.SOIL_WATER_CONSERVATION_DISTRICT.path("pub")
                ),
            },
            # Education attributes.
            {
                "field_name": "schooldist",
                "overlay_field_name": "district",
                "overlay_dataset_path": dataset.SCHOOL_DISTRICT.path("pub"),
            },
            {
                "field_name": "elem",
                "overlay_field_name": "attend",
                "overlay_dataset_path": dataset.ELEMENTARY_SCHOOL_AREA.path("pub"),
            },
            {
                "field_name": "middle",
                "overlay_field_name": "attend",
                "overlay_dataset_path": dataset.MIDDLE_SCHOOL_AREA.path("pub"),
            },
            {
                "field_name": "high",
                "overlay_field_name": "attend",
                "overlay_dataset_path": dataset.HIGH_SCHOOL_AREA.path("pub"),
            },
            # Transportation attributes.
            {
                "field_name": "ltddist",
                "overlay_field_name": "LTD",
                "overlay_dataset_path": os.path.join(
                    path.REGIONAL_DATA, "transport\\ltd\\2012 LTD Boundary.shp"
                ),
            },
            {
                "field_name": "ltdridesrc",
                "overlay_field_name": "LTD",
                "overlay_dataset_path": os.path.join(
                    path.REGIONAL_DATA, "transport\\ltd\\2015 RideSource Boundary.shp"
                ),
            },
            {
                "field_name": "cats",
                "overlay_field_name": "CATSBNDY",
                "overlay_dataset_path": os.path.join(
                    path.REGIONAL_DATA, "transport\\eug\\catsbndy.shp"
                ),
            },
            {
                "field_name": "trans_analysis_zone",
                "overlay_field_name": "TAZ_NUM",
                "overlay_dataset_path": os.path.join(
                    path.REGIONAL_DATA, "transport\\MTAZ16.shp"
                ),
            },
            # Natural attributes.
            {
                "field_name": "firmnumber",
                "overlay_field_name": "firm_pan",
                "overlay_dataset_path": os.path.join(
                    path.REGIONAL_DATA, "natural\\flood\\Flood.gdb\\FIRMPanel"
                ),
            },
            {
                "field_name": "soilkey",
                "overlay_field_name": "mukey",
                "overlay_dataset_path": os.path.join(
                    path.REGIONAL_DATA, "natural\\soils\\Soils.gdb\\Soil"
                ),
            },
            {
                "field_name": "wetland",
                "overlay_field_name": "WET_TYPE",
                "overlay_dataset_path": os.path.join(
                    path.REGIONAL_DATA, "natural\\eug\\Wetland\\wetlands.shp"
                ),
            },
            # Census attributes.
            {
                "field_name": "ctract",
                "overlay_field_name": "TRACT",
                "overlay_dataset_path": os.path.join(
                    path.REGIONAL_DATA,
                    "federal\\census\\lane\\2010",
                    "lc_census2010.gdb\\lc_tracts2010",
                ),
            },
            {
                "field_name": "blockgr",
                "overlay_field_name": "BlockGroup",
                "overlay_dataset_path": os.path.join(
                    path.REGIONAL_DATA,
                    "federal\\census\\lane\\2010",
                    "lc_census2010.gdb\\lc_blockgroups2010",
                ),
            },
            # Other district attributes.
            {
                "field_name": "neighbor",
                "overlay_field_name": "NEIBORHD",
                "overlay_dataset_path": os.path.join(
                    path.REGIONAL_DATA,
                    "boundary\\districts\\eug",
                    "Boundary.gdb\\EugNeighborhoods",
                ),
            },
        ]
        for kwargs in overlay_kwargs:
            etl.transform(
                arcetl.attributes.update_by_overlay,
                overlay_central_coincident=True,
                **kwargs
            )
        # Override overlays for special cases.
        for override in OVERRIDE_ATTRS:
            for kwargs in OVERRIDE_ATTRS[override].get("overlay_kwargs", []):
                etl.transform(
                    arcetl.attributes.update_by_value,
                    dataset_where_sql=OVERRIDE_ATTRS[override].get("where_sql"),
                    **kwargs
                )
        # Clean overlay values.
        transform.clean_whitespace(
            etl, field_names=["police_beat", "wetland", "ctract", "blockgr", "neighbor"]
        )
        transform.force_uppercase(etl, field_names=["cats", "ltddist", "ltdridesrc"])
        # Set default overlay values where missing.
        transform.force_yn(
            etl,
            field_names=["greenwy", "sprsvcbndy", "cats", "ltddist", "ltdridesrc"],
            default="N",
        )
        # Remove invalid overlay values.
        transform.clear_nonpositive(etl, field_names=["ctract", "blockgr"])
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="neighbor",
            function=(lambda x: x if x and int(x) != 99 else None),
        )
        # Assign joinable field values after overlays.
        join_kwargs = [
            # Core attributes.
            {
                "field_name": "pre_direction",
                "join_field_name": "description",
                "join_dataset_path": dataset.STREET_DIRECTION.path(),
                "on_field_pairs": [("pre_direction_code", "code")],
            },
            {
                "field_name": "street_type",
                "join_field_name": "description",
                "join_dataset_path": dataset.STREET_TYPE.path(),
                "on_field_pairs": [("street_type_code", "code")],
            },
            {
                "field_name": "unit_type",
                "join_field_name": "description",
                "join_dataset_path": dataset.UNIT_TYPE.path(),
                "on_field_pairs": [("unit_type_code", "code")],
            },
            {
                "field_name": "city_name_abbr",
                "join_field_name": "CityNameAbbr",
                "join_dataset_path": dataset.CITY.path(),
                "on_field_pairs": [("city_name", "CityName")],
            },
            # Extended attributes.
            {
                "field_name": "five_digit_zip_code",
                "join_field_name": "zip_code",
                "join_dataset_path": dataset.ADDRESS_POSTAL_INFO.path(),
                "on_field_pairs": [("geofeat_id", "geofeat_id")],
            },
            # Any addresses not assigned zip from USPS gets an overlay zip.
            {
                "field_name": "five_digit_zip_code",
                "dataset_where_sql": "five_digit_zip_code is null",
                "join_field_name": "zip_code_overlay",
                "join_dataset_path": dataset.ADDRESS_POSTAL_INFO.path(),
                "on_field_pairs": [("geofeat_id", "geofeat_id")],
            },
            {
                "field_name": "four_digit_zip_code",
                "join_field_name": "plus_four_code",
                "join_dataset_path": dataset.ADDRESS_POSTAL_INFO.path(),
                "on_field_pairs": [("geofeat_id", "geofeat_id")],
            },
            {
                "field_name": "usps_delivery_point_code",
                "join_field_name": "delivery_point_code",
                "join_dataset_path": dataset.ADDRESS_POSTAL_INFO.path(),
                "on_field_pairs": [("geofeat_id", "geofeat_id")],
            },
            {
                "field_name": "postal_carrier_route",
                "join_field_name": "carrier_route",
                "join_dataset_path": dataset.ADDRESS_POSTAL_INFO.path(),
                "on_field_pairs": [("geofeat_id", "geofeat_id")],
            },
            {
                "field_name": "usps_is_cmra",
                "join_field_name": "is_cmra",
                "join_dataset_path": dataset.ADDRESS_POSTAL_INFO.path(),
                "on_field_pairs": [("geofeat_id", "geofeat_id")],
            },
            {
                "field_name": "usps_is_vacant",
                "join_field_name": "is_vacant",
                "join_dataset_path": dataset.ADDRESS_POSTAL_INFO.path(),
                "on_field_pairs": [("geofeat_id", "geofeat_id")],
            },
            {
                "field_name": "usps_has_mail_service",
                "join_field_name": "has_mail_service",
                "join_dataset_path": dataset.ADDRESS_POSTAL_INFO.path(),
                "on_field_pairs": [("geofeat_id", "geofeat_id")],
            },
            {
                "field_name": "landuse_desc",
                "join_field_name": "ludesc",
                "join_dataset_path": dataset.LAND_USE_CODES_DETAILED.path("pub"),
                "on_field_pairs": [("landuse", "landusec")],
            },
            {
                "field_name": "usecode",
                "join_field_name": "usecode",
                "join_dataset_path": dataset.LAND_USE_CODES_DETAILED.path("pub"),
                "on_field_pairs": [("landuse", "landusec")],
            },
            {
                "field_name": "usedesc",
                "join_field_name": "ucname",
                "join_dataset_path": dataset.LAND_USE_CODES_USE_CODES.path("pub"),
                "on_field_pairs": [("usecode", "usecode")],
            },
            # A&T attributes.
            {
                "field_name": "tca",
                "join_field_name": "tax_code_overlay",
                "join_dataset_path": dataset.ADDRESS_ASSESS_TAX_INFO.path(),
                "on_field_pairs": [("geofeat_id", "geofeat_id")],
            },
            # City attributes.
            {
                "field_name": "geocity_name",
                "join_field_name": "inccityname",
                "join_dataset_path": dataset.INCORPORATED_CITY_LIMITS.path(),
                "on_field_pairs": [("geocity", "inccityabbr")],
            },
            {
                "field_name": "ugb_city_name",
                "join_field_name": "ugbcityname",
                "join_dataset_path": dataset.UGB.path("pub"),
                "on_field_pairs": [("ugb", "ugbcity")],
            },
            # Planning & zoning attributes.
            {
                "field_name": "nodaldev_name",
                "join_field_name": "nodename",
                "join_dataset_path": dataset.NODAL_DEVELOPMENT_AREA.path("pub"),
                "on_field_pairs": [("nodaldev", "nodearea")],
            },
            {
                "field_name": "plandesjuris",
                "join_field_name": "planjuris",
                "join_dataset_path": dataset.PLAN_DESIGNATION.path("pub"),
                "on_field_pairs": [("plandes_id", "plandes_id")],
            },
            {
                "field_name": "plandes",
                "join_field_name": "plandes",
                "join_dataset_path": dataset.PLAN_DESIGNATION.path("pub"),
                "on_field_pairs": [("plandes_id", "plandes_id")],
            },
            {
                "field_name": "plandesdesc",
                "join_field_name": "plandesnam",
                "join_dataset_path": dataset.PLAN_DESIGNATION.path("pub"),
                "on_field_pairs": [("plandes_id", "plandes_id")],
            },
            # Public safety attributes.
            {
                "field_name": "ambulance_service_area",
                "join_field_name": "asa",
                "join_dataset_path": dataset.AMBULANCE_SERVICE_AREA.path("pub"),
                "on_field_pairs": [("ambulance_district", "asacode")],
            },
            {
                "field_name": "ambulance_service_provider",
                "join_field_name": "provider",
                "join_dataset_path": dataset.AMBULANCE_SERVICE_AREA.path("pub"),
                "on_field_pairs": [("ambulance_district", "asacode")],
            },
            {
                "field_name": "fire_protection_provider",
                "join_field_name": "fpprovname",
                "join_dataset_path": dataset.FIRE_PROTECTION_AREA.path("pub"),
                "on_field_pairs": [("firedist", "fireprotprov")],
            },
            {
                "field_name": "psap_name",
                "join_field_name": "psap_name",
                "join_dataset_path": dataset.PSAP_AREA.path("pub"),
                "on_field_pairs": [("psap_code", "psap_code")],
            },
            {
                "field_name": "emergency_service_number",
                "join_field_name": "emergency_service_number",
                "join_dataset_path": dataset.EMERGENCY_SERVICE_NUMBER.path(),
                "on_field_pairs": [
                    # City used as proxy for police.
                    ("geocity", "city_limits"),
                    ("ambulance_district", "asa_code"),
                    ("firedist", "fire_district"),
                    ("psap_code", "psap_code")
                ],
            },
            {
                "field_name": "emergency_service_number",
                "join_field_name": "emergency_service_number",
                "join_dataset_path": dataset.EMERGENCY_SERVICE_NUMBER.path(),
                "on_field_pairs": [
                    # City used as proxy for police.
                    ("geocity", "city_limits"),
                    ("ambulance_district", "asa_code"),
                    ("firedist", "fire_district"),
                ],
                "dataset_where_sql": "emergency_service_number is null",
            },
            # Election attributes.
            {
                "field_name": "city_councilor",
                "join_field_name": "councilor",
                "join_dataset_path": dataset.CITY_WARD.path(),
                "on_field_pairs": [("ccward", "ward")],
            },
            {
                "field_name": "cocommdist_name",
                "join_field_name": "cmdistname",
                "join_dataset_path": dataset.COUNTY_COMMISSIONER_DISTRICT.path("pub"),
                "on_field_pairs": [("cocommdist", "commrdist")],
            },
            {
                "field_name": "county_commissioner",
                "join_field_name": "commrname",
                "join_dataset_path": dataset.COUNTY_COMMISSIONER_DISTRICT.path("pub"),
                "on_field_pairs": [("cocommdist", "commrdist")],
            },
            {
                "field_name": "eweb_commissioner_name",
                "join_field_name": "eweb_commissioner_name",
                "join_dataset_path": dataset.EWEB_COMMISSIONER.path("pub"),
                "on_field_pairs": [("ccward", "city_council_ward")],
            },
            {
                "field_name": "state_representative",
                "join_field_name": "repname",
                "join_dataset_path": dataset.STATE_REPRESENTATIVE_DISTRICT.path("pub"),
                "on_field_pairs": [("strepdist", "repdist")],
            },
            {
                "field_name": "state_senator",
                "join_field_name": "senname",
                "join_dataset_path": dataset.STATE_SENATOR_DISTRICT.path("pub"),
                "on_field_pairs": [("senatedist", "sendist")],
            },
            # Education attributes.
            {
                "field_name": "schooldist_name",
                "join_field_name": "names",
                "join_dataset_path": dataset.SCHOOL_DISTRICT.path("pub"),
                "on_field_pairs": [("schooldist", "district")],
            },
            {
                "field_name": "elem_name",
                "join_field_name": "elem_school",
                "join_dataset_path": dataset.ELEMENTARY_SCHOOL_AREA.path("pub"),
                "on_field_pairs": [("elem", "attend")],
            },
            {
                "field_name": "middle_name",
                "join_field_name": "middle_school",
                "join_dataset_path": dataset.MIDDLE_SCHOOL_AREA.path("pub"),
                "on_field_pairs": [("middle", "attend")],
            },
            {
                "field_name": "high_name",
                "join_field_name": "high_school",
                "join_dataset_path": dataset.HIGH_SCHOOL_AREA.path("pub"),
                "on_field_pairs": [("high", "attend")],
            },
            # Natural attributes.
            {
                "field_name": "firmprinted",
                "join_field_name": "panel_printed",
                "join_dataset_path": os.path.join(
                    path.REGIONAL_DATA, "natural\\flood\\Flood.gdb\\FIRMPanel"
                ),
                "on_field_pairs": [("firmnumber", "firm_pan")],
            },
            {
                "field_name": "firm_community_id",
                "join_field_name": "com_nfo_id",
                "join_dataset_path": os.path.join(
                    path.REGIONAL_DATA, "natural\\flood\\Flood.gdb\\CommunityInfo"
                ),
                "on_field_pairs": [("geocity", "community_code")],
            },
            {
                "field_name": "firm_community_post_firm_date",
                "join_field_name": "in_frm_dat",
                "join_dataset_path": os.path.join(
                    path.REGIONAL_DATA, "natural\\flood\\Flood.gdb\\CommunityInfo"
                ),
                "on_field_pairs": [("geocity", "community_code")],
            },
            {
                "field_name": "soiltype",
                "join_field_name": "musym",
                "join_dataset_path": os.path.join(
                    path.REGIONAL_DATA, "natural\\soils\\Soils.gdb\\MUAggAtt"
                ),
                "on_field_pairs": [("soilkey", "mukey")],
            },
            # Other district attributes.
            {
                "field_name": "neighborhood_name",
                "join_field_name": "NAME",
                "join_dataset_path": os.path.join(
                    path.REGIONAL_DATA,
                    "boundary\\districts\\eug\\Boundary.gdb\\EugNeighborhoods",
                ),
                "on_field_pairs": [("neighbor", "NEIBORHD")],
            },
        ]
        for kwargs in join_kwargs:
            etl.transform(arcetl.attributes.update_by_joined_value, **kwargs)
        # Clean join values.
        transform.clean_whitespace(etl, field_names=["neighborhood_name"])
        # Remove Metro Plan designations, per City of Eugene request.
        transform.clear_all_values(
            etl,
            field_names=["plandes", "plandesdesc"],
            dataset_where_sql="plandesjuris = 'MTP'",
        )
        # Remove +4 ZIP where initial ZIP is missing.
        transform.clear_all_values(
            etl,
            field_names=["four_digit_zip_code"],
            dataset_where_sql="five_digit_zip_code is null",
        )
        # Assign constants.
        constant_kwargs = [
            {"field_name": "state_code", "value": "OR"},
            {"field_name": "state_name", "value": "Oregon"},
            {"field_name": "county_name", "value": "Lane"},
        ]
        for kwargs in constant_kwargs:
            etl.transform(arcetl.attributes.update_by_value, **kwargs)
        # Override constants for special cases.
        for override in OVERRIDE_ATTRS:
            for kwargs in OVERRIDE_ATTRS[override].get("constant_kwargs", []):
                etl.transform(
                    arcetl.attributes.update_by_value,
                    dataset_where_sql=OVERRIDE_ATTRS[override].get("where_sql"),
                    **kwargs
                )
        # Build values from functions.
        function_kwargs = [
            {
                "field_name": "street_name_full",
                "function": concatenate_arguments,
                "arg_field_names": [
                    "pre_direction_code",
                    "street_name",
                    "street_type_code",
                ],
            },
            {
                "field_name": "city_state_zip",
                "function": city_state_zip,
                "kwarg_field_names": ["city_name", "state_code", "five_digit_zip_code"],
            },
            {
                "field_name": "concat_address_no_unit",
                "function": concatenate_arguments,
                "arg_field_names": [
                    "house_nbr",
                    "house_suffix_code",
                    "street_name_full",
                ],
            },
            {
                "field_name": "concat_address",
                "function": concatenate_arguments,
                "arg_field_names": [
                    "concat_address_no_unit",
                    "unit_type_code",
                    "unit_id",
                ],
            },
            {
                "field_name": "concat_address_no_direction",
                "function": concatenate_arguments,
                "arg_field_names": [
                    "house_nbr",
                    "house_suffix_code",
                    "street_name",
                    "street_type_code",
                    "unit_type_code",
                    "unit_id",
                ],
            },
            {
                "field_name": "concat_address_full",
                "function": concat_address_full,
                "kwarg_field_names": [
                    "concat_address",
                    "city_name",
                    "state_code",
                    "five_digit_zip_code",
                    "four_digit_zip_code",
                ],
            },
            {
                "field_name": "mapnumber",
                "function": (lambda x: x[:8] if x else None),
                "arg_field_names": ["maptaxlot"],
            },
            {
                "field_name": "taxlot",
                "function": (lambda x: x[-5:] if x else None),
                "arg_field_names": ["maptaxlot"],
            },
            {
                "field_name": "maptaxlot_hyphen",
                "function": maptaxlot_separated,
                "arg_field_names": ["maptaxlot"],
            },
        ]
        for kwargs in function_kwargs:
            etl.transform(
                arcetl.attributes.update_by_function, field_as_first_arg=False, **kwargs
            )
        # Take care of addresses flagged not to update in publication.
        ids = {}
        id_set_kwargs = {
            "in_publication": {"dataset_path": dataset.SITE_ADDRESS.path("pub")},
            "in_transform": {"dataset_path": etl.transform_path},
            "no_update": {
                "dataset_path": dataset.ADDRESS_ISSUES.path(),
                "dataset_where_sql": "update_publication = 0",
            },
        }
        for key, kwargs in id_set_kwargs.items():
            ids[key] = set(
                _id
                for _id, in arcetl.attributes.as_iters(
                    field_names="site_address_gfid", **kwargs
                )
            )
        ids["rollback"] = ids["no_update"] & ids["in_transform"] & ids["in_publication"]
        ids["hold"] = ids["no_update"] & (ids["in_transform"] - ids["in_publication"])
        rollback_features = [
            feat
            for feat in arcetl.attributes.as_dicts(dataset.SITE_ADDRESS.path("pub"))
            if feat["site_address_gfid"] in ids["rollback"]
        ]
        # Strip OIDs (not part of update).
        for feat in rollback_features:
            del feat["oid@"]
        if rollback_features:
            etl.transform(
                arcetl.features.update_from_dicts,
                update_features=rollback_features,
                id_field_names="site_address_gfid",
                field_names=rollback_features[0].keys(),
                delete_missing_features=False,
            )
        etl.transform(
            arcetl.features.delete_by_id,
            delete_ids=ids["hold"],
            id_field_names="site_address_gfid",
        )
        LOG.info("%s addresses held from publication", len(ids["hold"]))
        LOG.info("%s addresses rolled-back from publication", len(ids["rollback"]))
        if any([ids["hold"], ids["rollback"]]):
            send_publication_issues_message()
        etl.load(dataset.SITE_ADDRESS.path("pub"))
    send_new_lincom_address_message()


# Jobs.


WEEKLY_JOB = Job("Address_Datasets_Weekly", etls=[site_address_etl, facility_etl])


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
