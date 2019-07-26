"""Execution code for Lane County Sheriff CAD system extract."""
import argparse
import logging
import os

from etlassist.pipeline import Job, execute_pipeline

from helper.communicate import send_links_email
from helper import credential
from helper import database
from helper.misc import datestamp
from helper import path
from helper import transform
from helper import url


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""

DELIVERABLES_PATH = os.path.join(path.LCSO_EIS_CAD_PROJECT, "Deliverables")

DATASET_KWARGS = {
    ##TODO: Add `ambulance_district` to RLIDGeo.dbo.Site_Address, so we can copy this
    ##from there & drop the extract_where_sql.
    ##Also add "ambulance_district": "asa_code" to field_name_change_map.
    "AddressPts": {
        "source_path": os.path.join(database.ETL_LOAD_A.path, "dbo.SiteAddress"),
        "extract_where_sql": """
            -- Omit archived & invalid addresses.
            archived != 'Y' and valid != 'N'
            -- Omit addresses where required values are missing.
            and house_nbr > 0 and house_nbr is not null
            and street_name is not null and city_name is not null
        """,
        "field_name_change_map": {
            "concat_address": "concat_add",
            "house_suffix_code": "house_suff",
            "pre_direction_code": "pre_direct",
            "street_name": "street_nam",
            "street_type_code": "street_typ",
            "unit_type_code": "unit_type_",
            "city_name_abbr": "city_name_",
            "ambulance_district": "asa_code",
            "five_digit_zip_code": "five_digit",
            "psap_code": "psap",
        },
        "adjust_for_shapefile": True,
    },
    "Centerlines": {
        "source_path": os.path.join(database.RLIDGEO.path, "dbo.Road"),
        "adjust_for_shapefile": True,
    },
    "CityLimits": {
        "source_path": os.path.join(database.RLIDGEO.path, "dbo.IncCityLimits"),
        "field_name_change_map": {
            "inccityabbr": "inccityabb",
            "inccityname": "inccitynam",
        },
        "adjust_for_shapefile": True,
    },
}
"""dict: Mapping of dataset name to keyword arguments for ETL."""
MESSAGE_KWARGS = {
    "subject": "New EIS CAD GIS Data Available from LCOG",
    "recipients": ["jonna.hill@co.lane.or.us"],
    "copy_recipients": [],
    "blind_copy_recipients": ["jblair@lcog.org"],
    "reply_to": "jblair@lcog.org",
    "body_pre_links": """
        <p>A new EIS CAD GIS data deliverable is available for download. Download a
        zipped copy from the link below.<p>
    """,
    "body_post_links": """
        <h3>Datasets:</h3>
        <ul>
            <li>AddressPts.shp</li>
            <li>Centerlines.shp</li>
            <li>CityLimits.shp</li>
        </ul>
    """,
}
"""dict: Keyword arguments for sending message."""


# ETLs.


def lcso_cad_datasets_etl():
    """Run ETL for LSCO CAD delivery datasets."""
    for dataset_name, kwargs in DATASET_KWARGS.items():
        kwargs["output_path"] = os.path.join(DELIVERABLES_PATH, dataset_name + ".shp")
        transform.etl_dataset(**kwargs)
    zip_name = "LCSO_CAD_{}.zip".format(datestamp())
    zip_path = os.path.join(path.RLID_MAPS_WWW_SHARE, "Download", zip_name)
    conn = credential.UNCPathCredential(
        path.RLID_MAPS_WWW_SHARE, **credential.CPA_MAP_SERVER
    )
    with conn:
        path.archive_directory(
            directory_path=DELIVERABLES_PATH,
            archive_path=zip_path,
            directory_as_base=False,
            archive_exclude_patterns=[".lock", ".zip"],
        )
    zip_url = url.RLID_MAPS + "Download/" + zip_name
    send_links_email(urls=[zip_url], **MESSAGE_KWARGS)


# Jobs.


MONTHLY_JOB = Job("LCSO_CAD_Delivery", etls=[lcso_cad_datasets_etl])


# Execution.


def main():
    """Script execution code."""
    args = argparse.ArgumentParser()
    args.add_argument("pipelines", nargs="*", help="Pipeline(s) to run")
    pipelines = [globals()[arg] for arg in args.parse_args().pipelines]
    for pipeline in pipelines:
        execute_pipeline(pipeline)


if __name__ == "__main__":
    main()
