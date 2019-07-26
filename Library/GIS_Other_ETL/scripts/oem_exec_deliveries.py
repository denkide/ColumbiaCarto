"""Execution code for OEM contract delivery processing."""
import argparse
from itertools import chain
import logging
import os

import arcetl

from etlassist.pipeline import Job, execute_pipeline

from helper.communicate import send_links_email
from helper import credential
from helper import database
from helper import dataset
from helper.misc import datestamp
from helper import path
from helper import transform
from helper import url


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""

PATH = {
    "lane_deliverables": os.path.join(path.LANE_ADDRESS_POINTS_PROJECT, "Deliverables"),
    "tillamook_deliverables": os.path.join(path.TILLAMOOK_PROJECT, "Deliverables"),
    "tillamook_gis_data": os.path.join(path.TILLAMOOK_PROJECT, "GIS_Data"),
    "tillamook_odf_data": os.path.join(
        path.TILLAMOOK_PROJECT, "Rural_Roads_Grant", "ODF_Data"
    ),
}
"""dict: Mapping of identifier tag to path string."""

OEM_LANE_DATASET_KWARGS = {
    "AmbulanceServiceArea": {
        "source_path": os.path.join(database.RLIDGEO.path, "dbo.AmbulanceServiceArea")
    },
    "ESNArea": {
        "source_path": os.path.join(database.RLIDGEO.path, "dbo.EmergencyServiceZone")
    },
    "IncCityLimits": {
        "source_path": os.path.join(database.RLIDGEO.path, "dbo.IncCityLimits")
    },
    "Facilities": {
        "source_path": os.path.join(
            path.REGIONAL_FILE_SHARE, "data", "base", "Facilities.gdb", "Facilities"
        )
    },
    "FireProtectionArea": {
        "source_path": os.path.join(database.RLIDGEO.path, "dbo.FireProtectionArea")
    },
    "Road": {"source_path": os.path.join(database.RLIDGEO.path, "dbo.Road")},
    "SiteAddress": {
        "source_path": os.path.join(database.RLIDGEO.path, "dbo.Site_Address")
    },
    "Taxlot": {"source_path": os.path.join(database.RLIDGEO.path, "dbo.Taxlot")},
}
"""dict: Mapping of dataset name to keyword arguments."""
OEM_TILLAMOOK_DATASET_KWARGS = {
    "Address_Point": {
        "source_path": dataset.TILLAMOOK_ADDRESS_POINT.path("pub"),
        "extract_where_sql": "archived = 'N' and valid = 'Y'",
    },
    "City_Limits": {"source_path": dataset.TILLAMOOK_CITY_LIMITS.path()},
    "Emergency_Service_Zone": {
        "source_path": dataset.TILLAMOOK_EMERGENCY_SERVICE_ZONE.path()
    },
    "EMS": {"source_path": dataset.TILLAMOOK_EMS.path()},
    "Fire": {"source_path": dataset.TILLAMOOK_FIRE.path()},
    "Police": {"source_path": dataset.TILLAMOOK_POLICE.path()},
    "Road_Centerline": {"source_path": dataset.TILLAMOOK_ROAD_CENTERLINE.path("pub")},
}
"""dict: Mapping of dataset name to keyword arguments."""
TILLAMOOK_DATASET_KWARGS = {
    "Address_Point": {
        "source_path": dataset.TILLAMOOK_ADDRESS_POINT.path("pub"),
        "extract_where_sql": "archived = 'N' and valid = 'Y'",
    },
    "Alternate_Street_Name": {
        "source_path": dataset.TILLAMOOK_ALTERNATE_STREET_NAME.path(),
        "field_name_change_map": {
            "alt_predir": "predir",
            "alt_pretype": "pretype",
            "alt_name": "name",
            "alt_type": "type",
            "alt_sufdir": "sufdir",
        },
    },
    "City_Limits": {"source_path": dataset.TILLAMOOK_CITY_LIMITS.path()},
    "Emergency_Service_Zone": {
        "source_path": dataset.TILLAMOOK_EMERGENCY_SERVICE_ZONE.path()
    },
    "EMS": {"source_path": dataset.TILLAMOOK_EMS.path()},
    "EMS_ARA": {"source_path": dataset.TILLAMOOK_EMS_ARA.path()},
    "Fire": {"source_path": dataset.TILLAMOOK_FIRE.path()},
    "Fire_ARA": {"source_path": dataset.TILLAMOOK_FIRE_ARA.path()},
    "Police": {"source_path": dataset.TILLAMOOK_POLICE.path()},
    "Police_ARA": {"source_path": dataset.TILLAMOOK_POLICE_ARA.path()},
    "Road_Centerline": {"source_path": dataset.TILLAMOOK_ROAD_CENTERLINE.path("pub")},
}
"""dict: Mapping of dataset name to keyword arguments."""
TILLAMOOK_911_DATASET_KWARGS = {
    "ADDRESS": {
        "source_path": dataset.TILLAMOOK_ADDRESS_POINT.path("pub"),
        "extract_where_sql": "archived = 'N'",
        "field_name_change_map": {
            "stnum": "STRUCTNO",
            "stnumsuf": "STRUCTNOSU",
            "name": "STREETNAME",
            "type": "STREETTYPE",
            "postcomm": "COMMUNITY",
            "county": "COUNTY_TAG",
            "lon": "LONGITUDE",
            "lat": "LATITUDE",
        },
    },
    "CITY": {"source_path": dataset.TILLAMOOK_CITY_LIMITS.path()},
    "EMS": {"source_path": dataset.TILLAMOOK_EMS.path()},
    "EMS_ARA": {"source_path": dataset.TILLAMOOK_EMS_ARA.path()},
    "ESN": {"source_path": dataset.TILLAMOOK_EMERGENCY_SERVICE_ZONE.path()},
    "FIRE": {"source_path": dataset.TILLAMOOK_FIRE.path()},
    "FIRE_ARA": {"source_path": dataset.TILLAMOOK_FIRE_ARA.path()},
    "POLICE": {"source_path": dataset.TILLAMOOK_POLICE.path()},
    "POLICE_ARA": {"source_path": dataset.TILLAMOOK_POLICE_ARA.path()},
    "STREETS": {
        "source_path": dataset.TILLAMOOK_ROAD_CENTERLINE.path("pub"),
        "field_name_change_map": {
            "segid": "LOCAL_ID",
            "full_name": "STREET",
            "name": "STREETNAME",
            "type": "STREETTYPE",
            "postcomm_L": "LCITY",
            "postcomm_R": "RCITY",
            "zip_L": "LZIP",
            "zip_R": "RZIP",
            "county_L": "COUNTY_TAG",
            "init_date": "CREATE_DT",
            "mod_date": "UPDATE_DT",
            "esn_L": "LESN",
            "esn_R": "RESN",
        },
    },
}
"""dict: Mapping of dataset name to keyword arguments."""
TILLAMOOK_GIS_DATASET_KWARGS = {
    "GIS_Data\\Beach_Access": {
        "source_path": os.path.join(
            PATH["tillamook_gis_data"],
            "Oregon_Parks",
            "OPRD_TillamookCo_ParkData.gdb",
            "TillamookBeachAccess_locations",
        )
    },
    "GIS_Data\\Beach_Access_Signs": {
        "source_path": os.path.join(
            PATH["tillamook_gis_data"],
            "Oregon_Parks",
            "EMERGENCY BEACH ACCESS SIGNS_Tillamook.shp",
        )
    },
    "GIS_Data\\Building_Footprints": {
        "source_path": os.path.join(
            PATH["tillamook_gis_data"], "DOGAMI", "tillamook_buildings.shp"
        )
    },
    "GIS_Data\\Camp_Sites": {
        "source_path": os.path.join(
            PATH["tillamook_gis_data"],
            "Oregon_Parks",
            "OPRD_TillamookCo_ParkData.gdb",
            "CampingSpots",
        )
    },
    "GIS_Data\\Driveways": {
        "source_path": os.path.join(
            PATH["tillamook_gis_data"], "Tillamook_Working.gdb", "ODF_Driveways_Snapped"
        )
    },
    "GIS_Data\\Railroads": {
        "source_path": os.path.join(
            PATH["tillamook_gis_data"], "Tillamook.gdb", "Railroads"
        )
    },
    "GIS_Data\\Recreation_Facilities": {
        "source_path": os.path.join(
            PATH["tillamook_odf_data"],
            "RecreationForPSAP_20160127.gdb",
            "Recreation_Facilities",
        )
    },
    "GIS_Data\\Trail_Features": {
        "source_path": os.path.join(
            PATH["tillamook_odf_data"],
            "RecreationForPSAP_20160127.gdb",
            "Recreation_TrailFeatures",
        )
    },
    "GIS_Data\\Trails": {
        "source_path": os.path.join(
            PATH["tillamook_odf_data"],
            "RecreationForPSAP_20160127.gdb",
            "Recreation_Trails",
        )
    },
    "GIS_Data\\Water_Access": {
        "source_path": os.path.join(
            PATH["tillamook_gis_data"], "Tillamook.gdb", "Water_Access"
        )
    },
}
"""dict: Mapping of dataset name to keyword arguments."""

OEM_LANE_MESSAGE_KWARGS = {
    "subject": "To Do: Upload OEM Data Delivery",
    "recipients": ["jblair@lcog.org"],
    "copy_recipients": [],
    "blind_copy_recipients": [],
    "reply_to": "jblair@lcog.org",
    "body_pre_links": None,
    "body_post_links": """
        <ol>
            <li>Navigate to <a href="{url}">{url}</a>.</li>
            <li>Enter LOGIN={username}, PASSWORD={password},
                then click on arrow below.</li>
            <li>Scroll down folder list to "LCOG".</li>
            <li>Use "Upload" button in top-right to transfer delivery,
                or drag-and-drop.</li>
        </ol>
    """.format(url=url.OEM_FILE_SHARING, **credential.OEM_FILE_SHARING),
}
"""dict: Keyword arguments for message delivery."""
OEM_TILLAMOOK_MESSAGE_KWARGS = {
    "subject": "To Do: Upload OEM Data Delivery",
    "recipients": ["jblair@lcog.org"],
    "copy_recipients": [],
    "blind_copy_recipients": [],
    "reply_to": "jblair@lcog.org",
    "body_pre_links": None,
    "body_post_links": """
        <ol>
            <li>Navigate to <a href="{url}">{url}</a>.</li>
            <li>Enter LOGIN={username}, PASSWORD={password},
                then click on arrow below.</li>
            <li>Scroll down folder list to "Tillamook".</li>
            <li>Use "Upload" button in top-right to transfer delivery,
                or drag-and-drop.</li>
        </ol>
    """.format(url=url.OEM_FILE_SHARING, **credential.OEM_FILE_SHARING),
}
"""dict: Keyword arguments for message delivery."""
TILLAMOOK_911_MESSAGE_KWARGS = {
    "subject": "From LCOG: 911 GIS Data - Monthly Delivery",
    "recipients": [
        # Tiffany Miller: Operations Manager @ Tillamook ECD.
        "tmiller@tillamook911.com",
        # Doug Kettner: Administrator @ Tillamook ECD.
        "dkettner@tillamook911.com",
    ],
    "copy_recipients": [
        # Keith Massie: Maintainer by contract for Tillamook MSAG @ LCOG.
        "kmassie@lcog.org"
    ],
    "blind_copy_recipients": ["jblair@lcog.org"],
    "reply_to": "jblair@lcog.org",
    "body_pre_links": "<p>A new 911 GIS geodatabase is now available for download.<p>",
    # Will be added in-function.
    "body_post_links": None,
}
"""dict: Keyword arguments for message delivery."""
TILLAMOOK_MESSAGE_KWARGS = {
    "subject": "From LCOG: Tillamook GIS Data - Monthly Delivery",
    "recipients": [
        # Wendy Schink: GIS analyst @ Tillamook County.
        "wschink@co.tillamook.or.us",
        # Chad Brady: Manages All Roads project @ ODOT.
        "chad.w.brady@odot.state.or.us",
        # Moriah Joy: GIS Analyst @ ODOT?
        "moriah.joy@odot.state.or.us",
        # Hilary Leavell: MSAG Coordinator @ City of Salem.
        "hleavell@cityofsalem.net"
        # Jason Space - maintains data for Clatsop PSAP.
        "jspace@geo-comm.com",
        "alejandro.vizcarra@here.com",
    ],
    "copy_recipients": [
        # Keith Massie: Maintainer by contract for Tillamook MSAG @ LCOG.
        "kmassie@lcog.org"
    ],
    "blind_copy_recipients": ["jblair@lcog.org"],
    "reply_to": "jblair@lcog.org",
    "body_pre_links": "<p>A new GIS data deliverable is now available for download.<p>",
    # Will be added in-function.
    "body_post_links": None,
}
"""dict: Keyword arguments for message delivery."""


# Helpers.


##TODO: Jinja template?
def send_message_tillamook(deliverable_url, metadata_where_sql, **kwargs):
    """Send message of available deliverable download for Tillamook.

    Args:
        deliverable_url (str): URL for deliverable.
        metadata_where_sql (str): SQL where-clause for getting info from metadata
            table.
        **kwargs (dict): Keyword arguments. See below.

    Keyword Args:
        See etlassist.communicate.send_links_email for details.
    """
    # Get dataset change dates for message body.
    dataset_last_change = arcetl.attributes.as_iters(
        dataset.TILLAMOOK_METADATA_DELIVERABLES.path(),
        field_names=["dataset_name", "last_change_date"],
        dataset_where_sql=metadata_where_sql,
    )
    html_table_rows = ["<tr><th>Dataset Name</th><th>Last Change Date</th></tr>"]
    for dataset_name, last_change_date in sorted(dataset_last_change):
        html_table_rows.append(
            "<tr><td>{}</td><td>{}</td></tr>".format(
                dataset_name, last_change_date.strftime("%Y-%m-%d")
            )
        )
    # Insert table rows into body.
    kwargs[
        "body_post_links"
    ] = """
        <h3>Datasets (ordered by most recent change date)</h3>
        <table style="width:100%">{}</table>
     """.format(
        "".join(html_table_rows)
    )
    send_links_email(urls=[deliverable_url], **kwargs)


# ETLs.


def oem_lane_delivery_etl():
    """Run ETL for OEM-Lane delivery."""
    name = "OEM_Lane"
    gdb_path = os.path.join(PATH["lane_deliverables"], name + ".gdb")
    for dataset_name, kwargs in OEM_LANE_DATASET_KWARGS.items():
        kwargs["output_path"] = os.path.join(gdb_path, dataset_name)
        transform.etl_dataset(**kwargs)
    zip_name = "{}_{}.zip".format(name, datestamp())
    zip_path = os.path.join(PATH["lane_deliverables"], zip_name)
    path.archive_directory(
        directory_path=gdb_path,
        archive_path=zip_path,
        directory_as_base=True,
        archive_exclude_patterns=[".lock"],
    )
    send_links_email(urls=[zip_path], **OEM_LANE_MESSAGE_KWARGS)


def oem_tillamook_delivery_etl():
    """Run ETL for OEM-Tillamook delivery."""
    name = "OEM_Tillamook"
    gdb_path = os.path.join(PATH["tillamook_deliverables"], name + ".gdb")
    for dataset_name, kwargs in OEM_TILLAMOOK_DATASET_KWARGS.items():
        kwargs["output_path"] = os.path.join(gdb_path, dataset_name)
        transform.etl_dataset(**kwargs)
    zip_name = "{}_{}.zip".format(name, datestamp())
    zip_path = os.path.join(PATH["tillamook_deliverables"], zip_name)
    path.archive_directory(
        directory_path=gdb_path,
        archive_path=zip_path,
        directory_as_base=True,
        archive_exclude_patterns=[".lock"],
    )
    send_links_email(urls=[zip_path], **OEM_TILLAMOOK_MESSAGE_KWARGS)


def tillamook_911_delivery_etl():
    """Run ETL for Tillamook 911 CAD delivery."""
    name = "Tillamook_911"
    gdb_path = os.path.join(PATH["tillamook_deliverables"], name + ".gdb")
    for dataset_name, kwargs in chain(
        TILLAMOOK_911_DATASET_KWARGS.items(), TILLAMOOK_GIS_DATASET_KWARGS.items()
    ):
        kwargs["output_path"] = os.path.join(gdb_path, dataset_name)
        transform.etl_dataset(**kwargs)
    zip_name = "{}_{}.zip".format(name, datestamp())
    zip_path = os.path.join(path.RLID_MAPS_WWW_SHARE, "Download", zip_name)
    conn = credential.UNCPathCredential(
        path.RLID_MAPS_WWW_SHARE, **credential.CPA_MAP_SERVER
    )
    with conn:
        path.archive_directory(
            directory_path=gdb_path,
            archive_path=zip_path,
            directory_as_base=True,
            archive_exclude_patterns=[".lock"],
        )
    zip_url = url.RLID_MAPS + "Download/" + zip_name
    send_message_tillamook(
        zip_url,
        metadata_where_sql="in_tillamook_911 = 1",
        **TILLAMOOK_911_MESSAGE_KWARGS
    )


def tillamook_delivery_etl():
    """Run ETL for Tillamook delivery."""
    name = "Tillamook"
    gdb_path = os.path.join(PATH["tillamook_deliverables"], name + ".gdb")
    for dataset_name, kwargs in chain(
        TILLAMOOK_DATASET_KWARGS.items(), TILLAMOOK_GIS_DATASET_KWARGS.items()
    ):
        kwargs["output_path"] = os.path.join(gdb_path, dataset_name)
        transform.etl_dataset(**kwargs)
    zip_name = "{}_{}.zip".format(name, datestamp())
    zip_path = os.path.join(path.RLID_MAPS_WWW_SHARE, "Download", zip_name)
    conn = credential.UNCPathCredential(
        path.RLID_MAPS_WWW_SHARE, **credential.CPA_MAP_SERVER
    )
    with conn:
        path.archive_directory(
            directory_path=gdb_path,
            archive_path=zip_path,
            directory_as_base=True,
            archive_exclude_patterns=[".lock"],
        )
    zip_url = url.RLID_MAPS + "Download/" + zip_name
    send_message_tillamook(
        zip_url, metadata_where_sql="in_tillamook = 1", **TILLAMOOK_MESSAGE_KWARGS
    )


# Jobs.


MONTHLY_JOB = Job(
    "OEM_Deliveries_Monthly",
    etls=[
        tillamook_911_delivery_etl,
        tillamook_delivery_etl,
        oem_lane_delivery_etl,
        oem_tillamook_delivery_etl,
    ],
)


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
