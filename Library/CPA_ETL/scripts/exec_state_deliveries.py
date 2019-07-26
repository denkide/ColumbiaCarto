"""Execution code for delivering data packages to the state."""
import argparse
import csv
from itertools import chain
import logging
import os

import pyodbc

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

EPERMITTING_JURIS_ADDRESS_WHERE_SQL = {
    "Coburg": "city_limits_abbr = 'COB'",
    "Cottage_Grove": "city_limits_abbr = 'COT'",
    "Creswell": "city_limits_abbr = 'CRE'",
    "Dunes_City": "city_limits_abbr = 'DUN'",
    "Eugene": "(ex.addr_id is null and ugb = 'EUG') or ex.administered_by = 'EUG'",
    "Florence": "city_limits_abbr = 'FLO'",
    "Junction_City": "city_limits_abbr = 'JUN'",
    "Lowell": "city_limits_abbr = 'LOW'",
    "Oakridge": "city_limits_abbr = 'OAK'",
    "Springfield": "ugb = 'SPR'",
    "Veneta": "city_limits_abbr = 'VEN'",
    "Lane_Co": """
        (ex.addr_id is null and isnull(ugb, '') not in ('EUG', 'SPR')
        and isnull(city_limits_abbr, '') in ('', 'WEF'))
        or ex.administered_by = 'LC'
    """,
}
"""dict: Mapping of jurisdiction to SQL where-clause for extracting basic addresses."""
EPERMITTING_JURIS_APO_VIEWS = {
    "Coburg": {
        "Address_Type": "dbo.vw_Cob_Address_Type",
        "Parcel_Address": "dbo.vw_Cob_Parcel_Address",
        "Parcel_Attr": "dbo.vw_Cob_Parcel_Attr",
        "Parcel_Base": "dbo.vw_Cob_Parcel_Base",
        "Parcel_Districts": "dbo.vw_Cob_Parcel_District",
        "Parcel_Owner": "dbo.vw_Cob_Parcel_Owner",
    },
    "Cottage_Grove": {
        "Address_Type": "dbo.vw_Cot_Address_Type",
        "Parcel_Address": "dbo.vw_Cot_Parcel_Address",
        "Parcel_Attr": "dbo.vw_Cot_Parcel_Attr",
        "Parcel_Base": "dbo.vw_Cot_Parcel_Base",
        "Parcel_Districts": "dbo.vw_Cot_Parcel_District",
        "Parcel_Owner": "dbo.vw_Cot_Parcel_Owner",
    },
    "Creswell": {
        "Address_Type": "dbo.vw_Cre_Address_Type",
        "Parcel_Address": "dbo.vw_Cre_Parcel_Address",
        "Parcel_Attr": "dbo.vw_Cre_Parcel_Attribute",
        "Parcel_Base": "dbo.vw_Cre_Parcel_Base",
        "Parcel_Districts": "dbo.vw_Cre_Parcel_District",
        "Parcel_Owner": "dbo.vw_Cre_Parcel_Owner",
    },
    "Florence": {
        "Address_Type": "dbo.vw_Flo_Address_Type",
        "Parcel_Address": "dbo.vw_Flo_Parcel_Address",
        "Parcel_Attr": "dbo.vw_Flo_Parcel_Attr",
        "Parcel_Base": "dbo.vw_Flo_Parcel_Base",
        "Parcel_Districts": "dbo.vw_Flo_Parcel_District",
        "Parcel_Owner": "dbo.vw_Flo_Parcel_Owner",
    },
    "Junction_City": {
        "Address_Type": "dbo.vw_Jun_Address_Type",
        "Parcel_Address": "dbo.vw_Jun_Parcel_Address",
        "Parcel_Attr": "dbo.vw_Jun_Parcel_Attr",
        "Parcel_Base": "dbo.vw_Jun_Parcel_Base",
        "Parcel_Districts": "dbo.vw_Jun_Parcel_District",
        "Parcel_Owner": "dbo.vw_Jun_Parcel_Owner",
    },
    # Springfield APO fully-generated in scheduled SSIS package.
    # Still needs to be a key here, so SFTP delivery happens.
    "Springfield": {},
    "Veneta": {
        "Address_Type": "dbo.vw_Ven_Address_Type",
        "Parcel_Address": "dbo.vw_Ven_Parcel_Address",
        "Parcel_Attr": "dbo.vw_Ven_Parcel_Attr",
        "Parcel_Base": "dbo.vw_Ven_Parcel_Base",
        "Parcel_Districts": "dbo.vw_Ven_Parcel_District",
        "Parcel_Owner": "dbo.vw_Ven_Parcel_Owner",
    },
    "Lane": {
        # "Address_Type": None,  # Lane County generates this.
        # "Parcel_Address": None,  # Lane County generates this.
        "Parcel_Attr": "dbo.vw_Lane_Co_Parcel_Attr",
        "Parcel_Base": "dbo.vw_Lane_Co_Parcel_Base",
        # "Parcel_Districts": None,  # Lane County generates this.
        "Parcel_Owner": "dbo.vw_Lane_Co_Parcel_Owner",
    },
    # Lane County contracts to do the Lake County APO.
    # As a courtesy to them, we do SFTP delivery.
    "Lake_County": {},
}
"""dict: Mapping of jurisdiction to mapping of APO file name to source view."""
EPERMITTING_JURIS_ID = {
    "Coburg": 14400,
    "Cottage_Grove": 15950,
    "Creswell": 16950,
    "Dunes_City": 21150,
    "Eugene": 23850,
    "Florence": 26050,
    "Junction_City": 38000,
    "Lowell": 44050,
    "Oakridge": 54100,
    "Springfield": 69600,
    "Veneta": 77050,
    "Lane_Co": 99039,
}
"""dict: Mapping of jurisdiction name to ID."""
ODOT_MESSAGE_KWARGS = {
    "subject": "New Road GIS Data Available from LCOG",
    "recipients": ["chad.w.brady@odot.state.or.us", "moriah.joy@odot.state.or.us"],
    "copy_recipients": [],
    "blind_copy_recipients": ["jblair@lcog.org"],
    "reply_to": "jblair@lcog.org",
    "body_pre_links": """
        <p>A new road GIS data deliverable is available for download. Download a zipped
        copy from the link below.<p>
    """,
}
"""dict: Keyword arguments for sending deliverable message to ODOT."""


# Helpers.


def create_apo_file_from_rlid_view(file_path, view_name):
    """Create dataset textfile from RLID database view.

    File is sent to state ePermitting, for Accela/buildingpermits.oregon.gov. A
    separate subdirectory with a set of APO textfiles is created for each jurisdiction.

    Args:
        file_path (str): Path for APO textfile output.
        view_name (str): Name of the view in the export database.
    """
    LOG.info("Start: Write pipe-delimited APO textfile %s.", file_path)
    conn = pyodbc.connect(database.RLID_EXPORT_ACCELA.odbc_string)
    with conn.cursor() as cursor:
        cursor.execute("select * from {}".format(view_name))
        # Write a header row with the column names.
        header = [column[0] for column in cursor.description]
        # Write dashed separator row the length of the column names.
        separator = ["".ljust(len(name), "-") for name in header]
        with open(file_path, "wb") as apofile:
            writer = csv.writer(apofile, delimiter="|")
            for row in chain([header, separator], cursor):
                writer.writerow(row)
    LOG.info("End: Write.")


def create_address_textfile(jurisdiction_name, jurisdiction_id, extract_where_sql):
    """Create address fixed-width textfile for permitting jurisdiction.

    File is sent to state ePermitting, for buildingpermits.oregon.gov.
    Notes:
        * A separate file is created for each jurisdiction.
        * Jurisdiction coverage:
            - UGB for Eugene & Springfield.
            - City limits for other cities (except Westfir).
            - All other areas (including Westfir) for Lane County.
        * There are some Lane County-owned buildings that are in City of Eugene
        jurisdiction & vice-versa. Eugene & Lane County have intergovernmental
        agreements (IGAs) that allow permitting & inspection on those buildings to
        their owner. The table GISQL113.RLID_Staging.dbo.bldg_permit_exception houses
        the addresses in question.

    Args:
        jurisdiction_name (str): Name of the jurisdiction.
        jurisdiction_id (int): ID of the jurisdiction.
        extract_where_sql (str): SQL where-clause for extracting basic addresses.

    Returns:
        str: Path of the address textfile created.
    """
    LOG.info("Start: Write fixed-width address textfile for %s.", jurisdiction_name)
    sql = """
        -- Convert all to specific-length char, making fixed-width columns.
        select
            jurisdiction_id = right(space(5) + cast({0} as nvarchar), 5),
            uniq_id = right(space(36) + cast(address_geofeature_id as nvarchar), 36),
            juris_scope = space(3),
            elecperflag = '1',
            structperflag = space(1),
            plumbperflag = '1',
            mechperflag = '1',
            number = right(space(10) + cast(house_number as nvarchar), 10),
            sub_num = right(space(3) + isnull(left(house_suffix, 3), ''), 3),
            pre_dir = right(space(2) + isnull(pre_direction_code, ''), 2),
            str_nam = right(space(30) + street_name, 30),
            str_type = right(space(4) + isnull(street_type_code, ''), 4),
            suf_dir = right(space(2) + isnull(post_direction_code, ''), 2),
            unit_type = right(space(6) + isnull(unit_type_code, ''), 6),
            unit_num = right(space(6) + isnull(unit_number, ''), 6),
            city = right(space(17) + isnull(mail_city_name, ''), 17),
            st = right(space(2) + isnull(state_code, ''), 2),
            zip5 = right(space(5) + isnull(zip_code, ''), 5),
            zip4 = right(space(4) + isnull(zip_plus4, ''), 4),
            county = '39',
            maptaxlot = right(space(24) + isnull(maplot, ''), 24),
            -- Choose most recent update (if present) for datemade.
            -- Format 101 = mm/dd/yyyy.
            datemade = case
                when date_address_updated > date_address_entered
                    then convert(nvarchar, date_address_updated, 101)
                else convert(nvarchar, date_address_entered, 101)
                end,
            business = space(60)
        from dbo.site_address as sa
            left join RLID_Staging.dbo.bldg_permit_exception as ex
                on sa.address_geofeature_id = ex.addr_id
        where {1};
    """
    textfile_path = os.path.join(
        path.REGIONAL_STAGING,
        "OregonEPermitting\\BasicAddresses",
        jurisdiction_name.replace(" ", "_").lower() + ".txt",
    )
    conn = pyodbc.connect(database.RLID.odbc_string)
    with conn.cursor() as cursor:
        cursor.execute(sql.format(jurisdiction_id, extract_where_sql))
        result = cursor.fetchall()
        with open(textfile_path, "w") as textfile:
            # Write a 'header that contains the ID, count, & datestamp.
            header = "{0!s:>5}{1!s:>5} {2:>10}\n".format(
                jurisdiction_id, len(result), datestamp()
            )
            textfile.write(header)
            for row in result:
                textfile.write("".join(row) + "\n")
    LOG.info("End: Write.")
    return textfile_path


# ETLs.


def epermitting_addresses_etl():
    """Run ETL for ePermitting basic addresses delivery datasets."""
    LOG.info("Creating ePermitting basic address textfiles.")
    for juris_name, extract_where_sql in sorted(
        EPERMITTING_JURIS_ADDRESS_WHERE_SQL.items()
    ):
        create_address_textfile(
            juris_name, EPERMITTING_JURIS_ID[juris_name], extract_where_sql
        )
    LOG.info("Sending textfiles to FTP site.")
    url.send_file_by_sftp(
        os.path.join(path.REGIONAL_STAGING, "OregonEPermitting\\BasicAddresses", "*.*"),
        url.EPERMITTING_ADDRESSES_FTP,
        **credential.EPERMITTING_ADDRESSES_FTP
    )
    LOG.info("ETL complete.")


def epermitting_apos_create_etl():
    """Run ETL for creating ePermitting APO delivery datasets."""
    for juris_name, apo_views in sorted(EPERMITTING_JURIS_APO_VIEWS.items()):
        LOG.info("Starting ETL for creating %s APO textfiles.", juris_name)
        for apo_name, view_name in apo_views.items():
            file_path = os.path.join(
                path.REGIONAL_STAGING, "Accela", juris_name, "APO", apo_name + ".txt"
            )
            create_apo_file_from_rlid_view(file_path, view_name)
        LOG.info("%s complete.", juris_name)


def epermitting_apos_send_etl():
    """Run ETL for sending ePermitting APO delivery datasets."""
    for juris_name in sorted(EPERMITTING_JURIS_APO_VIEWS):
        LOG.info("Starting ETL for sending %s APO textfiles.", juris_name)
        files_path = os.path.join(
            path.REGIONAL_STAGING, "Accela", juris_name, "APO", "*.*"
        )
        ftp_path = "{}/{}/Data/APO".format(url.EPERMITTING_ACCELA_FTP, juris_name)
        url.send_file_by_sftp(files_path, ftp_path, **credential.EPERMITTING_ACCELA_FTP)
        LOG.info("%s complete.", juris_name)


def accela_parcel_overlays_etl():
    """Run ETL for Lane Accela Parcel Overlay."""
    for parcel_overlay_dataset in dataset.ACCELA_PARCEL_OVERLAYS:
        transform.etl_dataset(
            source_path=parcel_overlay_dataset.path("maint"),
            output_path=parcel_overlay_dataset.path("pub"),
        )


def odot_delivery_etl():
    """Run ETL for ODOT delivery datasets."""
    name = "ODOT_Lane_Deliverable"
    gdb_path = os.path.join(path.REGIONAL_STAGING, "ODOT", name + ".gdb")
    transform.etl_dataset(
        source_path=os.path.join(database.RLIDGEO.path, "dbo.Road"),
        output_path=os.path.join(gdb_path, "Road"),
    )
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
    send_links_email(urls=[zip_url], **ODOT_MESSAGE_KWARGS)


# Jobs.


WEEKLY_JOB = Job(
    "State_Deliveries_Weekly",
    etls=[
        accela_parcel_overlays_etl,
        epermitting_apos_create_etl,
        epermitting_apos_send_etl,
    ],
)

MONTHLY_JOB = Job(
    "State_Deliveries_Monthly", etls=[odot_delivery_etl, epermitting_addresses_etl]
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
