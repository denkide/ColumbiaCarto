"""Execution code for OEM contract Tillamook dataset processing."""
import argparse
import datetime
import logging
import uuid

import sys
sys.path.insert(0, 'C:\\ColumbiaCarto\\work\\TillamookPy\\Tillamook_data')
sys.path.insert(0, 'C:\\ColumbiaCarto\\work\\TillamookPy\\Tillamook_data\\ArcETL')
sys.path.insert(0, 'C:\\ColumbiaCarto\\work\\TillamookPy\\Tillamook_data\\ETLAssist')
sys.path.insert(0, 'C:\ColumbiaCarto\work\TillamookPy\Tillamook_data\GIS_Other_ETL\scripts\helper')

import arcetl
import arcpy
from etlassist.pipeline import Job, execute_pipeline

from helper.communicate import send_email
from helper import database
from helper import dataset
#from helper.misc import EPSG, TOLERANCE, parity
from helper import transform
from helper.value import concatenate_arguments


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""

CCLASS_ST_CLASS = {
    100: "HWY",
    200: "HWY",
    121: "RMP",
    221: "RMP",
    300: "ART",
    350: "ART",
    # 400: "COL",
    # 450: "COL",
    # 500: "LOC",
    # 600: "ALY",
    700: "ACC",
    900: "RES",
    950: "RES",
}
"""dict: Mapping of cartographic class `cclass` to corresponding `st_class`."""
KWARGS_ISSUES_MESSAGE = {
    "subject": "Tillamook Address Point Publication Issues",
    "recipients": ["kmassie@lcog.org"],
    "blind_copy_recipients": ["jblair@lcog.org"],
    "reply_to": "jblair@lcog.org",
    "body": """
        <p>
            Validation issues were found that prevent publishing.
            <em>
                Refer to feature class Address_Point_Issues for all validation issues.
            </em>
        </p>
        <table style="width:100%">
            {}{}
        </table>
    """,
    "body_format": "HTML",
}
"""dict: Mapping of messaging function keyword to its value."""
MSAG_ADDRESS_KEYS = {"street": ["predir", "name", "type", "sufdir", "postcomm"]}
MSAG_ADDRESS_KEYS["range"] = MSAG_ADDRESS_KEYS["street"] + ["esn"]
MSAG_ADDRESS_KEYS["sort_order"] = MSAG_ADDRESS_KEYS["street"] + ["stnum", "esn"]
MSAG_ADDRESS_KEYS["address"] = MSAG_ADDRESS_KEYS["sort_order"] + ["shape@"]
"""dict: Mapping of MSAG address attribute types to list of attribute keys."""
MSAG_GEOMETRY_STYLE = "convex-hull"
"""str: Style of geometry for the MSAG output.

Valid styles: "convex-hull", "hull-rectangle", "multipoint".
"""
MSAG_KEYS = {
    "core": [
        "emergency_service_number",
        "parity_code",
        "parity",
        "from_structure_number",
        "to_structure_number",
        "prefix_direction_code",
        "street_name",
        "street_type_code",
        "suffix_direction_code",
        "postal_community",
    ]
}
"""dict: Mapping of MSAG type flag to list of attribute keys."""
MSAG_KEYS["master"] = ["msag_id"] + MSAG_KEYS["core"] + ["effective_date", "shape@"]
TILLAMOOK_SOURCES_MAP = {
    "Address": [dataset.TILLAMOOK_ADDRESS_POINT.path("pub")],
    "Address_Point": [dataset.TILLAMOOK_ADDRESS_POINT.path("pub")],
    "Alternate_Name": [dataset.TILLAMOOK_ALTERNATE_STREET_NAME.path()],
    "City": [dataset.TILLAMOOK_CITY_LIMITS.path()],
    "EMS": [dataset.TILLAMOOK_EMS.path()],
    "EMS_ARA": [dataset.TILLAMOOK_EMS_ARA.path()],
    "ESN": [
        dataset.TILLAMOOK_EMERGENCY_SERVICE_NUMBER.path(),
        dataset.TILLAMOOK_EMS.path(),
        dataset.TILLAMOOK_FIRE.path(),
        dataset.TILLAMOOK_POLICE.path(),
    ],
    "Fire": [dataset.TILLAMOOK_FIRE.path()],
    "Fire_ARA": [dataset.TILLAMOOK_FIRE_ARA.path()],
    "Police": [dataset.TILLAMOOK_POLICE.path()],
    "Police_ARA": [dataset.TILLAMOOK_POLICE_ARA.path()],
    "Road_Centerline": [dataset.TILLAMOOK_ROAD_CENTERLINE.path("pub")],
    "Streets": [dataset.TILLAMOOK_ROAD_CENTERLINE.path("pub")],
}
"""dict: Mapping of dataset name to source path."""


# Helpers.


def address_matches_range(address, msag_range):
    """Return True if address matches to city-street-ESN range.

    Args:
        address (dict): Mapping of address attribute name to value.
        msag_range (dict): Mapping of range attribute name to value.

    Returns:
        bool: True if address matches the range for city, street, & ESN.
    """
    matches = all(address[key] == msag_range[key] for key in MSAG_ADDRESS_KEYS["range"])
    return matches


def init_range(init_address):
    """Initialize info for MSAG range dictionary.

    Args:
        init_address (dict): Mapping of address attribute name to value.

    Returns:
        dict: Mapping of range attribute name to value.
    """
    new_range = init_address.copy()
    new_range["structure_numbers"] = [new_range.pop("stnum")]
    new_range["points"] = [new_range.pop("shape@").firstPoint]
    return new_range


def finish_range(msag_range):
    """Finish attributes in MSAG range dictionary.

    Args:
        msag_range (dict): Mapping of range attribute name to value.

    Returns:
        dict: Mapping of range attribute name to value.
    """
    new_range = msag_range.copy()
    # Walk names from address to MSAG range style.
    change_key = {
        "esn": "emergency_service_number",
        "predir": "prefix_direction_code",
        "name": "street_name",
        "type": "street_type_code",
        "sufdir": "suffix_direction_code",
        "postcomm": "postal_community",
    }
    sref = arcpy.SpatialReference(EPSG)
    for old_key, new_key in change_key.items():
        new_range[new_key] = new_range.pop(old_key)
    new_range["parity"] = parity(new_range["structure_numbers"])
    new_range["parity_code"] = new_range["parity"][0].upper()
    new_range["from_structure_number"] = min(new_range["structure_numbers"])
    new_range["to_structure_number"] = max(new_range["structure_numbers"])
    if MSAG_GEOMETRY_STYLE == "convex-hull":
        new_range["shape@"] = (
            arcpy.Multipoint(arcpy.Array(new_range["points"]), sref)
            .convexHull()
            .buffer(50)
        )
    elif MSAG_GEOMETRY_STYLE == "hull-rectangle":
        LOG.warning(
            "Hull rectangle style currently cannot create polygons for 2-point ranges."
        )
        if len(new_range["points"]) == 1:
            centroid = new_range["points"][0]
            new_range["points"] = [
                arcpy.Point(centroid.X - 50, centroid.Y + 50),
                arcpy.Point(centroid.X + 50, centroid.Y + 50),
                arcpy.Point(centroid.X + 50, centroid.Y - 50),
                arcpy.Point(centroid.X - 50, centroid.Y - 50),
            ]
            new_range["shape@"] = arcpy.Polygon(arcpy.Array(new_range["points"]), sref)
        else:
            new_range["shape@"] = arcpy.Multipoint(arcpy.Array(new_range["points"]))
            nums = [float(coord) for coord in new_range["shape@"].hullRectangle.split()]
            new_range["points"] = [
                arcpy.Point(*nums[i : i + 2]) for i in range(0, len(nums), 2)
            ]
            new_range["shape@"] = arcpy.Polygon(
                arcpy.Array(new_range["points"]), sref
            ).buffer(50)
    elif MSAG_GEOMETRY_STYLE == "multipoint":
        new_range["shape@"] = arcpy.Multipoint(arcpy.Array(new_range["points"]))
    return new_range


def msag_ranges():
    """Generate Master Street Address Guide (MSAG) reference ranges.

    The order of the cursor rows is critically important! The rows must be order-
    grouped by a full street name + city combo, then ascending order of the house
    numbers. It is best to then sort by ESN, so that if an address straddles an ESZ
    boundary (e.g. multiple units), it will assign the same every time.

    Yields:
        dict: Mapping of range attribute name to value.
    """
    msag_address_sql = " and ".join(
        [
            # Omit archived addresses (but allow not-valid).
            "archived != 'Y'",
            # Omit addresses where required values are missing.
            "stnum > 0",
            "stnum is not null",
            "name is not null",
            "postcomm is not null",
            "shape is not null",
            "shape.STIsEmpty() != 1",
            # Tillamook addresses have mileposts in them. Omit for MSAG.
            "name not like 'MP %'",
        ]
    )
    # Need to extract as tuples before making dict to use the tuple-sorting.
    address_rows = sorted(
        arcetl.attributes.as_iters(
            dataset.TILLAMOOK_ADDRESS_POINT.path("pub"),
            field_names=MSAG_ADDRESS_KEYS["address"],
            dataset_where_sql=msag_address_sql,
        )
    )
    for i, row in enumerate(address_rows):
        address = dict(zip((MSAG_ADDRESS_KEYS["address"]), row))
        # First row is a special case, do not need city-street comparison.
        if i == 0:
            msag_range = init_range(address)
        # Row still on same city-street combo *and* same ESN.
        elif address_matches_range(address, msag_range):
            msag_range["structure_numbers"].append(address["stnum"])
            msag_range["points"].append(address["shape@"].firstPoint)
        # Row changed street, city, or ESN.
        else:
            # Finish & yield range info before starting anew.
            yield finish_range(msag_range)

            # Reset current attributes with current address.
            msag_range = init_range(address)
    # Outside loop, finish & append last range.
    yield finish_range(msag_range)


# ETLs.


def address_point_etl():
    """Run ETL for address points."""
    with arcetl.ArcETL("Address Points") as etl:
        etl.extract(dataset.TILLAMOOK_ADDRESS_POINT.path("maint"))
        # Remove addresses flagged in validationas "not OK to publish".
        etl.transform(
            arcetl.dataset.join_field,
            join_dataset_path=dataset.TILLAMOOK_ADDRESS_POINT_ISSUES.path(),
            join_field_name="ok_to_publish",
            on_field_name="address_id",
            on_join_field_name="address_id",
        )
        etl.transform(arcetl.features.delete, dataset_where_sql="ok_to_publish = 0")
        etl.transform(arcetl.dataset.delete_field, field_name="ok_to_publish")
        # Clean maintenance values.
        transform.clear_nonpositive(etl, field_names=["stnum"])
        transform.clean_whitespace(
            etl,
            field_names=[
                "stnumsuf",
                "predir",
                "name",
                "type",
                "sufdir",
                "unit_type",
                "unit",
                "postcomm",
                "zip",
                "county",
            ],
        )
        transform.force_uppercase(
            etl,
            field_names=[
                "stnumsuf",
                "predir",
                "name",
                "type",
                "unit_type",
                "unit",
                "postcomm",
                "county",
                "valid",
                "archived",
                "confidence",
            ],
        )
        transform.clear_non_numeric_text(etl, field_names=["zip"])
        transform.force_yn(etl, field_names=["archived"], default="N")
        transform.force_yn(etl, field_names=["valid"], default="Y")
        transform.add_missing_fields(etl, dataset.TILLAMOOK_ADDRESS_POINT, tags=["pub"])
        # Assign geometry attributes.
        for x_name, y_name, srid in [("lon", "lat", 4326)]:
            for name, axis in [(x_name, "x"), (y_name, "y")]:
                etl.transform(
                    arcetl.attributes.update_by_geometry,
                    field_name=name,
                    spatial_reference_item=srid,
                    geometry_properties=["centroid", axis],
                )
        # Assign joined values.
        etl.transform(
            arcetl.attributes.update_by_joined_value,
            field_name="join_id",
            join_dataset_path=dataset.TILLAMOOK_ALTERNATE_STREET_NAME.path(),
            join_field_name="join_id",
            on_field_pairs=[
                ("predir", "prime_predir"),
                ("name", "prime_name"),
                ("type", "prime_type"),
                ("sufdir", "prime_sufdir"),
            ],
        )
        # Assign overlays.
        overlay_kwargs = [
            {
                "field_name": "city_limit",
                "overlay_field_name": "city",
                "overlay_dataset_path": dataset.TILLAMOOK_CITY_LIMITS.path(),
            },
            {
                "field_name": "ems",
                "overlay_field_name": "district",
                "overlay_dataset_path": dataset.TILLAMOOK_EMS.path(),
            },
            {
                "field_name": "esn",
                "overlay_field_name": "esn",
                "overlay_dataset_path": dataset.TILLAMOOK_EMERGENCY_SERVICE_ZONE.path(),
            },
            {
                "field_name": "fire",
                "overlay_field_name": "district",
                "overlay_dataset_path": dataset.TILLAMOOK_FIRE.path(),
            },
            {
                "field_name": "police",
                "overlay_field_name": "district",
                "overlay_dataset_path": dataset.TILLAMOOK_POLICE.path(),
            },
        ]
        for kwargs in overlay_kwargs:
            etl.transform(
                arcetl.attributes.update_by_overlay,
                overlay_central_coincident=True,
                **kwargs
            )
        # Build values: Constants.
        value_kwargs = [{"field_name": "state", "value": "OR"}]
        transform.update_attributes_by_values(etl, value_kwargs)
        # Build values: Concatenations.
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="address",
            function=concatenate_arguments,
            field_as_first_arg=False,
            arg_field_names=[
                "stnum",
                "stnumsuf",
                "predir",
                "name",
                "type",
                "sufdir",
                "unit_type",
                "unit",
            ],
        )
        etl.load(dataset.TILLAMOOK_ADDRESS_POINT.path("pub"))


def emergency_service_zone_etl():
    """Run ETL for emergency service zones."""
    with arcetl.ArcETL("Emergency Service Zones") as etl:
        etl.extract(dataset.TILLAMOOK_EMS.path())
        transform.add_missing_fields(etl, dataset.TILLAMOOK_EMERGENCY_SERVICE_ZONE)
        identity_kwargs = [
            {
                "field_name": "ems",
                "identity_field_name": "district",
                "identity_dataset_path": dataset.TILLAMOOK_EMS.path(),
            },
            {
                "field_name": "fire",
                "identity_field_name": "district",
                "identity_dataset_path": dataset.TILLAMOOK_FIRE.path(),
            },
            {
                "field_name": "police",
                "identity_field_name": "district",
                "identity_dataset_path": dataset.TILLAMOOK_POLICE.path(),
            },
        ]
        for kwargs in identity_kwargs:
            etl.transform(arcetl.geoset.identity, tolerance=2, **kwargs)
        # Drop where feature lacks city, fire, & ambulance.
        etl.transform(
            arcetl.features.delete,
            dataset_where_sql=" and ".join(
                ["ems is null", "fire is null", "police is null"]
            ),
        )
        join_kwargs = [
            {
                "field_name": "esn",
                "join_field_name": "esn",
                "join_dataset_path": dataset.TILLAMOOK_EMERGENCY_SERVICE_NUMBER.path(),
                "on_field_pairs": [
                    ("police", "police"),
                    ("fire", "fire"),
                    ("ems", "ems"),
                ],
            }
        ]
        for kwargs in join_kwargs:
            etl.transform(arcetl.attributes.update_by_joined_value, **kwargs)
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=[
                field["name"]
                for field in dataset.TILLAMOOK_EMERGENCY_SERVICE_NUMBER.fields
            ],
            tolerance=TOLERANCE,
        )
        etl.load(dataset.TILLAMOOK_EMERGENCY_SERVICE_ZONE.path())


def msag_ranges_current_etl():
    """Run ETL for current model of Master Street Address Guide (MSAG)."""
    with arcetl.ArcETL("MSAG Ranges - Current") as etl:
        etl.init_schema(dataset.TILLAMOOK_MSAG_RANGE.path("current"))
        etl.transform(
            arcetl.features.insert_from_dicts,
            insert_features=msag_ranges,
            field_names=MSAG_KEYS["core"] + ["shape@"],
        )
        old_msag_id = {
            row[:-1]: row[-1]
            for row in arcetl.attributes.as_iters(
                dataset.TILLAMOOK_MSAG_RANGE.path("master"),
                field_names=MSAG_KEYS["core"] + ["msag_id"],
                dataset_where_sql="expiration_date is null",
            )
        }

        def _assign_msag_id(*core_values):
            """Assign MSAG ID for the given range core attributes.

            Args:
                core_values: Tuple of MSAG range core values.

            Returns:
                str: Esri/MS-style UUID for MSAG range.
            """
            if core_values in old_msag_id:
                _id = old_msag_id[core_values]
            else:
                _id = "{" + str(uuid.uuid4()) + "}"
            return _id

        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="msag_id",
            function=_assign_msag_id,
            arg_field_names=MSAG_KEYS["core"],
            field_as_first_arg=False,
        )
        # Get effective date for ranges already in master.
        etl.transform(
            arcetl.attributes.update_by_joined_value,
            field_name="effective_date",
            join_field_name="effective_date",
            join_dataset_path=dataset.TILLAMOOK_MSAG_RANGE.path("master"),
            on_field_pairs=[("msag_id", "msag_id")],
        )
        # Assign effective date for new ranges.
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="effective_date",
            function=datetime.date.today,
            field_as_first_arg=False,
            dataset_where_sql="effective_date is null",
        )
        etl.load(dataset.TILLAMOOK_MSAG_RANGE.path("current"))


def msag_update():
    """Run toggle for the Master Street Address Guide."""
    unretired_where_sql = "expiration_date is null"
    # Do not update MSAG if there will be a significant change.
    count = {
        "current": arcetl.features.count(dataset.TILLAMOOK_MSAG_RANGE.path("current")),
        "previous": arcetl.features.count(
            dataset.TILLAMOOK_MSAG_RANGE.path("master"),
            dataset_where_sql=unretired_where_sql,
        ),
    }
    if abs(count["current"] - count["previous"]) > count["previous"] * 0.01:
        ##TODO: This should probably trigger an email as well.
        raise ValueError("Significant change in row counts.")

    LOG.info("Gather information about current MSAG ranges.")
    current_ranges = list(
        arcetl.attributes.as_dicts(
            dataset.TILLAMOOK_MSAG_RANGE.path("current"),
            field_names=MSAG_KEYS["master"],
        )
    )
    current_id_geoms = {rng["msag_id"]: rng["shape@"] for rng in current_ranges}
    current_ids = set(rng["msag_id"] for rng in current_ranges)
    most_recent_date = max(rng["effective_date"] for rng in current_ranges)
    LOG.info(
        "Start: Retire ranges no longer in current MSAG;"
        " update geometry for present ranges."
    )
    master_cursor = arcpy.da.UpdateCursor(
        dataset.TILLAMOOK_MSAG_RANGE.path("master"),
        field_names=["msag_id", "expiration_date", "shape@"],
        where_clause=unretired_where_sql,
    )
    with master_cursor:
        for msag_id, expiration_date, geom in master_cursor:
            if msag_id not in current_ids:
                LOG.info("Range (ID=%s) not in current: retiring.", msag_id)
                expiration_date = most_recent_date
                master_cursor.updateRow((msag_id, expiration_date, geom))
            elif geom is None or not geom.equals(current_id_geoms[msag_id]):
                LOG.info("Range (ID=%s) changed geometry.", msag_id)
                geom = current_id_geoms[msag_id]
                master_cursor.updateRow((msag_id, expiration_date, geom))
    LOG.info("End: Retire & update ranges.")
    LOG.info("Start: Add new ranges from current MSAG.")
    master_ids = set(
        msag_id
        for msag_id, in arcetl.attributes.as_iters(
            dataset.TILLAMOOK_MSAG_RANGE.path("master"),
            field_names=["msag_id"],
            dataset_where_sql=unretired_where_sql,
        )
    )
    current_cursor = arcpy.da.SearchCursor(
        dataset.TILLAMOOK_MSAG_RANGE.path("current"), field_names=MSAG_KEYS["master"]
    )
    master_cursor = arcpy.da.InsertCursor(
        dataset.TILLAMOOK_MSAG_RANGE.path("master"), field_names=MSAG_KEYS["master"]
    )
    with current_cursor, master_cursor:
        for row in current_cursor:
            msag_id = row[0]
            if msag_id not in master_ids:
                LOG.info("Range (ID=%s) new in current: adding.", msag_id)
                master_cursor.insertRow(row)
    LOG.info("End: Add ranges.")


def metadata_tillamook_ecd_etl():
    """Run ETL for Tillamook ECD deliverable metadata."""
    with arcetl.ArcETL("Metadata - Tillamook ECD Deliverable") as etl:
        etl.init_schema(dataset.TILLAMOOK_METADATA_DELIVERABLES.path())
        deliverables_meta = []
        for dataset_name, source_paths in TILLAMOOK_SOURCES_MAP.items():
            last_change_date = max(
                dataset.last_change_date(source_path) for source_path in source_paths
            )
            if last_change_date is None:
                last_change_date = datetime.datetime(2015, 4, 15)
            deliverables_meta.append([dataset_name, last_change_date])
        kwargs = {
            "id_field_names": ["dataset_name"],
            "field_names": ["dataset_name", "last_change_date"],
            "delete_missing_features": False,
            "use_edit_session": False,
        }
        etl.transform(
            arcetl.features.update_from_iters,
            update_features=deliverables_meta,
            **kwargs
        )
        etl.update(dataset.TILLAMOOK_METADATA_DELIVERABLES.path(), **kwargs)


def production_datasets_etl():
    """Run ETL for production cleaning & QA."""
    arcetl.workspace.execute_sql(
        "exec dbo.proc_Update_Production_Datasets;", database.LCOG_TILLAMOOK_ECD.path
    )


def publication_issues_message_etl():
    """Send message of issues that affect address publication."""
    field_names = ["description", "postcomm", "address", "address_id"]
    issues = sorted(
        arcetl.attributes.as_iters(
            dataset.TILLAMOOK_ADDRESS_POINT_ISSUES.path(),
            field_names,
            dataset_where_sql="ok_to_publish = 0",
        )
    )
    LOG.warning("Found validation publication issues: sending email.")
    table_header = "<tr>{}</tr>".format(
        "".join("<th>{}</th>".format(column) for column in field_names)
    )
    row_template = "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>"
    table_rows = "".join(row_template.format(*issue) for issue in issues)
    KWARGS_ISSUES_MESSAGE["body"] = KWARGS_ISSUES_MESSAGE["body"].format(
        table_header, table_rows
    )
    send_email(**KWARGS_ISSUES_MESSAGE)


##TODO: Ensure is like Lane version.
def road_centerline_etl():
    """Run ETL for road centerlines."""
    with arcetl.ArcETL("Road Centerlines") as etl:
        etl.extract(dataset.TILLAMOOK_ROAD_CENTERLINE.path("maint"))
        transform.add_missing_fields(
            etl, dataset.TILLAMOOK_ROAD_CENTERLINE, tags=["pub"]
        )
        # Assign overlays.
        overlay_kwargs = [
            {
                "field_name": "esn_L",
                "overlay_field_name": "esn",
                "overlay_dataset_path": dataset.TILLAMOOK_EMERGENCY_SERVICE_ZONE.path(),
            },
            {
                "field_name": "esn_R",
                "overlay_field_name": "esn",
                "overlay_dataset_path": dataset.TILLAMOOK_EMERGENCY_SERVICE_ZONE.path(),
            },
        ]
        for kwargs in overlay_kwargs:
            etl.transform(
                arcetl.attributes.update_by_overlay,
                overlay_central_coincident=True,
                **kwargs
            )
        # Assign joined values.
        etl.transform(
            arcetl.attributes.update_by_joined_value,
            field_name="join_id",
            join_dataset_path=dataset.TILLAMOOK_ALTERNATE_STREET_NAME.path(),
            join_field_name="join_id",
            on_field_pairs=[
                ("predir", "prime_predir"),
                ("name", "prime_name"),
                ("type", "prime_type"),
                ("sufdir", "prime_sufdir"),
            ],
        )
        # Build values: Translations.
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="st_class",
            function=(lambda cc, cc_st_map=CCLASS_ST_CLASS: cc_st_map.get(cc)),
            field_as_first_arg=False,
            arg_field_names=["cclass"],
        )
        # Build values: Constants.
        value_kwargs = [
            {"field_name": "state_L", "value": "OR"},
            {"field_name": "state_R", "value": "OR"},
        ]
        transform.update_attributes_by_values(etl, value_kwargs)
        # Build values: Concatenations.
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="full_name",
            function=concatenate_arguments,
            field_as_first_arg=False,
            arg_field_names=["predir", "name", "type", "sufdir"],
        )
        etl.load(dataset.TILLAMOOK_ROAD_CENTERLINE.path("pub"))


# Jobs.


NIGHTLY_JOB = Job("OEM_Tillamook_Datasets_Nightly", etls=[production_datasets_etl])


WEEKLY_JOB = Job(
    "OEM_Tillamook_Datasets_Weekly",
    etls=[
        emergency_service_zone_etl,
        # Must addresses & roads after emergency service zones.
        address_point_etl,
        road_centerline_etl,
        publication_issues_message_etl,
        # Must run after addresses.
        msag_ranges_current_etl,
        # Must run after current MSAG ranges.
        msag_update,
        # Must run after *all* dataset ETLs.
        metadata_tillamook_ecd_etl,
    ],
)


# Execution.


def main():
    """Script execution code."""
    args = argparse.ArgumentParser()
    args.add_argument("pipelines", nargs="*", help="Pipeline(s) to run")
    # Collect pipeline objects.
    pipelines = [globals()[arg] for arg in args.parse_args().pipelines]
    for pipeline in pipelines:
        execute_pipeline(pipeline)


if __name__ == "__main__":
    main()
