import arcpy
from arcpy import env

import logging
from operator import itemgetter

import argparse
from collections import defaultdict

import sys
sys.path.insert(0, 'Library')
sys.path.insert(0, 'Library/ArcETL')
sys.path.insert(0, 'Library/ETLAssist')
sys.path.insert(0, 'Library/helper')
sys.path.insert(0, 'Library/GIS_Other_ETL/scripts')

import arcetl
from etlassist.pipeline import Job, execute_pipeline
from etlassist.value import concatenate_arguments

tillamook_gdb = "D:\Tillamook_911\TillamookQA\Tillamook_911.gdb" #arcpy.GetParameterAsText(0)     #
tillamook_QC_gdb = "D:\Tillamook_911\TillamookQA\Tillamook_QC_Reference.gdb" #arcpy.GetParameterAsText(1)  # 

#DJR:
ADDRESS_PTS_PATH = tillamook_gdb + "\\Address_Point"
TILLAMOOK_POSTAL_COMMUNITY = tillamook_gdb + "\\Postal_Community"
OEM_VALID_STREET_DIRECTION = tillamook_gdb + "\\Valid_Street_Direction"
OEM_VALID_NUMBER_SUFFIX =  tillamook_gdb + "\\Valid_Number_Suffix"
OEM_VALID_STREET_TYPE = tillamook_gdb + "\\Valid_Street_Type"
OEM_VALID_UNIT_TYPE = tillamook_gdb + "\\Valid_Unit_Type"
OEM_VALID_UNIT = tillamook_gdb + "\\Valid_Unit"
TILLAMOOK_ROAD_CENTERLINE = tillamook_gdb + "\\Road_Centerline"
OEM_VALID_COUNTY = tillamook_gdb + "\\Valid_County"
TILLAMOOK_ADDRESS_POINT_ISSUES = tillamook_QC_gdb + "\\Address_Point_Issues"

ADDRESS_FIELD_NAMES = [
    # Core address attributes.
    "address_id",
    "stnum",
    "stnumsuf",
    "predir",
    "name",
    "type",
    "sufdir",
    "unit_type",
    "unit",
    "postcomm",
    # Extended address attributes.
    "zip",
    "county",
    # Maintenance attributes.
    "valid",
    "archived",
    "confidence",
    # "notes",
    "init_date",
    "mod_date",
    # "editor",
    # auto_notes,
    # Geometry attributes.
    "shape@",
]
"""list: Collection of address dataset field names."""
ROAD_FIELD_NAMES = [
    # Core road attributes.
    "segid",
    "predir",
    "name",
    "type",
    "sufdir",
    # Geocoding attributes.
    "fromleft",
    "toleft",
    "fromright",
    "toright",
    "postcomm_L",
    "postcomm_R",
    "zip_L",
    "zip_R",
    "county_L",
    "county_R",
    # Classification attributes.
    "road_class",
    "cclass",
    "spdlimit",
    # Network definition attributes.
    "oneway",
    "F_elev",
    "T_elev",
    # Maintenance attributes.
    # "notes",
    "init_date",
    "mod_date",
    # "editor",
    # "auto_notes",
    # Geometry attributes.
    "shape@",
]
"""list: Collection of road dataset field names."""


#import DatabaseHelpers
#env.Workspace = r"C:\ColumbiaCarto\work\GDB\Tillamook_911.gdb"
#validStreetDir = DatabaseHelpers.returnValidStreetDirection()

###########General Stuff
# clean field values:
#   trim edge-spaces
#   empty text gets 'NULL'
# Remove features missing Geometry
#   happens when accidentally typing attributes in the bottom row of the table panel of ArcMap



###########Address Points:
###################################################################################################
###################################################################################################
# 'stnum' must not be null or negative:  Not OK to publish
def checkStnum():
    print('Check stnum')

# 'stnumsuf' must be in 'code' field of Valid_Number_Suffix table:  Not OK to publish
def checkStnumsuf():
    print('Check stnumsuf')

# 'predir' and sufdir must be in 'code' field of Valid_Street_Direction table:  Not OK to publish

# 'name' must not be null:  Not OK to publish

# 'type' must be in 'code' field of Valid_Street_Type table:  Not OK to publish

# 'postcomm' must be in 'postcomm' field of Postal_Community feature class:  OK to publish

# 'unit_type' must be in 'code' field of Valid_Unit_Type table:  Not OK to publish

# 'unit' must be in 'unit' field of Valid_Unit table:  Not OK to publish

# 'unit' must not be null if 'unit_type' is not null:  Not OK to publish

# Street name + community must match fields name and postcomm_L or postcomm_R of Road_Centerline feature class:  Not OK to publish


######## Extended attributes
# 'zip' must not be null and must be in 'zip' field of Postal_Community feature class:  OK to publish  

# 'county' must be in 'county' field of Valid_County table:  OK to publish

######### Maintenance attributes:
# 'valid' & archived must be 'N' or 'Y'.

# If valid or aarchived = Y, these records are not published

###########Duplicate addresses:  
############  The following fields are concatenated and then compared. 
############Duplicate addresses are flagged and the first occurrence is passed through to the publication database.
# stnum  House Number

# stnumsuf House Number Suffix 

# predir Prefix Direction

# name Street Name

# type Street Type

#suffix Street Suffix

#community City or Community Name
def address_issues_extended(addresses):
    """Generate issue dictionaries for invalid extended attributes.

    Args:
        addresses (dict): Collection of mappings of address attribute name to value.

    Yields:
        dict: Dictionary with issue attributes.
    """
    valid_attr_details = {
        "county": {
            "dataset_path": OEM_VALID_COUNTY,
            "field_names": ["county"],
            "func": (lambda vals: {val for val, in iter(vals)}),
        },
        "zip": {
            "dataset_path": TILLAMOOK_POSTAL_COMMUNITY,
            "field_names": ["zip"],
            "func": (lambda vals: {val for val, in iter(vals) if val}),
        },
    }
    valid_attrs = {
        attr_key: details["func"](arcetl.attributes.as_iters(**details))
        for attr_key, details in valid_attr_details.items()
    }
    for address in addresses:
        if address["zip"] is None:
            yield address_issue(address, description="`zip` must not be null.")

        if address["zip"] not in valid_attrs["zip"]:
            yield address_issue(
                address, description="`zip` must be in `zip` field of Postal_Community."
            )

        if address["county"] not in valid_attrs["county"]:
            yield address_issue(
                address,
                description="`county` must be in `county` field of Valid_County.",
            )

def address_issues_core(addresses):
    """Generate issue dictionaries for invalid core attributes.

    Args:
        addresses (dict): Collection of mappings of address attribute name to value.

    Yields:
        dict: Dictionary with issue attributes.
    """

    valid_attr_details = {
        "community": {
            "dataset_path": TILLAMOOK_POSTAL_COMMUNITY,
            "field_names": ["postcomm"],
            # Address points use uppercase community names.
            "func": (lambda vals: {val.upper() for val, in iter(vals) if val}),
        },
        "direction": {
            "dataset_path": OEM_VALID_STREET_DIRECTION,
            "field_names": ["code"],
            "func": (lambda vals: {val for val, in iter(vals)}),
        },
        "suffix": {
            "dataset_path": OEM_VALID_NUMBER_SUFFIX,
            "field_names": ["code"],
            "func": (lambda vals: {val for val, in iter(vals)}),
        },
        "type": {
            "dataset_path": OEM_VALID_STREET_TYPE,
            "field_names": ["code"],
            "func": (lambda vals: {val for val, in iter(vals)}),
        },
        "unit_type": {
            "dataset_path": OEM_VALID_UNIT_TYPE,
            "field_names": ["code"],
            "func": (lambda vals: {val for val, in iter(vals)}),
        },
        "unit": {
            "dataset_path": OEM_VALID_UNIT,
            "field_names": ["unit"],
            "func": (lambda vals: {val for val, in iter(vals)}),
        },
    }
    valid_attrs = {
        attr_key: details["func"](arcetl.attributes.as_iters(**details))
        for attr_key, details in valid_attr_details.items()
    }
    for address in addresses:
        if any([address["stnum"] is None, address["stnum"] < 0]):
            yield address_issue(
                address,
                description="`stnum` must not be null or negative.",
                ok_to_publish=False,
            )

        if address["stnumsuf"] not in valid_attrs["suffix"]:
            yield address_issue(
                address,
                description=(
                    "`stnumsuf` must be in `code` field of Valid_Number_Suffix."
                ),
                ok_to_publish=False,
            )

        if address["predir"] not in valid_attrs["direction"]:
            yield address_issue(
                address,
                description=(
                    "`predir` must be in `code` field of Valid_Street_Direction."
                ),
                ok_to_publish=False,
            )

        if address["name"] is None:
            yield address_issue(
                address, description="`name` must not be null.", ok_to_publish=False
            )

        if address["type"] not in valid_attrs["type"]:
            yield address_issue(
                address,
                description="`type` must be in `code` field of Valid_Street_Type.",
                ok_to_publish=False,
            )

        if address["sufdir"] not in valid_attrs["direction"]:
            yield address_issue(
                address,
                description=(
                    "`sufdir` must be in `code` field of Valid_Street_Direction."
                ),
                ok_to_publish=False,
            )

        if address["unit_type"] not in valid_attrs["unit_type"]:
            yield address_issue(
                address,
                description="`unit_type` must be in `code` field of Valid_Unit_Type.",
                ok_to_publish=False,
            )

        if address["unit"] not in valid_attrs["unit"]:
            yield address_issue(
                address,
                description="`unit` must be in `unit` field of Valid_Unit.",
                ok_to_publish=False,
            )

        if address["unit"] is None and address["unit_type"] is not None:
            yield address_issue(
                address,
                description="`unit` must not be null if `unit_type` is not null.",
                ok_to_publish=False,
            )

        if all(
            [
                address["postcomm"] not in valid_attrs["community"],
                last_update_days_ago(address) > 1,
            ]
        ):
            yield address_issue(
                address,
                description=(
                    "`postcomm` must be in `postcomm` field of Postal_Community."
                ),
                ok_to_publish=False,
            )

def address_issues_street_xref(addresses):
    """Generate issue dictionaries for invalid address & street cross-reference.

    Args:
        addresses (dict): Collection of mappings of address attribute name to value.

    Yields:
        dict: Dictionary with issue attributes.
    """
    road_name_keys = ["predir", "name", "type", "sufdir"]
    valid_attr_details = {
        "road_left": {
            "dataset_path": TILLAMOOK_ROAD_CENTERLINE,
            "field_names": road_name_keys + ["postcomm_L"],
            "func": set,
        },
        "road_right": {
            "dataset_path": TILLAMOOK_ROAD_CENTERLINE,
            "field_names": road_name_keys + ["postcomm_R"],
            "func": set,
        },
    }
    valid_attrs = {
        attr_key: details["func"](arcetl.attributes.as_iters(**details))
        for attr_key, details in valid_attr_details.items()
    }
    valid_attrs["road"] = valid_attrs.pop("road_left") | valid_attrs.pop("road_right")
    for address in addresses:
        name = address["name"]
        # Mileposts have "MP " at the beginning of the road name. Remove for eval.
        if address["name"] is not None and address["name"].startswith("MP "):
            name = address["name"][3:]
        road = (
            address["predir"],
            name,
            address["type"],
            address["sufdir"],
            address["postcomm"],
        )
        if road not in valid_attrs["road"]:
            yield address_issue(
                address,
                description=(
                    "Road name + community must match fields of Road_Centerline."
                ),
            )


def address_issues_maintenance(addresses):
    """Generate issue dictionaries for invalid maintenance attributes.

    Args:
        addresses (dict): Collection of mappings of address attribute name to value.

    Yields:
        dict: Dictionary with issue attributes.
    """
    valid_attrs = {"ny": {"N", "Y"}, "lmh": {"L", "M", "H"}}
    for address in addresses:
        if address["valid"] not in valid_attrs["ny"]:
            yield address_issue(
                address,
                description="""`valid` must be "N" or "Y".""",
                ok_to_publish=False,
            )

        if address["archived"] not in valid_attrs["ny"]:
            yield address_issue(
                address,
                description="""`archived` must be "N" or "Y".""",
                ok_to_publish=False,
            )

        if address["confidence"] not in valid_attrs["lmh"]:
            yield address_issue(
                address,
                description="""`address_confidence` must be "L", "M", or "H".""",
            )


def address_issues_duplicates(addresses):
    """Generate issue dictionaries for invalid duplicates.

    Core address is defined as concatenated address & postal community.
    The address with the earliest init_date will not be flagged.

    Args:
        addresses (dict): Collection of mappings of address attribute name to value.

    Yields:
        dict: Dictionary with issue attributes.
    """
    core_address_map = defaultdict(list)
    for address in addresses:
        core_address = (address["address"], address["postcomm"])
        core_address_map[core_address].append(address)
    for duplicates in core_address_map.values():
        # Ignore non-duplicates.
        if len(duplicates) == 1:
            continue

        duplicates = sorted(duplicates, key=itemgetter("init_date"))
        for i, address in enumerate(duplicates, start=1):
            yield address_issue(
                address,
                description="Must have unique address + community.",
                # Allows oldest address to publish, others to not.
                ok_to_publish=True if i == 1 else False,
            )

def last_update_days_ago(address):
    """Return days since last update for address.

    Args:
        address (dict): Mapping of attribute names to values.

    Returns:
        int: Number of days.
    """
    if address["mod_date"]:
        date = address["mod_date"]
    else:
        date = address["init_date"]
    
    #if address["last_update_date"]:
    #    date = address["last_update_date"]
    #else:
    #    date = address["initial_create_date"]

    return (datetime.datetime.now() - date).total_seconds() / 86400


def address_issue(address, description="Init.", ok_to_publish=True):
    """Return initialized issue dictionary for address.

    Args:
        address (dict): Mapping of address attribute name to value.
        description (str): Description of the issue.
        ok_to_publish (bool): Flag indicating whether the address should be published.

    Returns:
        dict: Dictionary with issue attributes.
    """
    issue = {"description": description, "ok_to_publish": ok_to_publish}
    for key in ["address_id", "address", "postcomm", "shape@"]:
        issue[key] = address[key]
    return issue

## ----------------------------------
#DJR:: This works 5/26/2019
## ----------------------------------
## ----------------------------------
def address_point_generator():
    """Generate issue dictionaries representing maintenance address points.

    Yields:
        dict: Mapping of address attribute name to value.
    """
    # path: C:\ColumbiaCarto\work\GDB\Tillamook_911.gdb\Address_Point
    # field_names 
    # kwargs 
    addresses = arcetl.attributes.as_dicts(
        ADDRESS_PTS_PATH,
        ADDRESS_FIELD_NAMES,
        dataset_where_sql="archived = 'N' and valid = 'Y'",
    )
    #addresses = arcetl.attributes.as_dicts(
    #    dataset.TILLAMOOK_ADDRESS_POINT.path("maint"),
    #    ADDRESS_FIELD_NAMES,
    #    dataset_where_sql="archived = 'N' and valid = 'Y'",
    #)

    for address in addresses:
        # Add compiled address & road full name.
        address["address"] = concatenate_arguments(
            address["stnum"],
            address["stnumsuf"],
            address["predir"],
            address["name"],
            address["type"],
            address["sufdir"],
            address["unit_type"],
            address["unit"],
        )
        yield address

def address_issue_maint_info_map(issues_path):
    """Map of issues (address ID + description) to maintenance info.

    Args:
        issues_path (str): Path to issues dataset.

    Returns:
        dict: Mapping of maintenance attribute name to value.
    """
    field_names = ["address_id", "description", "maint_notes", "maint_init_date"]
    features = arcetl.attributes.as_dicts(issues_path, field_names)
    issue_info = {(feat["address_id"], feat["description"]): feat for feat in features}
    return issue_info

###########
###########
###########
###########
###########
###########
###########

def address_point_issues_etl():
    """Run procedures to find invalid address maintenance attributes."""
    validators = [
        address_issues_core,
        address_issues_street_xref,
        address_issues_extended,
        address_issues_maintenance,
        address_issues_duplicates,
        # address_issues_geometry,
    ]
    with arcetl.ArcETL("Address Point Issues") as etl:
        ## DJR:: LOG.info("Start: Collect address attributes.")
        print("Start: Collect address attributes.")
        addresses = list(address_point_generator())
        ## DJR LOG.info("End: Collect.")
        print("End: Collect.")
        ## DJR LOG.info("Start: Validate address attributes.")
        print("Start: Validate address attributes.")
        issues = []
        for validator in validators:
            ## DJR LOG.info(validator.__name__)
            print(validator.__name__)
            issues.extend(validator(addresses))
        ## DJR LOG.info("End: Validate.")
        etl.init_schema(TILLAMOOK_ADDRESS_POINT_ISSUES)
        if issues:
            maint_info = address_issue_maint_info_map(
                TILLAMOOK_ADDRESS_POINT_ISSUES
            )
            for issue in issues:
                issue_maint_info = maint_info.get(
                    (issue["address_id"], issue["description"]), {}
                )
                issue.update(issue_maint_info)
                issue.setdefault("maint_notes")
                issue.setdefault("maint_init_date", datetime.datetime.now())
            etl.transform(
                arcetl.features.insert_from_dicts,
                insert_features=issues,field_names=issues[0].keys(),
            )
        etl.load(TILLAMOOK_ADDRESS_POINT_ISSUES)


def main():
    print("Main")
    #print validStreetDir
    print("do it")
    address_point_issues_etl()
    print("done")


if __name__ == "__main__":
    main()


