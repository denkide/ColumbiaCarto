"""Execution code for OEM/Tillamook production dataset QA/QC."""
import argparse
from collections import defaultdict
import datetime
import itertools
import logging
from operator import itemgetter

import sys
#sys.path.insert(0, 'C:\\ColumbiaCarto\\work\\TillamookPy\\Tillamook_data')
#sys.path.insert(0, 'C:\\ColumbiaCarto\\work\\TillamookPy\\Tillamook_data\\ArcETL')
#sys.path.insert(0, 'C:\\ColumbiaCarto\\work\\TillamookPy\\Tillamook_data\\ETLAssist')
#sys.path.insert(0, 'C:\\ColumbiaCarto\\work\\TillamookPy\\Tillamook_data\\helper')
#sys.path.insert(0, 'C:\\ColumbiaCarto\work\\TillamookPy\\Tillamook_data\\GIS_Other_ETL\\scripts')

sys.path.insert(0, 'D:\\Tillamook_911\\Code\\TillamookQA')
sys.path.insert(0, 'D:\\Tillamook_911\\Code\\TillamookQA\\Library')
sys.path.insert(0, 'D:\\Tillamook_911\\Code\\TillamookQA\\Library\\ArcETL')
sys.path.insert(0, 'D:\\Tillamook_911\\Code\\TillamookQA\\Library\\ETLAssist')
sys.path.insert(0, 'D:\\Tillamook_911\\Code\\TillamookQA\\Library\\helper')
sys.path.insert(0, 'D:\\Tillamook_911\\Code\\TillamookQA\\Library\\GIS_Other_ETL\\scripts')

import arcetl
from etlassist.pipeline import Job, execute_pipeline

from helper import database
from helper import dataset
from helper.value import concatenate_arguments


##TODO: Refactor to make more like Lane address issues procedure/output.

LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""

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


# Helpers.


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


def address_issues_core(addresses):
    """Generate issue dictionaries for invalid core attributes.

    Args:
        addresses (dict): Collection of mappings of address attribute name to value.

    Yields:
        dict: Dictionary with issue attributes.
    """
    valid_attr_details = {
        "community": {
            "dataset_path": dataset.TILLAMOOK_POSTAL_COMMUNITY.path(),
            "field_names": ["postcomm"],
            # Address points use uppercase community names.
            "func": (lambda vals: {val.upper() for val, in iter(vals) if val}),
        },
        "direction": {
            "dataset_path": dataset.OEM_VALID_STREET_DIRECTION.path(),
            "field_names": ["code"],
            "func": (lambda vals: {val for val, in iter(vals)}),
        },
        "suffix": {
            "dataset_path": dataset.OEM_VALID_NUMBER_SUFFIX.path(),
            "field_names": ["code"],
            "func": (lambda vals: {val for val, in iter(vals)}),
        },
        "type": {
            "dataset_path": dataset.OEM_VALID_STREET_TYPE.path(),
            "field_names": ["code"],
            "func": (lambda vals: {val for val, in iter(vals)}),
        },
        "unit_type": {
            "dataset_path": dataset.OEM_VALID_UNIT_TYPE.path(),
            "field_names": ["code"],
            "func": (lambda vals: {val for val, in iter(vals)}),
        },
        "unit": {
            "dataset_path": dataset.OEM_VALID_UNIT.path(),
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


def address_issues_extended(addresses):
    """Generate issue dictionaries for invalid extended attributes.

    Args:
        addresses (dict): Collection of mappings of address attribute name to value.

    Yields:
        dict: Dictionary with issue attributes.
    """
    valid_attr_details = {
        "county": {
            "dataset_path": dataset.OEM_VALID_COUNTY.path(),
            "field_names": ["county"],
            "func": (lambda vals: {val for val, in iter(vals)}),
        },
        "zip": {
            "dataset_path": dataset.TILLAMOOK_POSTAL_COMMUNITY.path(),
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


def address_issues_not_in_range(addresses):
    """Generate issue dictionaries for addresses not in a street range.

    Args:
        addresses (dict): Collection of mappings of address attribute name to value.

    Yields:
        dict: Dictionary with issue attributes.
    """
    rangeset = road_rangeset(road_centerline_generator(), segmented=False)
    for address in addresses:
        # Mileposts have "MP " at the beginning of the road name. Ignore here.
        if address["name"] is not None and any(
            [
                address["name"].startswith("MP "),
                address["name"].startswith("MPZ "),
            ]
        ):
            continue

        road_full_name = concatenate_arguments(
            address["predir"], address["name"], address["type"], address["sufdir"]
        )
        road_id = (address["postcomm"], road_full_name)
        if road_id not in rangeset:
            yield address_issue(
                address, description="Address does not exist in Road_Centerline ranges."
            )

        elif address["stnum"] not in rangeset[road_id]:
            yield address_issue(
                address, description="Address not covered by any road range."
            )


##TODO: If this works well, deprecate Valid_Street_Community table.
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
            "dataset_path": dataset.TILLAMOOK_ROAD_CENTERLINE.path("maint"),
            "field_names": road_name_keys + ["postcomm_L"],
            "func": set,
        },
        "road_right": {
            "dataset_path": dataset.TILLAMOOK_ROAD_CENTERLINE.path("maint"),
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


def address_point_generator():
    """Generate issue dictionaries representing maintenance address points.

    Yields:
        dict: Mapping of address attribute name to value.
    """
    addresses = arcetl.attributes.as_dicts(
        dataset.TILLAMOOK_ADDRESS_POINT.path("maint"),
        ADDRESS_FIELD_NAMES,
        dataset_where_sql="archived = 'N' and valid = 'Y'",
    )
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


def last_update_days_ago(feature, update_floor=datetime.datetime(1, 1, 1)):
    """Return integer of days since last update.

    Args:
        feature (dict): Mapping of feature attribute name to value.
        update_floor (datetime.datetime): Lowest update date-time to consider. If other
            feature timestamps are lower, function will return the floor.

    Returns:
        int: Days since last update.
    """
    dates = {feature["mod_date"], feature["init_date"], update_floor}
    dates.discard(None)
    last_update = max(dates)
    delta = datetime.datetime.today() - last_update
    return delta.days


def road_centerline_generator():
    """Generate issue dictionaries representing maintenance road centerlines.

    Yields:
        dict: Mapping of address attribute name to value.
    """
    roads = arcetl.attributes.as_dicts(
        dataset.TILLAMOOK_ROAD_CENTERLINE.path("maint"), ROAD_FIELD_NAMES
    )
    for road in roads:
        # Add compiled road name.
        road["full_name"] = concatenate_arguments(
            road["predir"], road["name"], road["type"], road["sufdir"]
        )
        yield road


def road_issue(road, description="Init.", ok_to_publish=True):
    """Return initialized issue dictionary for road.

    Args:
        road (dict): Mapping of road attribute name to value.
        description (str): Description of the issue.
        ok_to_publish (bool): Flag indicating whether the address should be published.

    Returns:
        dict: Dictionary with issue attributes.
    """
    issue = {"description": description, "ok_to_publish": ok_to_publish}
    for key in ("segid", "full_name", "postcomm_L", "postcomm_R", "shape@"):
        issue[key] = road[key]
    return issue


def road_issue_maint_info_map(issues_path):
    """Map of issues (segment ID + description) to maintenance info.

    Args:
        issues_path (str): Path to issues dataset.

    Returns:
        dict: Mapping of maintenance attribute name to value.
    """
    field_names = ["segid", "description", "maint_notes", "maint_init_date"]
    features = arcetl.attributes.as_dicts(issues_path, field_names)
    issue_info = {(feat["segid"], feat["description"]): feat for feat in features}
    return issue_info


def road_issues_classification(roads):
    """Generate issue dictionaries for invalid classification attributes.

    Args:
        roads (dict): Collection of mappings of road attribute name to value.

    Yields:
        dict: Dictionary with issue attributes.
    """
    valid_attr_details = {
        "cclass": {
            "domain_name": "CClass_Code",
            "workspace_path": database.LCOG_TILLAMOOK_ECD.path,
            "func": set,
        },
        # "road_class": {
        #     "domain_name": "MTFCC_S_Code",
        #     "workspace_path": database.LCOG_TILLAMOOK_ECD.path,
        #     "func": set,
        # },
    }
    valid_attrs = {
        attr_key: details["func"](
            arcetl.workspace.domain_metadata(
                details["domain_name"], details["workspace_path"]
            )["code_description_map"].keys()
        )
        for attr_key, details in valid_attr_details.items()
    }
    for road in roads:
        # if road["road_class"] not in valid_attrs["road_class"]:
        #     yield road_issue(
        #         road, description="`road_class` must be in `MTFCC_S_Code` domain.",
        #     )

        if road["cclass"] not in valid_attrs["cclass"]:
            yield road_issue(
                road, description="`cclass` must be in `CClass_Code` domain."
            )

        if road["spdlimit"] is not None:
            if road["spdlimit"] <= 0:
                yield road_issue(
                    road, description="`spdlimit` not be zero or negative."
                )

            if road["spdlimit"] % 5 != 0:
                yield road_issue(
                    road, description="`spdlimit` must be evenly divisible by five."
                )


def road_issues_core(roads):
    """Generate issue dictionaries for invalid core attributes.

    Args:
        roads (dict): Collection of mappings of road attribute name to value.

    Yields:
        dict: Dictionary with issue attributes.
    """
    valid_attr_details = {
        "direction": {
            "dataset_path": dataset.OEM_VALID_STREET_DIRECTION.path(),
            "field_names": ["code"],
            "func": (lambda vals: {val for val, in iter(vals)}),
        },
        "type": {
            "dataset_path": dataset.OEM_VALID_STREET_TYPE.path(),
            "field_names": ["code"],
            "func": (lambda vals: {val for val, in iter(vals)}),
        },
    }
    valid_attrs = {
        attr_key: details["func"](arcetl.attributes.as_iters(**details))
        for attr_key, details in valid_attr_details.items()
    }
    for road in roads:
        if road["predir"] not in valid_attrs["direction"]:
            yield road_issue(
                road,
                description=(
                    "`predir` must be in `code` field of Valid_Street_Direction."
                ),
            )

        if road["name"] is None:
            # Ignore roads that may never get road names assigned.
            if road["cclass"] is None or road["cclass"] >= 700:
                pass

            else:
                yield road_issue(road, description="`name` must not be null.")

        if road["type"] not in valid_attrs["type"]:
            yield road_issue(
                road, description="`type` must be in `code` field of Valid_Street_Type."
            )

        if road["sufdir"] not in valid_attrs["direction"]:
            yield road_issue(
                road,
                description=(
                    "`sufdir` must be in `code` field of Valid_Street_Direction."
                ),
            )


def road_issues_geocoding(roads):
    """Generate issue dictionaries for invalid geocoding attributes.

    Args:
        roads (dict): Collection of mappings of road attribute name to value.

    Yields:
        dict: Dictionary with issue attributes.
    """
    valid_attr_details = {
        "postcomm": {
            "dataset_path": dataset.TILLAMOOK_POSTAL_COMMUNITY.path(),
            "field_names": ["postcomm"],
            # Road centerlines use uppercase community names.
            "func": (lambda vals: {val.upper() for val, in iter(vals) if val}),
        },
        "county": {
            "dataset_path": dataset.OEM_VALID_COUNTY.path(),
            "field_names": ["county"],
            "func": (lambda vals: {val for val, in iter(vals) if val}),
        },
        "zip": {
            "dataset_path": dataset.TILLAMOOK_POSTAL_COMMUNITY.path(),
            "field_names": ["zip"],
            "func": (lambda vals: {val for val, in iter(vals)}),
        },
    }
    valid_attrs = {
        attr_key: details["func"](arcetl.attributes.as_iters(**details))
        for attr_key, details in valid_attr_details.items()
    }

    def _eval_number(road):
        """Generate issues with segment street numbers."""
        for end, side in itertools.product(["from", "to"], ["left", "right"]):
            attr = end + side
            if road[attr] is not None and road[attr] <= 0:
                yield road_issue(
                    road, description="`{}` must not be zero or negative.".format(attr)
                )

    def _eval_side_range(road):
        """Generate issues with segment side-ranges."""
        for side in ["left", "right"]:
            side_pair = side_from, side_to = (road["from" + side], road["to" + side])
            if side_from > side_to:
                yield road_issue(
                    road,
                    description="`{}` must be less than or equal to `{}`.".format(
                        side_from, side_to
                    ),
                )

            if side_pair.count(None) not in [0, 2]:
                yield road_issue(
                    road,
                    description="{}-range must be all null or not-null.".format(
                        side.title()
                    ),
                )

            if None not in side_pair and side_from % 2 != side_to % 2:
                yield road_issue(
                    road,
                    description="{}-range must be all odd, even, or null.".format(
                        side.title()
                    ),
                )

    def _eval_cross_range(road):
        """Generate issues with segment cross-range aspects."""
        if None in [
            road["fromleft"],
            road["fromright"],
            road["toleft"],
            road["toright"],
        ]:
            return

        if road["fromleft"] % 2 == road["fromright"] % 2:
            yield road_issue(
                road, description="Left & right ranges must not both be odd or even."
            )

    def _eval_areas(road):
        """Generate issues with segment postal areas."""
        # These areas usually get overlaid within a day.
        if last_update_days_ago(road) <= 1:
            return

        for side in ["L", "R"]:
            if road["postcomm_" + side] not in valid_attrs["postcomm"]:
                yield road_issue(
                    road,
                    description=(
                        "`postcomm` must be in `postcomm` field of Postal_Community."
                    ),
                )

            if road["zip_" + side] not in valid_attrs["zip"]:
                yield road_issue(
                    road,
                    description="`zip` must be in `zip` field of Postal_Community.",
                )

            if road["county_" + side] not in valid_attrs["county"]:
                yield road_issue(
                    road,
                    description="`county` must be in `county` field of Valid_County.",
                )

    for road in roads:
        for eval_func in [
            _eval_number,
            _eval_side_range,
            _eval_cross_range,
            _eval_areas,
        ]:
            for issue in eval_func(road):
                yield issue


def road_issues_geometry(roads):
    """Generate issue dictionaries for invalid geometry attributes.

    Args:
        roads (dict): Collection of mappings of road attribute name to value.

    Yields:
        dict: Dictionary with issue attributes.
    """
    for road in roads:
        if road["shape@"] is None:
            yield road_issue(
                road,
                description="Geometry must not be missing/empty.",
                ok_to_publish=False,
            )

        if road["shape@"].length < 4:
            yield road_issue(
                road, description="Geometry must not be shorter than four feet."
            )


def road_issues_network(roads):
    """Generate issue dictionaries for invalid network attributes.

    Args:
        roads (dict): Collection of mappings of road attribute name to value.

    Yields:
        dict: Dictionary with issue attributes.
    """
    valid_attrs = {"oneway": {None, "FT", "TF", "NT"}, "elev": {-1, 0, 1}}
    for road in roads:
        if road["oneway"] not in valid_attrs["oneway"]:
            yield road_issue(
                road, description="""`oneway` must be null, "FT", "TF", or "NT"."""
            )

        for end in ["F", "T"]:
            if road[end + "_elev"] not in valid_attrs["elev"]:
                yield road_issue(
                    road,
                    description=(
                        "`{0}_elev` must be"
                        " -1 (below grade), 0 (at grade), or 1 (above grade)."
                    ).format(end),
                )


def road_issues_overlaps(roads):
    """Generate issue dictionaries for overlapping road range attributes.

    Args:
        roads (dict): Collection of mappings of road attribute name to value.

    Yields:
        dict: Dictionary with issue attributes.
    """
    rangesets = road_rangeset(roads, segmented=True)
    for road in roads:
        for token, side in [("L", "left"), ("R", "right")]:
            road_id = (road["postcomm_" + token], road["full_name"])
            if None in (road["from" + side], road["to" + side]):
                continue

            side_range = set(range(road["from" + side], road["to" + side] + 1, 2))
            for other_segid, other_rangeset in rangesets[road_id].items():
                if road["segid"] == other_segid:
                    continue

                if all([road["segid"] != other_segid, side_range & other_rangeset]):
                    yield road_issue(
                        road,
                        description=(
                            "{}-range overlaps with other segment (segid={}).".format(
                                side.title(), other_segid
                            )
                        ),
                    )


def road_rangeset(roads, segmented=False):
    """Return dictionaries representing road ranges as sets.

    Args:
        roads (dict): Collection of mappings of road attribute name to value.
        segmented (bool): True makes the rangesets have a subdictionary of segment IDs
            as keys with their own range-set as their values.

    Returns:
        dict: Sets of road ranges.
            segmented = False: {
                (community, name, type, dir, sufdir): set(road-stnums)
            }
            segmented = True: {
                (community, name, type, dir, sufdir): {segid: set(segment-stnums)}
            }
    """
    rangesets = {}
    for road in roads:
        for token, side in [("L", "left"), ("R", "right")]:
            road_id = (road["postcomm_" + token], road["full_name"])
            if segmented:
                rangesets.setdefault(road_id, {})
                rangesets[road_id].setdefault(road["segid"], set())
            else:
                rangesets.setdefault(road_id, set())
            if None in [road["from" + side], road["to" + side]]:
                continue

            side_range = set(range(road["from" + side], road["to" + side] + 1, 2))
            if segmented:
                rangesets[road_id][road["segid"]].update(side_range)
            else:
                rangesets[road_id].update(side_range)
    return rangesets


# ETLs.


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
        LOG.info("Start: Collect address attributes.")
        addresses = list(address_point_generator())
        LOG.info("End: Collect.")
        LOG.info("Start: Validate address attributes.")
        issues = []
        for validator in validators:
            LOG.info(validator.__name__)
            issues.extend(validator(addresses))
        LOG.info("End: Validate.")
        etl.init_schema(dataset.TILLAMOOK_ADDRESS_POINT_ISSUES.path())
        if issues:
            maint_info = address_issue_maint_info_map(
                dataset.TILLAMOOK_ADDRESS_POINT_ISSUES.path()
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
                insert_features=issues,
                field_names=issues[0].keys(),
            )
        etl.load(dataset.TILLAMOOK_ADDRESS_POINT_ISSUES.path())


##TODO: GFQA - Addresses Not in Street Ranges.sql.
def address_point_not_in_range_etl():
    """Run procedures to find address points not covered by a road range.

    This could be folded into address_point_issues, if so desired.
    """
    validators = [address_issues_not_in_range]
    with arcetl.ArcETL("Address Point Issues: Not in Range") as etl:
        LOG.info("Start: Collect address attributes.")
        addresses = list(address_point_generator())
        LOG.info("End: Collect.")
        LOG.info("Start: Validate address attributes.")
        issues = []
        for validator in validators:
            LOG.info(validator.__name__)
            issues.extend(validator(addresses))
        LOG.info("End: Validate.")
        etl.init_schema(dataset.TILLAMOOK_ADDRESS_POINT_NOT_IN_ROAD_RANGE.path())
        if issues:
            maint_info = address_issue_maint_info_map(
                dataset.TILLAMOOK_ADDRESS_POINT_NOT_IN_ROAD_RANGE.path()
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
                insert_features=issues,
                field_names=issues[0].keys(),
            )
        etl.load(dataset.TILLAMOOK_ADDRESS_POINT_NOT_IN_ROAD_RANGE.path())


def road_centerline_issues_etl():
    """Run procedures to find invalid road maintenance attributes."""
    validators = [
        road_issues_core,
        road_issues_geocoding,
        road_issues_classification,
        road_issues_network,
        road_issues_geometry,
        road_issues_overlaps,
    ]
    with arcetl.ArcETL("Road Centerline Issues") as etl:
        LOG.info("Start: Collect road attributes.")
        roads = list(road_centerline_generator())
        LOG.info("End: Collect.")
        LOG.info("Start: Validate road attributes.")
        issues = []
        for validator in validators:
            LOG.info(validator.__name__)
            issues.extend(validator(roads))
        LOG.info("End: Validate.")
        etl.init_schema(dataset.TILLAMOOK_ROAD_CENTERLINE_ISSUES.path())
        if issues:
            maint_info = road_issue_maint_info_map(
                dataset.TILLAMOOK_ROAD_CENTERLINE_ISSUES.path()
            )
            for issue in issues:
                issue_maint_info = maint_info.get(
                    (issue["segid"], issue["description"]), {}
                )
                issue.update(issue_maint_info)
                issue.setdefault("maint_notes")
                issue.setdefault("maint_init_date", datetime.datetime.now())
            etl.transform(
                arcetl.features.insert_from_dicts,
                insert_features=issues,
                field_names=issues[0].keys(),
            )
        etl.load(dataset.TILLAMOOK_ROAD_CENTERLINE_ISSUES.path())


# Jobs.


NIGHTLY_JOB = Job(
    "OEM_Tillamook_QA_Nightly",
    etls=[
        address_point_issues_etl,
        address_point_not_in_range_etl,
        road_centerline_issues_etl,
    ],
)


# Execution.


def main():
    """Script execution code."""
    #args = argparse.ArgumentParser()
    args.add_argument("address_point_etl")
    args.add_argument("pipelines", nargs="*", help="Pipeline(s) to run")
    pipelines = [globals()[arg] for arg in args.parse_args().pipelines]
    for pipeline in pipelines:
        execute_pipeline(pipeline)


if __name__ == "__main__":
    main()
