"""Execution code for transportation processing."""
import argparse
from collections import defaultdict
from copy import copy
import functools
from itertools import combinations
import logging
import os

import arcetl
from etlassist.pipeline import Job, execute_pipeline

from helper import database
from helper import dataset
from helper.misc import TOLERANCE
from helper import path
from helper import transform
from helper.value import clean_whitespace_without_clear, concatenate_arguments


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""

ABBREVIATIONS_TO_EXPAND = {
    "CG": "Campground",
    "EX": "Extension",
    "FS": "Forest Service",
    "LS": "Landfill Station",
    "TS": "Transfer Station",
    "WC": "Work Camp",
}
"""dict: Mapping of abbreviation to expansion."""
ABBREVIATIONS_TO_LEAVE = {
    "BLM": "Bureau of Land Management",
    "CCC": None,  # Don't remember.
    "MC": None,  # Don't remember.
    "US": "United States",
    # Directionals.
    "NE": "Northeast",
    "NW": "Northwest",
    "SE": "Southeast",
    "SW": "Southwest",
    "EB": "Eastbound",
    "NB": "Northbound",
    "SB": "Southbound",
    "WB": "Westbound",
    # Roman numerals (single-numerals don"t get title-cased).
    "II": "Two",
    "III": "Three",
    "IV": "Four",
}
"""dict: Mapping of abbrevation to expansion."""
CCLASS_CODE = {
    "freeway": 100,
    "freeway_ramp": 121,
    "highway": 200,
    "highway_ramp": 221,
    "major_arterial": 300,
    "minor_arterial": 350,
    "major_collector": 400,
    "minor_collector": 450,
    "local": 500,
    "alley": 600,
    "private_unnamed": 700,
    "resource_mgmt": 900,
    "resource_mgmt_unpaved": 950,
    "unclassified": 999,
}
"""dict: Mapping of cartographic class name to integer value."""
FCLASS_CCLASS = {
    "ART": "major_arterial",
    "MAJART": "major_arterial",
    "MINART": "minor_arterial",
    "COLL": "major_collector",
    "MAJCOLL": "major_collector",
    "MINCOLL": "minor_collector",
    "LOCAL": "local",
}
"""dict: Mapping of local functional class code to cartographic class name."""
ORDINAL_ENDS = {
    "0th",
    "1st",
    "2nd",
    "3rd",
    "4th",
    "5th",
    "6th",
    "7th",
    "8th",
    "9th",
    # For the teens.
    "1th",
    "2th",
    "3th",
}
"""set: Collection of strings representing the neds of ordinal numbers."""
PRIVATE_ROAD_NAMES = {None, "", "PRIVATE", "PRIVATE DR", "PRIVATE ROAD"}
"""set: Collection of road names that represent private/unnamed roads."""


# Helpers.


def any_in_range(numbers, floor, ceiling):
    """Return True f any of the numbers are in given range.

    Args:
        numbers (iter): Iterable of integers.
        floor (int): Lowest integer in the range.
        ceiling (int): Highest integer in the range.

    Returns:
        bool
    """
    return any(number in range(floor, ceiling + 1) for number in numbers)


def cartographic_class(**road_attributes):
    """Return cartographic class integer based on given road attributes.

    Cartographic class is currently used to determine the features included in the
    highway and general arterial feature classes from the publication roads.

    So long as the keyword arguments have enough of the below keys, one can use any
    source for the attributes. One can get a value with few (or no) keys provided, but
    more information will enable a better classification.

    If changing the logic for these definitions, remember that it is written in the
    algorithmic logic of Python. This is very similar (and almost paste-able) to SQL,
    but some things will be different (==, startswith(), etc.).

    Args:
        **road_attributes (dict): Mapping of attribute name to value.

    Returns:
        int
    """
    road = defaultdict(None, road_attributes)
    road["dir_name_type"] = (road["dir"], road["name"], road["type"])
    road["addresses"] = (road["l_ladd"], road["l_hadd"], road["r_ladd"], road["r_hadd"])
    # Step 1: Apply functional-based classes.
    # cclass will reflect this value unless something further below applies.
    if "fclass" in road:
        road["cclass"] = CCLASS_CODE[FCLASS_CCLASS[road["fclass"]]]
    else:
        road["cclass"] = CCLASS_CODE["unclassified"]
    # Apply class upgrades/downgrades.
    cclass_adjustor_functions = [
        cclass_promote_freeway,
        cclass_promote_highway,
        cclass_demote_sublocal,
    ]
    for adjust_cclass in cclass_adjustor_functions:
        road["cclass"] = adjust_cclass(**road)
    return road["cclass"]


def cclass_demote_sublocal(**road_attributes):
    """Return demoted cclass if road attributes indicate an sub-local road.

    Args:
        **road_attributes (dict): Mapping of attribute name to value.

    Returns:
        int
    """
    road = copy(road_attributes)
    if road["cclass"] >= CCLASS_CODE["local"]:
        # Alleys.
        if road["type"] == "ALY":
            road["cclass"] = CCLASS_CODE["alley"]
        # Private unnamed roads.
        if all([road["owner"] == "PVT", road["name"].upper() in PRIVATE_ROAD_NAMES]):
            road["cclass"] = CCLASS_CODE["private_unnamed"]
        # Resource management roads.
        # Could add EWEB & landfill-WMD roads to these if we are into that.
        if road["owner"] in ["BLM", "USFS"]:
            if road["paved"] == "Y":
                road["cclass"] = CCLASS_CODE["resource_mgmt"]
            else:
                road["cclass"] = CCLASS_CODE["resource_mgmt_unpaved"]
    return road["cclass"]


def cclass_promote_freeway(**road_attributes):
    """Return promoted cclass if road attributes indicate a freeway.

    Args:
        **road_attributes (dict): Mapping of attribute name to value.

    Returns:
        int
    """
    road = copy(road_attributes)
    if CCLASS_CODE["freeway"] < road["cclass"] <= CCLASS_CODE["major_arterial"]:
        if any(
            [
                # Verify by name.
                road["dir_name_type"]
                in [
                    ("", "DELTA", "HWY"),
                    ("", "I-5", ""),
                    ("", "I-105", ""),
                    ("", "RANDY PAPE BELTLINE", ""),
                ],
                # Verify by UGB + name (and maybe range).
                # We pretend Hwy 126 through Springfield is a freeway.
                road["ugbcity"] == "SPR"
                and any([road["dir_name_type"] in [("", "HWY 126", "")]]),
            ]
        ):
            road["cclass"] = CCLASS_CODE["freeway"]
    # Ramps & connectors.
    if CCLASS_CODE["freeway_ramp"] < road["cclass"] <= CCLASS_CODE["minor_collector"]:
        if any(
            [
                road["name"].startswith("DELTA HWY "),
                road["name"].startswith("I-5 "),
                road["name"].startswith("I-105 "),
                # We pretend Hwy 126 through Springfield is a freeway.
                road["ugbcity"] == "SPR" and road["name"].startswith("HWY 126 "),
                road["name"].startswith("RANDY PAPE BELTLINE "),
                road["name"].startswith("BELTLINE "),
            ]
        ):
            if any(
                [
                    road["name"].endswith(" CONNECTOR"),
                    road["name"].endswith(" FLYOVER"),
                    road["name"].endswith(" OFFRAMP"),
                    road["name"].endswith(" ONRAMP"),
                    road["name"].endswith(" RAMP"),
                ]
            ):
                road["cclass"] = CCLASS_CODE["freeway_ramp"]
    return road["cclass"]


def cclass_promote_highway(**road_attributes):
    """Return promoted cclass if road attributes indicate a highway.

    Args:
        **road_attributes (dict): Mapping of attribute name to value.

    Returns:
        int
    """
    road = copy(road_attributes)
    if CCLASS_CODE["highway"] < road["cclass"] <= CCLASS_CODE["major_collector"]:
        if any(
            [
                # Verify by name.
                road["dir_name_type"]
                in [
                    ("", "HWY 36", ""),
                    ("", "HWY 58", ""),
                    ("", "HWY 99", ""),
                    ("", "HWY 99E", ""),
                    ("", "HWY 99N", ""),
                    ("", "HWY 99S", ""),
                    ("", "HWY 99W", ""),
                    ("", "HWY 101", ""),
                    ("", "HWY 126", ""),
                    ("", "MCKENZIE", "HWY"),
                    ("", "MCVAY", "HWY"),
                    ("", "NORTHWEST EXPRESSWAY", ""),
                    ("", "OLD MCKENZIE", "HWY"),
                    ("", "TERRITORIAL", "HWY"),
                    ("", "TERRITORIAL", "RD"),
                    # Hwy 58 aliases.
                    ("", "OLD WILLAMETTE", "HWY"),
                    # Hwy 99 aliases.
                    ("", "FRANKLIN", "BLVD"),
                    ("N", "PACIFIC", "HWY"),
                    # Springfield-Creswell Hwy aliases.
                    ("", "BOB STRAUB", "PKWY"),
                    ("", "CLOVERDALE", "RD"),
                    ("E", "CLOVERDALE", "RD"),
                    ("", "MARTIN LUTHER KING JR", "PKWY"),
                    ("E", "OREGON", "AVE"),
                    ("", "PARKWAY", "RD"),
                    ("", "PIONEER PARKWAY EAST", ""),
                    ("", "PIONEER PARKWAY WEST", ""),
                ],
                # Verify by UGB + name (and maybe range).
                road["ugbcity"] == "COT"
                and any(
                    [
                        # Hwy 99 aliases.
                        road["dir_name_type"]
                        in [("N", "9TH", "ST")]
                    ]
                ),
                road["ugbcity"] == "CRE"
                and any(
                    [
                        # Hwy 99 aliases.
                        road["dir_name_type"]
                        in [("S", "FRONT", "ST"), ("N", "MILL", "ST")]
                    ]
                ),
                road["ugbcity"] == "EUG"
                and any(
                    [
                        # Hwy 99 aliases.
                        road["dir_name_type"]
                        in [
                            ("E", "7TH", "AVE"),
                            ("E", "BROADWAY", ""),
                            ("", "MILL", "ST"),
                        ],
                        road["dir_name_type"] == ("W", "7TH", "AVE")
                        and any_in_range(road["addresses"], 2000, 28899),
                        # Hwy 126 aliases.
                        road["dir_name_type"] == ("W", "11TH", "AVE")
                        and any_in_range(road["addresses"], 0, 2400),
                        road["dir_name_type"] == (None, "GARFIELD", "ST")
                        and any_in_range(road["addresses"], 700, 1099),
                    ]
                ),
                road["ugbcity"] == "JUN"
                and any(
                    [
                        # Hwy 99 aliases.
                        road["dir_name_type"]
                        in [("", "IVY", "ST")]
                    ]
                ),
                road["ugbcity"] == "SPR"
                and any(
                    [
                        # Hwy 126 Business.
                        road["dir_name_type"]
                        in [("S", "A", "ST"), ("", "MAIN", "ST")]
                    ]
                ),
                # Verify by name + range.
                # Springfield-Creswell Hwy aliases.
                road["dir_name_type"] in [("", "JASPER", "RD")]
                and any_in_range(road["addresses"], 36300, 36799),
            ]
        ):
            road["cclass"] = CCLASS_CODE["highway"]
    # Ramps & connectors.
    if CCLASS_CODE["highway_ramp"] < road["cclass"] <= CCLASS_CODE["minor_collector"]:
        if any(
            [
                road["name"].startswith("HWY 36 "),
                road["name"].startswith("HWY 58 "),
                road["name"].startswith("HWY 99 "),
                road["name"].startswith("HWY 99E "),
                road["name"].startswith("HWY 99N "),
                road["name"].startswith("HWY 99S "),
                road["name"].startswith("HWY 99W "),
                road["name"].startswith("HWY 101 "),
                road["name"].startswith("HWY 126 "),
            ]
        ):
            if any(
                [
                    road["name"].endswith(" CONNECTOR"),
                    road["name"].endswith(" FLYOVER"),
                    road["name"].endswith(" OFFRAMP"),
                    road["name"].endswith(" ONRAMP"),
                    road["name"].endswith(" RAMP"),
                ]
            ):
                road["cclass"] = CCLASS_CODE["highway_ramp"]
    return road["cclass"]


def intersection_name(reverse=False, **road_attributes):
    """Return intersection name, a concatenation of the street name parts.

    Args:
        reverse (bool): Flag indicating whether to reverse road names.
        **road_attributes (dict): Mapping of attribute name to value.

    Returns:
        str.
    """
    road = copy(road_attributes)
    road_names = [
        concatenate_arguments(road["dir01"], road["name01"], road["type01"]).strip(),
        concatenate_arguments(road["dir02"], road["name02"], road["type02"]).strip(),
    ]
    return " & ".join(reversed(road_names)) if reverse else " & ".join(road_names)


def is_general_arterial(**road_attributes):
    """Return True if given attributes define a general arterial road.

    Cartographic class is currently used to determine the features included in the
    highway and general arterial feature classes from the publication roads.

    So long as the keyword arguments have enough of the below keys, one can use any
    source for the attributes. One can get a value with few (or no) keys provided, but
    more information will enable a better classification.

    If changing the logic for these definitions, remember that it is written in the
    algorithmic logic of Python. This is very similar (and almost paste-able) to SQL,
    but some things will be different (==, startswith(), etc.).

    Args:
        **road_attributes (dict): Mapping of attribute name to value.

    Returns:
        bool
    """
    road = defaultdict(None, road_attributes)
    road["dir_name_type"] = (road["dir"], road["name"], road["type"])
    road["addresses"] = (road["l_ladd"], road["l_hadd"], road["r_ladd"], road["r_hadd"])
    # All freeways & highways included (not ramps/connectors).
    if road["cclass"] in [CCLASS_CODE["freeway"], CCLASS_CODE["highway"]]:
        return True

    # No extra roads beyond highways:
    # Alsea, Blachly, Cheshire, Deadwood, Dexter, Elmira, Leaburg, Mapleton,
    # McKenzie Bridge, Marcola, Pleasant Hill, Swisshome, Tidewater, Vida, Yachats.

    if road["mailcity"] == "Blue River" and any(
        [
            # Verify by class + name.
            road["cclass"] <= CCLASS_CODE["major_collector"]
            and road["dir_name_type"]
            in [
                ("", "AUFDERHEIDE", "DR"),
            ],
            # Special cases (currently none).
        ]
    ):
        return True

    if road["mailcity"] == "Coburg" and any(
        [
            # Verify by class + name.
            road["cclass"] <= CCLASS_CODE["minor_arterial"]
            and road["dir_name_type"]
            in [
                ("E", "PEARL", "ST"),
                ("", "VAN DUYN", "RD"),
                ("W", "VAN DUYN", "ST"),
                ("N", "WILLAMETTE", "ST"),
                ("S", "WILLAMETTE", "ST"),
            ],
            # Special cases (currently none).
        ]
    ):
        return True

    if road["mailcity"] == "Cottage Grove" and any(
        [
            # Verify by class + name.
            road["cclass"] <= CCLASS_CODE["minor_arterial"]
            and road["dir_name_type"]
            in [
                ("E", "MAIN", "ST"),
                ("W", "MAIN", "ST"),
                ("N", "RIVER", "RD"),
                ("S", "RIVER", "RD"),
                ("E", "WHITEAKER" ,"AVE"),
                ("", "WOODSON", "PL"),
            ],
            road["cclass"] <= CCLASS_CODE["major_collector"]
            and road["dir_name_type"]
            in [
                ("S", "6TH", "ST"),
                ("", "MOSBY CREEK", "RD"),
                ("", "ROW RIVER", "RD"),
                ("", "SHOREVIEW", "DR"),
            ],
            # Special cases.
            (
                road["dir_name_type"] == ("", "COTTAGE GROVE LORANE", "RD")
                and not max(road["addresses"]) <= 27899
            ),
        ]
    ):
        return True

    if road["mailcity"] == "Creswell" and any(
        [
            # Verify by class + name.
            road["cclass"] <= CCLASS_CODE["minor_arterial"]
            and road["dir_name_type"]
            in [
                ("W", "OREGON", "AVE"),
            ],
            road["cclass"] <= CCLASS_CODE["major_collector"]
            and road["dir_name_type"]
            in [
                ("", "CAMAS SWALE", "RD"),
                ("", "HAMM", "RD"),
            ],
            # Special cases (currently none).
        ]
    ):
        return True

    if road["mailcity"] == "Dorena" and any(
        [
            # Verify by class + name.
            road["cclass"] <= CCLASS_CODE["major_collector"]
            and road["dir_name_type"]
            in [
                ("", "ROW RIVER", "RD"),
                ("", "SHOREVIEW", "DR"),
            ],
            # Special cases (currently none).
        ]
    ):
        return True

    if road["mailcity"] == "Eugene" and any(
        [
            # Verify by class + name.
            road["cclass"] <= CCLASS_CODE["minor_arterial"]
            and road["dir_name_type"]
            in [
                ("E", "6TH", "AVE"),
                ("W", "6TH", "AVE"),
                ("E", "7TH", "AVE"),
                ("W", "7TH", "AVE"),
                ("E", "13TH", "AVE"),
                ("E", "11TH", "AVE"),
                ("W", "11TH", "AVE"),
                ("E", "18TH", "AVE"),
                ("W", "18TH", "AVE"),
                ("E", "24TH", "AVE"),
                ("E", "29TH", "AVE"),
                ("", "AGATE", "ST"),
                ("", "AIRPORT", "RD"),
                ("E", "AMAZON", "DR"),
                ("", "AMAZON", "PKWY"),
                ("N", "BERTELSEN", "RD"),
                ("S", "BERTELSEN", "RD"),
                ("", "CAL YOUNG", "RD"),
                ("", "CHAMBERS", "ST"),
                ("", "CRESCENT", "AVE"),
                ("", "FERRY STREET BRIDGE", ""),
                ("", "GLENWOOD", "BLVD"),
                ("", "GOODPASTURE ISLAND", "RD"),
                ("", "GREEN ACRES", "RD"),
                ("", "HILYARD", "ST"),
                ("", "IRVINGTON", "DR"),
                ("", "NORKENZIE", "RD"),
                ("", "PEARL", "ST"),
                ("", "RIVER", "RD"),
                ("", "SENECA", "RD"),
                ("N", "SENECA", "RD"),
                ("", "VALLEY RIVER", "DR"),
                ("", "VAN DUYN", "RD"),
                ("", "WILLAMETTE", "ST"),
                ("", "WILLOW CREEK", "RD"),
            ],
            road["cclass"] <= CCLASS_CODE["major_collector"]
            and road["dir_name_type"]
            in [
                ("", "ALVADORE", "RD"),
                ("", "GREEN HILL", "RD"),
                ("", "HAMM", "RD"),
            ],
            road["cclass"] <= CCLASS_CODE["minor_collector"]
            and road["dir_name_type"]
            in [
                # ("W", "1ST", "AVE"),
                # ("W", "7TH", "PL"),
                # ("E", "40TH", "AVE"),
                ("", "ARTHUR", "ST"),
                ("", "BAILEY HILL", "RD"),
                ("", "CLEAR LAKE", "RD"),
                ("", "CROW", "RD"),
                ("N", "DELTA", "HWY"),
                ("", "DILLARD", "RD"),
                # ("", "DONALD", "ST"),
                ("", "FISHER", "RD"),
                ("N", "GAME FARM", "RD"),
                ("", "GARFIELD", "ST"),
                ("N", "GARFIELD", "ST"),
                ("", "GIMPL HILL", "RD"),
                ("", "FOX HOLLOW", "RD"),
                ("", "LORANE", "HWY"),
                ("", "MCKENZIE VIEW", "DR"),
                ("", "PRAIRIE", "RD"),
                # ("", "RAILROAD", "BLVD"),
                ("", "ROYAL", "AVE"),
                ("S", "WILLAMETTE", "ST"),
            ],
            # Special cases.
            (
                road["dir_name_type"] == ("W", "13TH", "AVE")
                and not max(road["addresses"]) >= 2000
            ),
            (
                road["dir_name_type"] == ("W", "28TH", "AVE")
                and road["cclass"] == CCLASS_CODE["minor_arterial"]
                and not any_in_range([road["r_ladd"], road["r_hadd"]], 501, 519)
            ),
            (
                road["dir_name_type"] == ("W", "29TH", "AVE")
                and road["cclass"] == CCLASS_CODE["minor_arterial"]
                and not any_in_range([road["l_ladd"], road["l_hadd"]], 494, 494)
            ),
            (
                road["dir_name_type"] == ("E", "30TH", "AVE")
                and not max(road["addresses"]) <= 699
            ),
            (
                road["dir_name_type"] == ("", "COBURG", "RD")
                and not any_in_range(road["addresses"], 7, 7)
            ),
            (
                road["dir_name_type"] == ("", "CREST", "DR")
                and not any_in_range(road["addresses"], 600, 699)
            ),
            (
                road["dir_name_type"] == ("", "JEFFERSON", "ST")
                and road["cclass"] <= CCLASS_CODE["major_collector"]
                and not any_in_range(road["addresses"], 100, 699)
                # and not any_in_range(road["addresses"], 2848, 2850)
            ),
            (
                road["dir_name_type"] == ("", "WASHINGTON", "ST")
                and road["cclass"] <= CCLASS_CODE["minor_arterial"]
                and not any_in_range(road["addresses"], 2800, 2839)
                and not any_in_range(road["addresses"], 2848, 2850)
            ),
            (
                road["dir_name_type"] == ("", "WILLAGILLESPIE", "RD")
                and road["cclass"] <= CCLASS_CODE["minor_arterial"]
                and not max(road["addresses"]) <= 1099
            ),
        ]
    ):
        return True

    if road["mailcity"] == "Fall Creek" and any(
        [
            # Verify by class + name.
            road["cclass"] <= CCLASS_CODE["major_collector"]
            and road["dir_name_type"]
            in [
                ("", "JASPER LOWELL", "RD"),
                ("", "PENGRA", "RD"),
            ],
            # Special cases (currently none).
        ]
    ):
        return True

    if road["mailcity"] == "Florence" and any(
        [
            # Verify by class + name.
            road["cclass"] <= CCLASS_CODE["minor_arterial"]
            and road["dir_name_type"]
            in [
                ("", "9TH", "ST"),
            ],
            road["cclass"] <= CCLASS_CODE["major_collector"]
            and road["dir_name_type"]
            in [
                ("", "CANARY", "RD"),
                ("", "CLEAR LAKE", "RD"),
                ("", "HECETA BEACH", "RD"),
                ("", "MUNSEL LAKE", "RD"),
                ("", "RHODODENDRON", "DR"),
                ("N", "RHODODENDRON", "DR"),
            ],
            # Special cases.
            (
                road["dir_name_type"] == ("", "35TH", "ST")
                and not max(road["addresses"]) >= 1800
            ),
        ]
    ):
        return True

    if road["mailcity"] == "Harrisburg" and any(
        [
            # Verify by class + name.
            road["cclass"] <= CCLASS_CODE["major_collector"]
            and road["dir_name_type"]
            in [
                ("", "COBURG", "RD"),
            ],
            # Special cases (currently none).
        ]
    ):
        return True

    if road["mailcity"] == "Jasper" and any(
        [
            # Verify by class + name.
            road["cclass"] <= CCLASS_CODE["major_collector"]
            and road["dir_name_type"]
            in [
                ("", "JASPER LOWELL", "RD"),
                ("", "PENGRA", "RD"),
            ],
            # Special cases (currently none).
        ]
    ):
        return True

    if road["mailcity"] == "Junction City" and any(
        [
            # Verify by class + name.
            road["cclass"] <= CCLASS_CODE["minor_arterial"]
            and road["dir_name_type"]
            in [
                ("", "RIVER", "RD"),
            ],
            road["cclass"] <= CCLASS_CODE["major_collector"]
            and road["dir_name_type"]
            in [
                ("", "ALVADORE", "RD"),
                ("", "HIGH PASS", "RD"),
                ("", "OAKLEA", "DR"),
            ],
            road["cclass"] <= CCLASS_CODE["minor_collector"]
            and road["dir_name_type"]
            in [
                ("", "CLEAR LAKE", "RD"),
                ("E", "1ST", "AVE"),
                ("W", "1ST", "AVE"),
                ("", "MEADOWVIEW", "RD"),
            ],
            # Special cases.
            (
                road["dir_name_type"] == ("W", "6TH", "AVE")
                and not max(road["addresses"]) <= 399
            ),
            (
                road["dir_name_type"] == ("W", "10TH", "AVE")
                and road["cclass"] <= CCLASS_CODE["major_collector"]
                and not max(road["addresses"]) <= 399
            ),
            (
                road["dir_name_type"] == ("", "PRAIRIE", "RD")
                and road["cclass"] <= CCLASS_CODE["major_collector"]
                and not any_in_range([road["r_ladd"], road["r_hadd"]], 93225, 93317)
            ),
        ]
    ):
        return True

    if road["mailcity"] == "Lorane" and any(
        [
            # Verify by class + name.
            road["cclass"] <= CCLASS_CODE["major_collector"]
            and road["dir_name_type"]
            in [
                ("", "SIUSLAW", "RD"),
            ],
            # Special cases (currently none).
        ]
    ):
        return True

    if road["mailcity"] == "Lowell" and any(
        [
            # Verify by class + name.
            road["cclass"] <= CCLASS_CODE["minor_arterial"]
            and road["dir_name_type"]
            in [
                ("", "NORTH SHORE", "DR"),
                ("E", "NORTH SHORE", "DR"),
            ],
            road["cclass"] <= CCLASS_CODE["major_collector"]
            and road["dir_name_type"]
            in [
                ("", "JASPER LOWELL", "RD"),
                ("", "PENGRA", "RD"),
                ("S", "PIONEER", "ST"),
            ],
            # Special cases.
            (
                road["dir_name_type"] == ("N", "MOSS", "ST")
                and road["cclass"] <= CCLASS_CODE["minor_arterial"]
                and not max(road["addresses"]) <= 99
            ),
        ]
    ):
        return True

    if road["mailcity"] == "Marcola" and any(
        [
            # Verify by class + name.
            road["cclass"] <= CCLASS_CODE["minor_collector"]
            and road["dir_name_type"]
            in [
                ("", "MARCOLA", "RD"),
            ],
            # Special cases (currently none).
        ]
    ):
        return True

    if road["mailcity"] == "Noti" and any(
        [
            # Verify by class + name.
            road["cclass"] <= CCLASS_CODE["major_collector"]
            and road["dir_name_type"]
            in [
                ("", "NOTI", "LP"),
                ("", "POODLE CREEK", "RD"),
            ],
            # Special cases (currently none).
        ]
    ):
        return True

    if road["mailcity"] == "Oakridge" and any(
        [
            # Verify by class + name.
            road["cclass"] <= CCLASS_CODE["major_collector"]
            and road["dir_name_type"]
            in [
                ("E", "1ST", "ST"),
                ("E", "2ND", "ST"),
                ("", "ASH", "ST"),
                ("", "AUFDERHEIDE", "DR"),
                ("", "BEECH", "ST"),
                ("", "CRESTVIEW", "ST"),
                ("", "HILLS", "ST"),
                ("", "ROSE", "ST"),
                ("", "WESTOAK", "RD"),
            ],
            road["cclass"] <= CCLASS_CODE["minor_collector"]
            and road["dir_name_type"]
            in [
                ("", "HIGH PRAIRIE", "RD"),
                ("", "HUCKLEBERRY", "RD"),
            ],
            # Special cases (currently none).
        ]
    ):
        return True

    if road["mailcity"] == "Springfield" and any(
        [
            # Verify by class + name.
            road["cclass"] <= CCLASS_CODE["minor_arterial"]
            and road["dir_name_type"]
            in [
                ("", "14TH", "ST"),
                ("S", "14TH", "ST"),
                ("", "42ND", "ST"),
                ("S", "42ND", "ST"),
                ("", "BELTLINE", "RD"),
                ("", "CENTENNIAL", "BLVD"),
                ("W", "CENTENNIAL", "BLVD"),
                ("", "GATEWAY", "ST"),
                ("", "HARLOW", "RD"),
                ("", "MAIN", "ST"),
                ("", "MOHAWK", "BLVD"),
            ],
            road["cclass"] <= CCLASS_CODE["major_collector"]
            and road["dir_name_type"]
            in [
                ("", "5TH", "ST"),
                ("", "19TH", "ST"),
                ("", "28TH", "ST"),
                ("S", "28TH", "ST"),
                ("S", "32ND", "ST"),
                ("", "48TH", "ST"),
                ("", "52ND", "ST"),
                ("", "58TH", "ST"),
                ("", "CAMP CREEK", "RD"),
                ("", "G", "ST"),
                ("", "HAYDEN BRIDGE", "RD"),
                ("", "HAYDEN BRIDGE", "WAY"),
                ("", "HIGH BANKS", "RD"),
                ("", "MARCOLA", "RD"),
            ],
            road["cclass"] <= CCLASS_CODE["minor_collector"]
            and road["dir_name_type"]
            in [
                ("", "HILL", "RD"),
                ("", "MCKENZIE VIEW", "DR"),
                ("", "OLD MOHAWK", "RD"),
                ("", "THURSTON", "RD"),
            ],
            # Special cases (currently none).
        ]
    ):
        return True

    if road["mailcity"] == "Veneta" and any(
        [
            # Verify by class + name.
            road["cclass"] <= CCLASS_CODE["major_collector"]
            and road["dir_name_type"]
            in [
                ("", "BOLTON HILL", "RD"),
                ("", "NOTI", "LP"),
                ("", "SIUSLAW", "RD"),
                ("", "VAUGHN", "RD"),
                ("", "WOLF CREEK", "RD"),
            ],
            # Special cases (currently none).
        ]
    ):
        return True

    if road["mailcity"] == "Walton" and any(
        [
            # Verify by class + name.
            road["cclass"] <= CCLASS_CODE["major_collector"]
            and road["dir_name_type"]
            in [
                ("", "SIUSLAW", "RD"),
            ],
            # Special cases (currently none).
        ]
    ):
        return True

    if road["mailcity"] == "Westfir" and any(
        [
            # Verify by class + name.
            road["cclass"] <= CCLASS_CODE["major_collector"]
            and road["dir_name_type"]
            in [
                ("", "AUFDERHEIDE", "DR"),
                ("", "WESTFIR-OAKRIDGE", "RD"),
                ("", "WESTOAK", "RD"),
            ],
            # Special cases (currently none).
        ]
    ):
        return True

    if road["mailcity"] == "Westlake" and any(
        [
            # Verify by class + name.
            road["cclass"] <= CCLASS_CODE["major_collector"]
            and road["dir_name_type"]
            in [
                ("", "CANARY", "RD"),
                ("S", "CANARY", "RD"),
            ],
            # Special cases (currently none).
        ]
    ):
        return True

    return False


def name_expand_abbreviations(name):
    """Returns road name with expanded abbreviations.

    Args:
        name (str): Road name.

    Returns:
        str
    """
    if name:
        for abbreviation, expansion in ABBREVIATIONS_TO_EXPAND.items():
            if abbreviation not in name:
                continue

            i = len(abbreviation)
            if name.startswith(abbreviation + " "):
                name = expansion + name[i:]
            if name.endswith(" " + abbreviation):
                name = name[:-i] + expansion
            # Need the while: replace won"t share spaces between substrings.
            while " {} ".format(abbreviation) in name:
                name = name.replace(
                    " {} ".format(abbreviation), " {} ".format(expansion)
                )
    return name


def name_fix_title_case(name):
    """Return road name with title-casing fixed.

    Args:
        name (str): Road name.

    Returns:
        str
    """
    for abbreviation in ABBREVIATIONS_TO_LEAVE:
        cased_abbreviation = abbreviation.title()
        if cased_abbreviation not in name:
            continue

        i = len(abbreviation)
        if name.startswith(cased_abbreviation + " "):
            name = abbreviation + name[i:]
        if name.endswith(" " + cased_abbreviation):
            name = name[:-i] + abbreviation
        # Need the while: replace won"t share spaces between found substrings.
        while " {} ".format(cased_abbreviation) in name:
            name = name.replace(
                " {} ".format(cased_abbreviation), " {} ".format(abbreviation)
            )
    # Fix road names with some Gaelic prefixes (e.g. "Mc", "O")."""
    if name and "Mc" in name:
        i = name.find("Mc") + len("Mc")
        name = name[:i] + name[i].upper() + name[i + 1 :]
    return name


def optimal_time_cost(**road_attributes):
    """Return edge traversal time-cost in seconds for network routing.

    Args:
        **road_attributes (dict): Mapping of attribute name to value.

    Returns:
        float
    """
    road = copy(road_attributes)
    # Cannot have zero division in networks, so set a default speed if none.
    if not road["speed"]:
        road["speed"] = 55 if road["cclass"] > 500 else 25
    mph_to_fps = 5280.0 / 3600.0
    return road["shape@length"] / (road["speed"] * mph_to_fps)


def rlid_road_name(direction_code, street_name, type_code):
    """Return RLID-formatted string of full road name.

    RLID street name style is title-cased, but contains some custom exclusions and
    alterations.

    Args:
        direction_code (str): Direction code prefix.
        street_name (str): Street name.
        type_code (str): Type code suffix.

    Returns:
        str
    """
    # Unnamed roads remain so in RLID ("Private Road" is a filler name).
    # RLID uses empty string instead of null/none.
    if street_name.upper() in PRIVATE_ROAD_NAMES:
        return ""

    street_name = name_expand_abbreviations(street_name)
    # Change name & type to title case (directions are acronyms).
    street_name = name_fix_title_case(street_name.title())
    type_code = type_code.title()
    # Fix ordinals.
    for end in ORDINAL_ENDS:
        cased = end.title()
        if cased in street_name:
            street_name = street_name.replace(cased, end)
    # Fix special cases.
    # RiverBend Dr has mid-word capital (Riverbend Ave does not).
    if street_name == "Riverbend" and type_code == "Dr":
        street_name = "RiverBend"
    return concatenate_arguments(direction_code, street_name, type_code).strip()


def road_intersections():
    """Generate mapping of road intersection attribute name to value.

    Yields:
        dict
    """

    def init_node(geometry):
        """Return initialized node dictionary."""
        return {
            "zlevs": set(),
            # Members: (<z_level>, (<dir>, <name>, <type>))
            "level_names": set(),
            "shape@": geometry,
        }

    road_segments = arcetl.attributes.as_dicts(
        dataset_path=dataset.ROAD.path("pub"),
        field_names=[
            "dir",
            "name",
            "type",
            "fnode",
            "tnode",
            "f_zlev",
            "t_zlev",
            "shape@",
        ],
        dataset_where_sql="county = 'Lane'",
    )
    node_info = {}
    for segment in road_segments:
        for end, point_attribute in [("f", "firstPoint"), ("t", "lastPoint")]:
            node_id = segment["{}node".format(end)]
            zlev = segment["{}_zlev".format(end)]
            if node_id not in node_info:
                node_info[node_id] = init_node(
                    geometry=getattr(segment["shape@"], point_attribute)
                )
            node_info[node_id]["zlevs"].add(zlev)
            node_info[node_id]["level_names"].add(
                (zlev, (segment["dir"], segment["name"], segment["type"]))
            )
    for node_id, info in node_info.items():
        for (zlev01, road01), (zlev02, road02) in combinations(info["level_names"], 2):
            intersection = {
                "nodeid": node_id,
                "shape@": info["shape@"],
                "numzlevs": len(info["zlevs"]),
                "zlev01": zlev01,
                "zlev02": zlev02,
                "dir01": road01[0],
                "name01": road01[1],
                "type01": road01[2],
                "dir02": road02[0],
                "name02": road02[1],
                "type02": road02[2],
            }
            yield intersection


# ETLs.


def bike_facility_etl():
    """Run ETL for bike facilities."""
    with arcetl.ArcETL("Bike Facilities") as etl:
        etl.extract(dataset.BIKE_FACILITY.path("maint"))
        # Clean maintenance values.
        transform.clean_whitespace(
            etl,
            field_names=[
                "name",
                "ftype",
                "lane_type",
                "status",
                "maint",
                "source",
                "easement",
                "lane_placement",
            ],
        )
        transform.force_uppercase(etl, field_names=["maint", "source"])
        transform.clear_nonpositive(
            etl, field_names=["bike_segid", "eug_id", "lane_num"]
        )
        transform.add_missing_fields(etl, dataset.BIKE_FACILITY, tags=["pub"])
        # Assign description values from domains.
        domain_kwargs = [
            {
                "field_name": "ftypedes",
                "code_field_name": "ftype",
                "domain_name": "BikeFType2",
            },
            {
                "field_name": "lane_typedes",
                "code_field_name": "lane_type",
                "domain_name": "BikeLaneType2",
            },
        ]
        for kwargs in domain_kwargs:
            etl.transform(
                arcetl.attributes.update_by_domain_code,
                domain_workspace_path=database.REGIONAL.path,
                **kwargs
            )
        etl.load(dataset.BIKE_FACILITY.path("pub"))


def bridge_etl():
    """Run ETL for bridges."""
    with arcetl.ArcETL("Bridges") as etl:
        etl.extract(dataset.BRIDGE.path("maint"))
        # Clean maintenance values.
        transform.clean_whitespace(
            etl,
            field_names=[
                "bridge_id",
                "local_name",
                "covered_bridge",
                "route_id",
                "route_seq",
                "source",
                "comments",
            ],
        )
        transform.force_uppercase(etl, field_names=["bridge_id", "covered_bridge"])
        transform.clear_nonpositive(etl, field_names=["lifeline", "psap_id"])
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="angle",
            function=(lambda x: x if x else 0),
        )
        # Set default maintenance values where missing.
        transform.force_yn(etl, field_names=["covered_bridge"], default="N")
        transform.add_missing_fields(etl, dataset.BRIDGE, tags=["pub"])
        # Transfer values to publication field names.
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="state_bridge_id",
            function=(lambda x: x),
            field_as_first_arg=False,
            arg_field_names=["bridge_id"],
        )
        etl.load(dataset.BRIDGE.path("pub"))


def road_etl():
    """Run ETL for roads."""
    with arcetl.ArcETL("Roads") as etl:
        etl.extract(dataset.ROAD.path("maint"))
        # Make mainteance fields nullable.
        for field in (
            field
            for field in dataset.ROAD.fields
            if field["name"] in ["l_ladd", "l_hadd", "r_ladd", "r_hadd", "ugbcity"]
        ):
            temp_field = dict(field)
            temp_field.update({"name": field["name"] + "_temp", "is_nullable": True})
            etl.transform(
                arcetl.dataset.add_field_from_metadata, add_metadata=temp_field
            )
            etl.transform(
                arcetl.attributes.update_by_function,
                field_name=temp_field["name"],
                function=(lambda x: x),
                field_as_first_arg=False,
                arg_field_names=[field["name"]],
            )
            etl.transform(arcetl.dataset.delete_field, field_name=field["name"])
            etl.transform(
                arcetl.dataset.rename_field,
                field_name=temp_field["name"],
                new_field_name=field["name"],
            )
        # Clean maintenance values.
        transform.clean_whitespace(
            etl,
            field_names=[
                "mailcity",
                "county",
                "fclass",
                "fed_class",
                "airsclass",
                "paved",
                "owner",
                "maint",
                "source",
                "method",
                "contributor",
                "ugbcity",
                "one_way",
            ],
        )
        for name in ["airsname", "dir", "name", "type"]:
            etl.transform(
                arcetl.attributes.update_by_function,
                field_name=name,
                function=clean_whitespace_without_clear,
            )
        transform.force_uppercase(
            etl,
            field_names=[
                "airsname",
                "dir",
                "name",
                "type",
                "fclass",
                "airsclass",
                "paved",
                "owner",
                "maint",
                "source",
                "method",
                "contributor",
                "ugbcity",
                "one_way",
            ],
        )
        transform.clear_nonpositive(
            etl,
            field_names=[
                "seg_id",
                "lcpwid",
                "eugid",
                "sprid",
                "lcogid",
                "l_ladd",
                "l_hadd",
                "r_ladd",
                "r_hadd",
                "speed",
                "snowroute",
                "f_zlev",
                "t_zlev",
            ],
        )
        fix_invalid_kwargs = [
            {
                "field_name": "ugbcity",
                "function": (lambda x: None if x == "OUT" else x),
            },
            {
                "field_name": "dir",
                "function": (
                    lambda val, name: "" if name in ["", "PRIVATE ROAD"] else val
                ),
                "field_as_first_arg": True,
                "arg_field_names": ["name"],
            },
            {
                "field_name": "type",
                "function": (
                    lambda val, name: "" if name in ["", "PRIVATE ROAD"] else val
                ),
                "field_as_first_arg": True,
                "arg_field_names": ["name"],
            },
        ]
        transform.update_attributes_by_functions(etl, fix_invalid_kwargs)
        transform.add_missing_fields(etl, dataset.ROAD, tags=["pub"])
        # Assign overlays.
        overlay_kwargs = [
            {
                "field_name": "zipcode",
                "overlay_field_name": "zipcode",
                "overlay_dataset_path": dataset.ZIP_CODE_AREA.path("pub"),
            }
        ]
        for kwargs in overlay_kwargs:
            etl.transform(
                arcetl.attributes.update_by_overlay,
                overlay_central_coincident=True,
                **kwargs
            )
        # Assign constants.
        etl.transform(arcetl.attributes.update_by_value, field_name="state", value="OR")
        # Build values from functions.
        function_kwargs = [
            {
                "field_name": "rlidname",
                "function": rlid_road_name,
                "field_as_first_arg": False,
                "arg_field_names": ["dir", "name", "type"],
            },
            {
                "field_name": "cclass",
                "function": cartographic_class,
                "field_as_first_arg": False,
                "kwarg_field_names": [
                    "ugbcity",
                    "dir",
                    "name",
                    "type",
                    "fclass",
                    "owner",
                    "paved",
                    "lcpwid",
                    "l_ladd",
                    "l_hadd",
                    "r_ladd",
                    "r_hadd",
                ],
            },
            {
                "field_name": "optcost",
                "function": optimal_time_cost,
                "field_as_first_arg": False,
                "kwarg_field_names": ["speed", "cclass", "shape@length"],
            },
        ]
        transform.update_attributes_by_functions(etl, function_kwargs)
        etl.transform(
            arcetl.attributes.update_by_node_ids,
            from_id_field_name="fnode",
            to_id_field_name="tnode",
        )
        ##TODO: Deprecate LTD costs.
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="ltd_a_cost",
            function=(
                lambda o, s: o
                * {
                    5: 6.25,
                    10: 6.25,
                    15: 6.25,
                    20: 5.56,
                    25: 5.21,
                    30: 5.00,
                    35: 4.86,
                    40: 4.17,
                    45: 4.17,
                    50: 3.91,
                    55: 3.53,
                    60: 3.41,
                    65: 3.39,
                    70: 3.18,
                    75: 3.13,
                }.get(s, 3.53)
            ),
            field_as_first_arg=False,
            arg_field_names=["optcost", "speed"],
        )
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="ltd_b_cost",
            function=(
                lambda o, s: o
                * {
                    5: 4.17,
                    10: 4.17,
                    15: 4.17,
                    20: 3.70,
                    25: 3.47,
                    30: 3.33,
                    35: 3.24,
                    40: 2.78,
                    45: 2.78,
                    50: 2.60,
                    55: 2.35,
                    60: 2.27,
                    65: 2.26,
                    70: 2.12,
                    75: 2.08,
                }.get(s, 2.35)
            ),
            field_as_first_arg=False,
            arg_field_names=["optcost", "speed"],
        )
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="ltd_c_cost",
            function=(
                lambda o, s: o
                * {
                    5: 3.13,
                    10: 3.13,
                    15: 3.13,
                    20: 2.78,
                    25: 2.60,
                    30: 2.50,
                    35: 2.43,
                    40: 2.08,
                    45: 2.08,
                    50: 1.95,
                    55: 1.76,
                    60: 1.70,
                    65: 1.69,
                    70: 1.59,
                    75: 1.56,
                }.get(s, 1.76)
            ),
            field_as_first_arg=False,
            arg_field_names=["optcost", "speed"],
        )
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="ltd_d_cost",
            function=(
                lambda o, s: o
                * {
                    5: 2.50,
                    10: 2.50,
                    15: 2.50,
                    20: 2.22,
                    25: 2.08,
                    30: 2.00,
                    35: 1.94,
                    40: 1.67,
                    45: 1.67,
                    50: 1.56,
                    55: 1.41,
                    60: 1.36,
                    65: 1.35,
                    70: 1.27,
                    75: 1.25,
                }.get(s, 1.41)
            ),
            field_as_first_arg=False,
            arg_field_names=["optcost", "speed"],
        )
        etl.load(dataset.ROAD.path("pub"))


def road_general_arterial_etl():
    """Run ETL for general arterial roads."""
    with arcetl.ArcETL("General Arterial Roads") as etl:
        etl.extract(dataset.ROAD.path("pub"), extract_where_sql="county = 'Lane'")
        # Use temporary flag for general arterial identification.
        etl.transform(
            arcetl.dataset.add_field,
            field_name="is_general_arterial",
            field_type="short",
        )
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="is_general_arterial",
            function=is_general_arterial,
            field_as_first_arg=False,
            kwarg_field_names=[
                "dir",
                "name",
                "type",
                "mailcity",
                "cclass",
                "l_ladd",
                "l_hadd",
                "r_ladd",
                "r_hadd",
            ],
        )
        etl.transform(
            arcetl.features.delete, dataset_where_sql="is_general_arterial <> 1"
        )
        # Merge alike & adjacent centerlines.
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=dataset.ROAD_GENERAL_ARTERIAL.field_names,
            multipart=False,
            unsplit_lines=True,
            tolerance=TOLERANCE["xy"],
        )
        # Dissolve does not respect intersections. Planarize to recover breaks.
        etl.transform(arcetl.convert.planarize)
        etl.load(dataset.ROAD_GENERAL_ARTERIAL.path())


def road_highway_etl():
    """Run ETL for highways."""
    with arcetl.ArcETL("Highways") as etl:
        etl.extract(
            dataset.ROAD.path("pub"),
            extract_where_sql="county = 'Lane' and cclass in (100, 200)",
        )
        # Merge alike & adjacent centerlines.
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=dataset.ROAD_HIGHWAY.field_names,
            multipart=False,
            unsplit_lines=True,
            tolerance=TOLERANCE["xy"],
        )
        # Dissolve does not respect intersections. Planarize to recover breaks.
        etl.transform(arcetl.convert.planarize)
        etl.load(dataset.ROAD_HIGHWAY.path())


def road_intersection_etl():
    """Run ETL for road intersections."""
    with arcetl.ArcETL("Road Intersections") as etl:
        etl.init_schema(dataset.ROAD_INTERSECTION.path())
        init_keys = [
            "nodeid",
            "dir01",
            "name01",
            "type01",
            "dir02",
            "name02",
            "type02",
            "numzlevs",
            "zlev01",
            "zlev02",
            "shape@",
        ]
        etl.transform(
            arcetl.features.insert_from_dicts,
            insert_features=road_intersections,
            field_names=init_keys,
        )
        # Assign geometry attributes.
        coordinate_system_xy_keys = {
            2914: {"x": "xcoord", "y": "ycoord"},
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
            {
                "field_name": "inccity",
                "overlay_field_name": "inccityabbr",
                "overlay_dataset_path": dataset.INCORPORATED_CITY_LIMITS.path(),
            },
            {
                "field_name": "mailcity",
                "overlay_field_name": "mailcitycode",
                "overlay_dataset_path": dataset.ZIP_CODE_AREA.path("pub"),
            },
            {
                "field_name": "neighbor",
                "overlay_field_name": "NEIBORHD",
                "overlay_dataset_path": os.path.join(
                    path.REGIONAL_DATA,
                    "boundary\\districts\\eug\\Boundary.gdb",
                    "EugNeighborhoods",
                ),
            },
            {
                "field_name": "ugbcity",
                "overlay_field_name": "ugbcity",
                "overlay_dataset_path": dataset.UGB.path("pub"),
            },
        ]
        for kwargs in overlay_kwargs:
            etl.transform(
                arcetl.attributes.update_by_overlay,
                overlay_central_coincident=True,
                **kwargs
            )
        # Clean overlay values.
        transform.clean_whitespace(etl, field_names=["neighbor"])
        # Remove invalid overlay values.
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="neighbor",
            function=(lambda x: x if x and int(x) != 99 else None),
        )
        # Assign joinable field values after overlays.
        join_kwargs = [
            {
                "field_name": "inccity_name",
                "join_field_name": "inccityname",
                "join_dataset_path": dataset.INCORPORATED_CITY_LIMITS.path(),
                "on_field_pairs": [("inccity", "inccityabbr")],
            },
            {
                "field_name": "neighborhood_name",
                "join_field_name": "NAME",
                "join_dataset_path": os.path.join(
                    path.REGIONAL_DATA,
                    "boundary\\districts\\eug\\Boundary.gdb",
                    "EugNeighborhoods",
                ),
                "on_field_pairs": [("neighbor", "NEIBORHD")],
            },
            {
                "field_name": "ugbcity_name",
                "join_field_name": "ugbcityname",
                "join_dataset_path": dataset.UGB.path("pub"),
                "on_field_pairs": [("ugbcity", "ugbcity")],
            },
        ]
        for kwargs in join_kwargs:
            etl.transform(arcetl.attributes.update_by_joined_value, **kwargs)
        # Clean join values.
        transform.clean_whitespace(etl, field_names=["neighborhood_name"])
        # Build values from functions.
        function_kwargs = [
            {
                "field_name": "intersection_name",
                "function": intersection_name,
                "kwarg_field_names": [
                    "dir01",
                    "name01",
                    "type01",
                    "dir02",
                    "name02",
                    "type02",
                ],
            },
            {
                "field_name": "intersection_name_reverse",
                "function": functools.partial(intersection_name, reverse=True),
                "kwarg_field_names": [
                    "dir01",
                    "name01",
                    "type01",
                    "dir02",
                    "name02",
                    "type02",
                ],
            },
            {
                "field_name": "rlidname01",
                "function": rlid_road_name,
                "arg_field_names": ["dir01", "name01", "type01"],
            },
            {
                "field_name": "rlidname02",
                "function": rlid_road_name,
                "arg_field_names": ["dir02", "name02", "type02"],
            },
        ]
        for kwargs in function_kwargs:
            etl.transform(
                arcetl.attributes.update_by_function, field_as_first_arg=False, **kwargs
            )
        # Delete intersections where roads names are same, or any unnamed.
        etl.transform(
            arcetl.features.delete,
            dataset_where_sql=(
                "rlidname01 = rlidname02 or rlidname01 = '' or rlidname02 = ''"
            ),
        )
        etl.load(dataset.ROAD_INTERSECTION.path())


def simple_network_etl():
    """Run ETL for the "simple" network."""
    for edge_dataset in dataset.SIMPLE_NETWORK_EDGES:
        with arcetl.ArcETL(os.path.basename(edge_dataset.path("pub"))) as etl:
            etl.extract(edge_dataset.path("source"))
            etl.load(edge_dataset.path("pub"), use_edit_session=True)
    arcetl.network.build(dataset.SIMPLE_NETWORK.path())


# Jobs.


WEEKLY_JOB = Job(
    "Transportation_Datasets",
    etls=[
        # Pass 1.
        bike_facility_etl,
        bridge_etl,
        road_etl,
        # Pass 2.
        ##TODO: Skip until refactor/retire.
        # road_general_arterial_etl,
        road_highway_etl,
        road_intersection_etl,
        simple_network_etl,
    ],
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
