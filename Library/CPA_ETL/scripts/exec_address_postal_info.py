"""Execution code for address postal info processing.

##TODO:
* Delete SiteAddress_CASS table.
* Remove postal_info atrtributes from Addressing.dbo.SiteAddress (confer with Keith).
* Add sending a message that tells when zip-overlay doesn't align with mp4 zip?
    + msg conditions:
        - zip_code_overlay is None
        - zip_code is not None and zip_code != zip_code_overlay
        - exceptions: "1711 OAK ST 1, Eugene", "9412 HWY 126, Florence"
"""
import argparse
from collections import defaultdict
import logging
import os
import subprocess
import types

import arcetl
from etlassist.pipeline import Job, execute_pipeline

from helper import database
from helper import dataset
from helper import path
from helper.value import clean_whitespace, concatenate_arguments


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""

ALTERNATES = {
    "street_name": {
        "HWY 36": {"HIGHWAY 36"},
        "HWY 58": {"HIGHWAY 58"},
        "HWY 99": {"HIGHWAY 99", "PACIFIC"},
        "HWY 99E": {"HIGHWAY 99"},
        "HWY 99N": {"HIGHWAY 99"},
        "HWY 99S": {"HIGHWAY 99"},
        "HWY 99W": {"HIGHWAY 99"},
        "HWY 101": {"HIGHWAY 101"},
        "HWY 126": {"HIGHWAY 126"},
        "MOUNT ZION": {"MT ZION"},
        "R R ANDERSON": {"RR ANDERSON"},
        "ROYAL ST GEORGES": {"ROYAL SAINT GEORGES"},
        "SAINT KITTS": {"ST KITTS"},
    },
    "street_type_code": {"LP": {"LOOP"}},
    "unit_type_code": {None: {"#"}, "SPACE": {"SPC"}},
}
"""dict: Mapping of attribute to mapping of value to collection of alternate values."""
DESCRIPTION = {
    "address_type": {
        "F": "Firm (single business or organization)",
        "G": "General Delivery (general mail pickup)",
        "H": "High-rise (business park, plaza, and so on)",
        "P": "PO Box (post office box)",
        "R": "Rural Route/Highway",
        "S": "Street/Residential Address",
    },
    "lot_order": {"A": "Ascending", "D": "Descending"},
    "result": {
        # Status.
        "AS01": "Address matched to postal database.",
        "AS02": "Street address match.",
        "AS03": "Non-USPS address.",
        "AS09": "Foreign postal code detected",
        "AS10": "Address matched to CMRA.",
        "AS11": "PBSA (Post Office Box Street Address).",
        "AS12": "Record moved to a new address.",
        "AS13": "Address has been updated by LACSLink.",
        "AS14": "Suite appended by SuiteLink.",
        "AS15": "Suite appended by AddressPlus.",
        "AS16": "Address is vacant.",
        "AS17": "Alternate delivery.",
        "AS18": "DPV error.",
        "AS20": "This address is deliverable by USPS only.",
        "AS23": "Extraneous information found.",
        "AS24": "USPS Door Not Accessible.",
        # Error.
        "AE01": "ZIP code error.",
        "AE02": "Unknown street error.",
        "AE03": "Component mismatch error.",
        "AE04": "Non-deliverable address error.",
        "AE05": "Multiple match error.",
        "AE06": "Early Warning System error.",
        "AE07": "Minimum address input error.",
        "AE08": "Suite range invalid error.",
        "AE09": "Suite range missing error.",
        "AE10": "Primary range invalid error.",
        "AE11": "Primary range missing error.",
        "AE12": "PO, HC, or RR box number invalid error.",
        "AE13": "PO, HC, or RR box number missing error.",
        "AE14": "CMRA secondary missing error.",
        "AE15": "Demo mode.",
        "AE16": "Expired database.",
        "AE17": "Suite range extraneous error.",
        # Change.
        "AC01": "ZIP code change.",
        "AC02": "State change.",
        "AC03": "City change.",
        "AC04": "Base/alternate change.",
        "AC05": "Alias change.",
        "AC06": "Address1/address2 swap.",
        "AC07": "Address1/company swap.",
        "AC08": "Plus 4 change.",
        "AC09": "Dependent locality (urbanization) change.",
        "AC10": "Street name change.",
        "AC11": "Suffix change.",
        "AC12": "Street directional change.",
        "AC13": "Suite name change.",
        "AC14": "Sub premise number change.",
        "AC15": "Double dependent locality change",
        "AC16": "Sub-administrative area change",
        "AC17": "Subnational Area Change",
        "AC18": "PO Box change",
        "AC19": "Premise type change",
        "AC20": "House number change",
        "AC22": "Organization change",
    },
    "zip_type": {
        None: "Standard",
        "M": "Military/APO",
        "P": "PO Box",
        "U": "Unique (assigned to specific organizations)",
    },
}
"""dict: Mapping of code type to mapping of code to its description."""
FLAG_KEYS = [
    "is_matched",
    "is_matched_no_unit",
    "is_cmra",
    "is_vacant",
    "has_mail_service",
]
"""list: Collection of keys for boolean "flag" attributes."""
INFO_KEYS = [
    "address_type",
    "address_type_desc",
    "zip_code",
    "plus_four_code",
    "zip_type",
    "zip_type_desc",
    "delivery_point_code",
    "carrier_route",
    "lot_number",
    "lot_order",
]
"""list: Collections of keys for postal info attributes."""
VALID_ADDRESS_WHERE_SQL = """
    site_address_gfid is not null
    and geofeat_id is not null
    and house_nbr is not null
    and valid = 'Y'
    and archived = 'N'
"""
"""str: SQL statement where-clause for filtering addresses."""


# Helpers.


def _neg_match_keys(address):
    """Update dictionary with cleared keys related to matching.

    Args:
        address (dict): Mapping of address atttributes, including the result_code
            attribute.

    Returns:
        dict: Updated mapping including output attributes.
    """
    addr = address.copy()
    for key in FLAG_KEYS:
        addr[key] = False if "matched" in key else None
    for key in INFO_KEYS:
        addr[key] = None
    return addr


def address_3rd_party_usps_rows():
    """Generate postal information table rows from the Mailers+4 workfile.

    Yields:
        dict: Postal information for given address.
    """
    keys = {
        # Use Postal Info keys, so update_address_from_codes can run on them.
        "info": [name.lower() for name in dataset.ADDRESS_POSTAL_INFO.field_names],
        "workfile": [
            name.lower() for name in dataset.ADDRESS_WORKFILE_3RD_PARTY.fields
        ],
    }
    addresses = arcetl.attributes.as_dicts(
        dataset.ADDRESS_WORKFILE_3RD_PARTY.path(), field_names=keys["workfile"]
    )
    for addr in addresses:
        for key in addr:
            if isinstance(addr[key], (str, type(u""))):
                addr[key] = clean_whitespace(addr[key])
        for key in keys["info"]:
            addr.setdefault(key)
        if not addr["result_code"]:
            addr["issues"] = "NO RESULT CODES FROM ADDRESS CHECK (LIKELY APP FAILURE)."
        addr = update_address_from_codes(addr)
        # Rename keys & convert attributes.
        for name, old_name in [("address", "concat_address"), ("city", "city_name")]:
            addr[name] = addr.pop(old_name)
        for tag, old_key in [
            ("exact", "is_matched"),
            ("ignore_unit", "is_matched_no_unit"),
        ]:
            addr["exists_usps_" + tag] = addr.pop(old_key) == "Y"
        yield addr


def address_zip_overlay_map():
    """Return mapping of address IDs to ZIP code based on an overlay.

    Returns:
        dict: Address ID to ZIP code mapping.
    """
    address_copy = arcetl.TempDatasetCopy(
        dataset.SITE_ADDRESS.path("maint"),
        VALID_ADDRESS_WHERE_SQL,
        field_names=[
            "geofeat_id",
            "house_nbr",
            "pre_direction_code",
            "street_name",
            "street_type_code",
            "city_name",
        ],
    )
    with address_copy:
        arcetl.dataset.add_field(
            address_copy.path,
            field_name="zip_overlay",
            field_type="text",
            field_length=5,
            log_level=None,
        )
        arcetl.attributes.update_by_overlay(
            address_copy.path,
            field_name="zip_overlay",
            overlay_dataset_path=dataset.ZIP_CODE_AREA.path("maint"),
            overlay_field_name="zipcode",
            log_level=None,
        )
        # Postal sorting center in Springfield has its own ZIP code.
        arcetl.attributes.update_by_value(
            address_copy.path,
            field_name="zip_overlay",
            value="97475",
            dataset_where_sql="""
                house_nbr = 3148
                and pre_direction_code is null
                and street_name = 'GATEWAY'
                and street_type_code = 'ST'
                and city_name = 'Springfield'
            """,
        )
        zip_overlay_map = defaultdict(
            None,
            arcetl.attributes.as_iters(
                address_copy.path, field_names=["geofeat_id", "zip_overlay"]
            ),
        )
        return zip_overlay_map


def check_addresses(parameters_path):
    """Run Mailers+4 Address Check using given parameters file.

    Args:
        parameters_path (str): Path of the parameters file.
    """
    LOG.info(
        "Start: Run Mailers+4 Address Check using parameters file %s.", parameters_path
    )
    subprocess.check_call([path.MAILERSPLUS4, parameters_path])
    LOG.info("End: Process.")


def postal_info_rows():
    """Generate postal information table rows from the Mailers+4 workfile.

    Yields:
        dict: Postal information for given address.
    """
    keys = {
        "info": [name.lower() for name in dataset.ADDRESS_POSTAL_INFO.field_names],
        "workfile": [name.lower() for name in dataset.ADDRESS_WORKFILE.field_names],
    }
    addresses = arcetl.attributes.as_dicts(
        dataset.ADDRESS_WORKFILE.path(), field_names=keys["workfile"]
    )
    zip_code_overlay = address_zip_overlay_map()
    for addr in addresses:
        for key in addr:
            if isinstance(addr[key], (str, type(u""))):
                addr[key] = clean_whitespace(addr[key])
        for key in keys["info"]:
            addr.setdefault(key)
        if not addr["result_code"]:
            addr["issues"] = "NO RESULT CODES FROM ADDRESS CHECK (LIKELY APP FAILURE)."
        addr = update_address_from_codes(addr)
        # Add extra attributes.
        addr["zip_code_overlay"] = zip_code_overlay[addr["geofeat_id"]]
        for key in ["address_type", "zip_type"]:
            addr[key + "_desc"] = DESCRIPTION[key].get(addr[key])
        if not addr["zip_code"]:
            addr["zip_type_desc"] = None
        yield addr


def update_address_from_codes(address):
    """Update dictionary with attributes based on result codes.

    Address Check status result codes referenced from  Mailers+4 User's Guide, Appendix
    B (found at `{program folder}/docs/mp4man.pdf`).

    User's Guide is missing some codes. Also refer to:
    http://wiki.melissadata.com/index.php?title=Result_Codes

    Args:
        address (dict): Mapping of address atttributes, including the result_code
            attribute.

    Returns:
        dict: Updated mapping including output attributes.
    """
    if not address["result_code"]:
        return address

    addr = address.copy()
    addr["is_matched"] = "AS01" in addr["result_code"]
    addr["is_matched_no_unit"] = any(
        ["AS01" in addr["result_code"], "AS02" in addr["result_code"]]
    )
    # Only assign values for other status flags if address was matched.
    if any([addr["is_matched"], addr["is_matched_no_unit"]]):
        addr["is_cmra"] = "AS10" in addr["result_code"]
        addr["is_vacant"] = "AS16" in addr["result_code"]
        addr["has_mail_service"] = not "AS17" in addr["result_code"]
    tag_code_issue = {
        "status": {
            "AS01": None,
            "AS02": None,
            "AS03": "exists but not serviced by USPS",
            "AS09": "foreign postal code detected",
            "AS10": None,
            "AS11": None,
            "AS12": None,
            # We shouldn"t have rural route addresses.
            "AS13": None,
            # AS14/15 only happen when supplying a company or person's name.
            "AS14": None,
            "AS15": None,
            "AS16": None,
            "AS17": None,
            "AS18": None,
            "AS20": None,
            "AS23": "claims extraneous information found (likely sub-unit)",
            "AS24": None,
        },
        "errors": {
            "AE01": "ZIP code does not exist/uncorrectable",
            "AE02": "street cannot be matched",
            "AE03": "too many address component mismatches",
            "AE04": "non-deliverable (no homes in street range)",
            "AE05": "multiple valid matches possible",
            "AE06": "USPS Early Warning System: will be in next postal data update",
            "AE07": "minimum required address components missing",
            "AE08": "unit ID invalid (cannot fully match with unit)",
            "AE09": "unit ID missing (cannot fully match without unit)",
            "AE10": "house number invalid",
            "AE11": "house number missing",
            # AE12-14: we do not manage box numbers in our addresses.
            "AE12": None,
            "AE13": None,
            "AE14": None,
            "AE17": "extraneous unit",
        },
        "changes": {
            # Ignore, since we have null ZIPs initially.
            "AC01": None,
            "AC02": """state_code = "OR" --> "{state_code}" (based on city & ZIP)""",
            "AC03": (
                """city_name = "{city_name}" --> "{mp4_city_name}" (based on ZIP)"""
            ),
            "AC04": "alternate address to base USPS address",
            "AC05": update_from_code_ac05,
            "AC10": update_from_code_ac10,
            "AC11": update_from_code_ac11,
            "AC12": update_from_code_ac12,
            "AC13": update_from_code_ac13,
            "AC14": update_from_code_ac14,
            "AC20": 'house_nbr = "{house}" --> "{mp4_house}"',
        },
    }
    result_codes = set(addr["result_code"].split(","))
    for tag in ["status", "errors", "changes"]:
        code_issue = tag_code_issue[tag]
        if result_codes.isdisjoint(code_issue):
            continue

        addr["issue_" + tag] = []
        for code in sorted(result_codes.intersection(code_issue)):
            # If code-issue is None, ignore.
            if not code_issue[code]:
                continue

            # If code-issue is a function, will return the address.
            elif isinstance(code_issue[code], types.FunctionType):
                addr = code_issue[code](addr)
            else:
                addr["issue_" + tag].append(code_issue[code].format(**addr))
        if not addr["issue_" + tag]:
            continue

        issue_string = "MP4 {}: {}.".format(tag, "; ".join(addr.pop("issue_" + tag)))
        if addr["issues"]:
            addr["issues"] += " " + issue_string
        else:
            addr["issues"] = issue_string
    # Convert flags from boolean to Y/N.
    for key in FLAG_KEYS:
        addr[key] = {True: "Y", False: "N"}.get(addr[key])
    return addr


def _update_from_street_name(address, code):
    """Update dictionary with attributes based on street name result codes.

    Args:
        address (dict): Mapping of address atttributes, including the result_code
            attribute.

    Returns:
        dict: Updated mapping including output attributes.
    """
    is_issue = code in address["result_code"]
    # Check for non-issue cases.
    while is_issue:
        if not address["street_name"]:
            break

        # Sometimes MP4 gives false positives for this.
        # Also, MP4 does not include dashes in street names.
        if address["street_name"].replace("-", " ") == address["mp4_street_name"]:
            is_issue = False
            break

        # Sometimes MP4 uses alternate names/abbreviations.
        alts = ALTERNATES["street_name"].get(address["street_name"], ())
        if address["mp4_street_name"] in alts:
            is_issue = False
            break

        # Special case false positives.
        # Incorrectly parsing directional from or into name.
        parsed_directional = any(
            all(
                [
                    direction in address["street_name"],
                    direction not in address["mp4_street_name"],
                    address["pre_direction_code"] is None,
                    address["mp4_pre_direction_code"] == direction[0],
                ]
            )
            for direction in ["NORTH", "EAST", "SOUTH", "WEST"]
        )
        if parsed_directional:
            is_issue = False
            break

        # Incorrectly parsing type from or into name.
        if address["street_name"]:
            if any(
                [
                    all(
                        [
                            "PARKWAY" in address["street_name"],
                            "PARKWAY" not in address["mp4_street_name"],
                            address["street_type_code"] == "PKWY",
                            address["mp4_street_type_code"] != "PKWY",
                        ]
                    )
                ]
            ):
                is_issue = False
                break

        break

    if is_issue:
        # Changed street name is still a valid match (no great changes).
        addr = address.copy()
        issue = 'street_name = "{street_name}" --> "{mp4_street_name}"'.format(**addr)
        addr["issue_changes"].append(issue)
    else:
        addr = address
    return addr


def update_from_code_ac05(address):
    """Update dictionary with attributes based on result code AC05.

    Args:
        address (dict): Mapping of address atttributes, including the result_code
            attribute.

    Returns:
        dict: Updated mapping including output attributes.
    """
    return _update_from_street_name(address, code="AC05")


def update_from_code_ac10(address):
    """Update dictionary with attributes based on result code AC10.

    Args:
        address (dict): Mapping of address atttributes, including the result_code
            attribute.

    Returns:
        dict: Updated mapping including output attributes.
    """
    return _update_from_street_name(address, code="AC10")


def update_from_code_ac11(address):
    """Update dictionary with attributes based on result code AC11.

    Args:
        address (dict): Mapping of address atttributes, including the result_code
        attribute.

    Returns:
        dict: Updated mapping including output attributes.
    """
    is_issue = "AC11" in address["result_code"]
    # Check for non-issue cases.
    while is_issue:
        # Sometimes MP4 gives false positives for this.
        if address["street_type_code"] == address["mp4_street_type_code"]:
            is_issue = False
            break

        # Sometimes MP4 uses alternate names/abbreviations.
        alts = ALTERNATES["street_type_code"].get(address["street_type_code"], [])
        if address["mp4_street_type_code"] in alts:
            is_issue = False
            break

        # Special case false positives.
        if any([]):
            is_issue = False
            break

        break

    if is_issue:
        # street_type_code False matches are OK (smaller effect than dir).
        addr = address.copy()
        issue = (
            'street_type_code = "{street_type_code}" --> "{mp4_street_type_code}"'
        ).format(**addr)
        addr["issue_changes"].append(issue)
    else:
        addr = address
    return addr


def update_from_code_ac12(address):
    """Update dictionary with attributes based on result code AC12.

    Args:
        address (dict): Mapping of address atttributes, including the result_code
            attribute.

    Returns:
        dict: Updated mapping including output attributes.
    """
    is_issue = "AC12" in address["result_code"]
    # Check for non-issue cases.
    while is_issue:
        # Sometimes MP4 gives false positives for this.
        if address["pre_direction_code"] == address["mp4_pre_direction_code"]:
            is_issue = False
            break

        # Special case false positives.
        if address["street_name"]:
            # Incorrectly parsing directional from or into name.
            parsed_directional = any(
                all(
                    [
                        direction in address["street_name"],
                        direction not in address["mp4_street_name"],
                        address["pre_direction_code"] is None,
                        address["mp4_pre_direction_code"] == direction[0],
                    ]
                )
                for direction in ["NORTH", "EAST", "SOUTH", "WEST"]
            )
            if parsed_directional:
                is_issue = False
                break

            # Very special cases.
            if any(
                [
                    # MP4 insists Beacon Dr has an "E" dir.
                    # Street signs do have a "E" *postdir*!
                    all(
                        [
                            address["city_name"] == "Eugene",
                            address["street_name"] == "BEACON",
                            address["street_type_code"] == "DR",
                            address["pre_direction_code"] is None,
                            address["mp4_pre_direction_code"] == "E",
                        ]
                    ),
                    # MP4 insists Van Duyn St has "N" north of Harlow.
                    all(
                        [
                            address["city_name"] == "Eugene",
                            address["street_name"] == "VAN DUYN",
                            address["street_type_code"] == "ST",
                            address["pre_direction_code"] is None,
                            address["mp4_pre_direction_code"] == "N",
                        ]
                    ),
                    # MP4 insists Fairview Dr has a "W" dir.
                    all(
                        [
                            address["city_name"] == "Springfield",
                            address["street_name"] == "FAIRVIEW",
                            address["street_type_code"] == "DR",
                            address["pre_direction_code"] is None,
                            address["mp4_pre_direction_code"] == "W",
                        ]
                    ),
                ]
            ):
                is_issue = False
                break

        break

    if is_issue:
        # Not considering changed street type to be a valid match.
        addr = _neg_match_keys(address)
        issue = (
            'pre_direction_code = "{pre_direction_code}" --> "{mp4_pre_direction_code}"'
        ).format(**addr)
        addr["issue_changes"].append(issue)
    else:
        addr = address
    return addr


def update_from_code_ac13(address):
    """Update dictionary with attributes based on result code AC13.

    Args:
        address (dict): Mapping of address atttributes, including the result_code
            attribute.

    Returns:
        dict: Updated mapping including output attributes.
    """
    is_issue = "AC13" in address["result_code"]
    # Check for non-issue cases.
    while is_issue:
        # Sometimes MP4 gives false positives for this.
        if address["unit_type_code"] == address["mp4_unit_type_code"]:
            is_issue = False
            break

        # Sometimes MP4 uses alternate names/abbreviations.
        alts = ALTERNATES["unit_type_code"].get(address["unit_type_code"], [])
        if address["mp4_unit_type_code"] in alts:
            is_issue = False
            break

        # Special case false positives.
        if any([]):
            is_issue = False
            break

        break

    if is_issue:
        # Changed unit type is still a valid match.
        addr = address.copy()
        issue = 'unit_type_code = "{unit_type_code}" --> "{mp4_unit_type_code}"'.format(
            **addr
        )
        addr["issue_changes"].append(issue)
    else:
        addr = address
    return addr


def update_from_code_ac14(address):
    """Update dictionary with attributes based on result code AC14.

    Args:
        address (dict): Mapping of address atttributes, including the result_code
            attribute.

    Returns:
        dict: Updated mapping including output attributes.
    """
    is_issue = "AC14" in address["result_code"]
    # Check for non-issue cases.
    while is_issue:
        # Sometimes MP4 gives false positives for this.
        if address["unit_id"] == address["mp4_unit_id"]:
            is_issue = False
            break
        # Special case false positives.
        if any([]):
            is_issue = False
            break
        break
    if is_issue:
        # Changed unit ID is still a valid match.
        addr = address.copy()
        issue = 'unit_id = "{unit_id}" --> "{mp4_unit_id}"'.format(**addr)
        addr["issue_changes"].append(issue)
    else:
        addr = address
    return addr


# ETLs & updates.


def address_3rd_party_etl():
    """Run ETL for third-party address QA dataset.

    Run ad-hoc.

    Update cursors do not seem to work on non-SDE datasets (so ETL here).
    """
    with arcetl.ArcETL("Address (Third-party) QA") as etl:
        etl.init_schema(template_path=dataset.ADDRESS_3RD_PARTY_USPS.path())
        etl.transform(
            arcetl.features.insert_from_dicts,
            insert_features=address_3rd_party_usps_rows,
            field_names=dataset.ADDRESS_3RD_PARTY_USPS.field_names,
        )
        etl.load(dataset.ADDRESS_3RD_PARTY_USPS.path())


def address_workfile_etl():
    """Run ETL for address workfile.

    Workfiles are for processing with Mailers+4 app for postal info.
    """
    temp_field_type = {"house_nbr": "long", "house_suffix_code": "text"}
    with arcetl.ArcETL("Address Workfile") as etl:
        etl.init_schema(field_metadata_list=dataset.ADDRESS_WORKFILE.fields)
        for key, _type in temp_field_type.items():
            etl.transform(arcetl.dataset.add_field, field_name=key, field_type=_type)
        etl.transform(
            arcetl.features.insert_from_path,
            insert_dataset_path=dataset.SITE_ADDRESS.path("maint"),
            insert_where_sql=VALID_ADDRESS_WHERE_SQL,
        )
        field_concat_keys = {
            # Mailers+4 doesn"t separate house number from suffix.
            "house": ["house_nbr", "house_suffix_code"],
            "concat_address": [
                "house_nbr",
                "house_suffix_code",
                "pre_direction_code",
                "street_name",
                "street_type_code",
                "unit_type_code",
                "unit_id",
            ],
        }
        for key, concat_keys in field_concat_keys.items():
            etl.transform(
                arcetl.attributes.update_by_function,
                field_name=key,
                function=concatenate_arguments,
                field_as_first_arg=False,
                arg_field_names=concat_keys,
            )
        for key in [
            "house",
            "pre_direction_code",
            "street_name",
            "street_type_code",
            "unit_type_code",
            "unit_id",
            "city_name",
        ]:
            etl.transform(
                arcetl.attributes.update_by_function,
                field_name="mp4_" + key,
                function=(lambda x: x),
                field_as_first_arg=False,
                arg_field_names=[key],
            )
        etl.transform(
            arcetl.attributes.update_by_value, field_name="state_code", value="OR"
        )
        etl.load(dataset.ADDRESS_WORKFILE.path())
    check_addresses(
        parameters_path=os.path.join(
            path.REGIONAL_STAGING, "MailersPlus4\\Address.parameters.txt"
        )
    )


def address_workfile_3rd_party_etl():
    """Run ETL for third-party address workfile.

    Third-pary addresses are address sources that we examine to determine if RLID/CPA
    is missing addresses in our coverage.

    Workfiles are for processing with Mailers+4 app for postal info.
    """
    field_insert_key = {"concat_address": "address", "city_name": "city"}
    with arcetl.ArcETL("Third-Party Address Workfile") as etl:
        etl.init_schema(field_metadata_list=dataset.ADDRESS_WORKFILE_3RD_PARTY.fields)
        for key, insert_key in field_insert_key.items():
            etl.transform(
                arcetl.dataset.duplicate_field,
                field_name=key,
                new_field_name=insert_key,
            )
        etl.transform(
            arcetl.features.insert_from_path,
            insert_dataset_path=os.path.join(
                database.RLID_STAGING.path, "dbo.Address_QA_Final"
            ),
        )
        for key, insert_key in field_insert_key.items():
            etl.transform(
                arcetl.attributes.update_by_function,
                field_name=key,
                function=(lambda x: x),
                field_as_first_arg=False,
                arg_field_names=[insert_key],
            )
            etl.transform(arcetl.dataset.delete_field, field_name=insert_key)
        for key in ["city_name"]:
            etl.transform(
                arcetl.attributes.update_by_function,
                field_name="mp4_" + key,
                function=(lambda x: x),
                field_as_first_arg=False,
                arg_field_names=[key],
            )
        etl.transform(
            arcetl.attributes.update_by_value, field_name="state_code", value="OR"
        )
        etl.load(dataset.ADDRESS_WORKFILE_3RD_PARTY.path())
    check_addresses(
        parameters_path=os.path.join(
            path.REGIONAL_STAGING, "MailersPlus4\\Address_Third_Party.parameters.txt"
        )
    )


##TODO: Check dicts for counts. If all/most of a column is None, throw error & don't write.
def postal_info_update():
    """Run update for address postal info dataset."""
    arcetl.features.update_from_dicts(
        dataset.ADDRESS_POSTAL_INFO.path(),
        update_features=postal_info_rows,
        id_field_names=dataset.ADDRESS_POSTAL_INFO.id_field_names,
        field_names=dataset.ADDRESS_POSTAL_INFO.field_names,
    )


# Jobs.


WEEKLY_JOB = Job(
    "Address_Postal_Info_Weekly", etls=[address_workfile_etl, postal_info_update]
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
